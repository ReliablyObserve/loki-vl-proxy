import type {ReactNode} from 'react';
import Link from '@docusaurus/Link';
import Heading from '@theme/Heading';
import MarketingLayout from '@site/src/components/MarketingLayout';
import styles from '@site/src/components/marketing.module.css';
import {coreFaqs} from '@site/src/data/faqs';

const comparisonRows = [
  {
    area: 'Grafana datasource type',
    loki: 'Native Loki backend, native Loki datasource.',
    victorialogs:
      'Native Loki datasource on the client side, Loki-VL-proxy in the middle, VictoriaLogs in the backend.',
  },
  {
    area: 'Compatibility layer',
    loki: 'Mostly implicit because backend and API are the same product family.',
    victorialogs:
      'Explicit read-side compatibility layer that translates, shapes, and observes the Loki-facing path.',
  },
  {
    area: 'Field semantics',
    loki: 'Loki-native labels and field expectations.',
    victorialogs:
      'Configurable label and metadata translation so dotted OTel fields can coexist with Loki-safe label surfaces.',
  },
  {
    area: 'Operational visibility',
    loki: 'Observe the backend directly.',
    victorialogs:
      'Observe both the proxy and the backend, with per-route latency, errors, and cache behavior split out.',
  },
  {
    area: 'Migration control',
    loki: 'No translation layer to tune.',
    victorialogs:
      'Progressive rollout is possible because Grafana can be cut over through a controlled proxy layer first.',
  },
  {
    area: 'Patterns and Drilldown compatibility',
    loki: 'Native Loki or Grafana app behavior.',
    victorialogs:
      'Handled as explicit contracts and compatibility tracks, including the Loki-compatible patterns endpoint.',
  },
];

export default function LokiVsVictoriaLogsGrafanaQueryWorkflows(): ReactNode {
  return (
    <MarketingLayout
      path="/loki-vs-victorialogs-grafana-query-workflows/"
      title="Loki vs VictoriaLogs for Grafana Query Workflows"
      description="Compare native Loki backends with VictoriaLogs routed through Loki-VL-proxy for Grafana query workflows, including datasource shape, field semantics, visibility, and migration control."
      eyebrow="Comparison"
      headline="Compare native Loki and VictoriaLogs-routed Grafana query workflows"
      lede="This comparison is intentionally narrow: it is about Grafana read and query workflows, not a generic benchmark or ingestion comparison. The decision point is whether you want to keep Grafana on Loki semantics while routing those reads to VictoriaLogs through an explicit proxy layer."
      primaryCta={{label: 'Read the compatibility matrix', to: '/docs/compatibility-matrix/'}}
      secondaryCta={{label: 'Read the migration guide', to: '/migrate-grafana-from-loki-to-victorialogs/'}}
      highlights={[
        {
          value: 'Same client contract',
          label: 'Grafana keeps the Loki datasource in both cases',
          detail: 'The difference is where compatibility lives.',
        },
        {
          value: 'Extra control plane',
          label: 'The VictoriaLogs path adds an observable proxy layer',
          detail: 'That is a feature, not just a cost.',
        },
        {
          value: 'Field translation optionality',
          label: 'VictoriaLogs workflows can explicitly manage dotted versus underscore field exposure',
          detail: 'Native Loki does not need this layer.',
        },
        {
          value: 'Route-aware metrics',
          label: 'The proxy path can show exactly where latency and errors come from',
          detail: 'Useful in migrations.',
        },
      ]}
      faqs={coreFaqs}
    >
      <section className={styles.section}>
        <div className={styles.tableWrap}>
          <table className={styles.comparisonTable}>
            <thead>
              <tr>
                <th>Area</th>
                <th>Native Loki backend</th>
                <th>VictoriaLogs via Loki-VL-proxy</th>
              </tr>
            </thead>
            <tbody>
              {comparisonRows.map((row) => (
                <tr key={row.area}>
                  <td>{row.area}</td>
                  <td>{row.loki}</td>
                  <td>{row.victorialogs}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      <section className={styles.section}>
        <div className={styles.cardGrid}>
          <div className={styles.card}>
            <h2 className={styles.cardTitle}>When the proxy path is attractive</h2>
            <p>
              Choose the proxy path when the user-facing contract is already Loki
              and you want to preserve that contract while making VictoriaLogs the
              data backend. It is especially useful when migration control and
              observability matter more than pretending the systems are identical.
            </p>
          </div>
          <div className={styles.card}>
            <h2 className={styles.cardTitle}>What changes operationally</h2>
            <p>
              You gain a real translation and cache layer that needs to be
              monitored. The upside is that you also gain a controlled place to
              tune translation modes, protect the backend, and see route-specific
              regressions.
            </p>
          </div>
        </div>
      </section>

      <section className={styles.section}>
        <div className={styles.callout}>
          <Heading as="h2" className={styles.sectionTitle}>
            Follow-up docs
          </Heading>
          <div className={styles.inlineLinks}>
            <Link to="/docs/compatibility-matrix/">Compatibility Matrix</Link>
            <Link to="/docs/observability/">Observability</Link>
            <Link to="/migrate-grafana-from-loki-to-victorialogs/">Migration Guide</Link>
          </div>
        </div>
      </section>
    </MarketingLayout>
  );
}
