import type {ReactNode} from 'react';
import Link from '@docusaurus/Link';
import Heading from '@theme/Heading';
import MarketingLayout from '@site/src/components/MarketingLayout';
import styles from '@site/src/components/marketing.module.css';
import {coreFaqs} from '@site/src/data/faqs';

export default function VictoriaLogsWithGrafanaLokiDatasource(): ReactNode {
  return (
    <MarketingLayout
      path="/victorialogs-with-grafana-loki-datasource/"
      title="VictoriaLogs with Grafana Loki Datasource"
      description="The deployment pattern for using VictoriaLogs with Grafana's native Loki datasource: route Grafana through Loki-VL-proxy and keep the compatibility work server-side."
      eyebrow="Architecture pattern"
      headline="Use VictoriaLogs behind Grafana's Loki datasource"
      lede="This is the end-to-end pattern teams adopt when they want VictoriaLogs in the backend and do not want to rewrite Grafana around a non-Loki datasource. The key is to keep compatibility server-side, where it can be measured and controlled."
      primaryCta={{label: 'See the deployment guide', to: '/docs/getting-started/'}}
      secondaryCta={{label: 'Open operations guidance', to: '/docs/operations/'}}
      highlights={[
        {
          value: 'Three-part path',
          label: 'Grafana, Loki-VL-proxy, VictoriaLogs',
          detail: 'Simple enough to reason about.',
        },
        {
          value: 'Helm-ready',
          label: 'The chart supports image selection, stateful cache, and peer cache',
          detail: 'Useful in Kubernetes.',
        },
        {
          value: 'Read-only surface',
          label: 'The proxy owns query and metadata compatibility, not ingest',
          detail: 'Keeps ownership boundaries clear.',
        },
        {
          value: 'Observability built in',
          label: 'Operational resources, cache layers, and per-route latency stay visible',
          detail: 'Before and after cutover.',
        },
      ]}
      faqs={coreFaqs}
    >
      <section className={styles.section}>
        <div className={styles.routeStrip}>
          <span>Grafana Loki datasource</span>
          <span>Loki-VL-proxy</span>
          <span>VictoriaLogs</span>
          <span>Optional vmalert</span>
        </div>
      </section>

      <section className={styles.section}>
        <div className={styles.cardGrid}>
          <div className={styles.card}>
            <h2 className={styles.cardTitle}>Why keep Grafana on Loki</h2>
            <p>
              Grafana's native Loki datasource already powers the query builders,
              Explore, and Drilldown workflows your users know. Preserving that
              contract keeps the migration smaller and easier to validate.
            </p>
          </div>
          <div className={styles.card}>
            <h2 className={styles.cardTitle}>Why keep compatibility server-side</h2>
            <p>
              The server-side layer can be observed, rate-limited, cached, and
              rolled out progressively. Those controls are much harder if the
              compatibility work is spread across clients and dashboards.
            </p>
          </div>
          <div className={styles.card}>
            <h2 className={styles.cardTitle}>How caching fits the pattern</h2>
            <p>
              Compatibility-edge cache, memory cache, disk cache, and optional
              peer cache all exist to make expensive metadata and query routes more
              predictable when many Grafana users hit the same paths.
            </p>
          </div>
          <div className={styles.card}>
            <h2 className={styles.cardTitle}>How to scale it</h2>
            <p>
              Start with a basic deployment, then add persistent disk cache or a
              peer-cache fleet when the workload justifies it. The docs already
              cover both patterns.
            </p>
          </div>
        </div>
      </section>

      <section className={styles.section}>
        <div className={styles.callout}>
          <Heading as="h2" className={styles.sectionTitle}>
            Deep docs
          </Heading>
          <div className={styles.inlineLinks}>
            <Link to="/docs/getting-started/">Getting Started</Link>
            <Link to="/docs/scaling/">Scaling</Link>
            <Link to="/docs/fleet-cache/">Fleet Cache</Link>
            <Link to="/docs/peer-cache-design/">Peer Cache Design</Link>
          </div>
        </div>
      </section>
    </MarketingLayout>
  );
}
