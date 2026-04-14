import type {ReactNode} from 'react';
import Link from '@docusaurus/Link';
import Heading from '@theme/Heading';
import MarketingLayout from '@site/src/components/MarketingLayout';
import styles from '@site/src/components/marketing.module.css';
import {coreFaqs} from '@site/src/data/faqs';

const datasourceYaml = `datasources:
  - name: Loki (via VL proxy)
    type: loki
    access: proxy
    url: http://loki-vl-proxy:3100`;

export default function GrafanaLokiDatasourceVictoriaLogs(): ReactNode {
  return (
    <MarketingLayout
      path="/grafana-loki-datasource-victorialogs/"
      title="Grafana Loki Datasource for VictoriaLogs"
      description="Use Grafana's native Loki datasource with VictoriaLogs by pointing Grafana at Loki-VL-proxy instead of asking Grafana to use a custom plugin."
      eyebrow="Datasource answer"
      headline="Use Grafana's native Loki datasource with VictoriaLogs"
      lede="If the concrete question is how to keep the Grafana Loki datasource while moving the backend to VictoriaLogs, the answer is to point Grafana at Loki-VL-proxy. Grafana still thinks it is talking to Loki."
      primaryCta={{label: 'Open the getting started guide', to: '/docs/getting-started/'}}
      secondaryCta={{label: 'See VictoriaLogs compatibility', to: '/docs/compatibility-victorialogs/'}}
      highlights={[
        {
          value: 'Datasource type stays loki',
          label: 'Grafana configuration stays on the built-in Loki datasource',
          detail: 'Only the URL changes.',
        },
        {
          value: 'No custom plugin',
          label: 'The proxy preserves the Grafana-side integration point',
          detail: 'Less operational churn.',
        },
        {
          value: 'Field translation controls',
          label: 'You can stay Loki-first, hybrid, or OTel-native on field surfaces',
          detail: 'Useful for dotted fields.',
        },
        {
          value: 'Route-aware telemetry',
          label: 'Client, proxy, cache, and upstream paths stay visible',
          detail: 'Useful before and after cutover.',
        },
      ]}
      faqs={coreFaqs}
    >
      <section className={styles.section}>
        <div className={styles.columns}>
          <div className={styles.card}>
            <h2 className={styles.cardTitle}>Minimal datasource shape</h2>
            <p>
              Grafana stays on the standard Loki datasource type. The proxy URL
              becomes the datasource target.
            </p>
            <pre>
              <code>{datasourceYaml}</code>
            </pre>
          </div>
          <div className={styles.card}>
            <h2 className={styles.cardTitle}>Why this matters</h2>
            <ul className={styles.list}>
              <li>No UI-side datasource fork to maintain.</li>
              <li>No extra Grafana plugin lifecycle to secure or upgrade.</li>
              <li>Existing dashboards and Explore entry points keep the Loki contract.</li>
              <li>Migration work moves into a controllable server-side layer.</li>
            </ul>
          </div>
        </div>
      </section>

      <section className={styles.section}>
        <div className={styles.sectionHeader}>
          <Heading as="h2" className={styles.sectionTitle}>
            What to validate when you point Grafana at the proxy
          </Heading>
          <p className={styles.sectionLead}>
            The risky parts are not the datasource form itself. They are field
            semantics, metadata lookups, and the latency or cache behavior of the
            translation layer under real dashboards and user queries.
          </p>
        </div>
        <div className={styles.cardGrid}>
          <div className={styles.card}>
            <h3 className={styles.cardTitle}>Label and field semantics</h3>
            <p>
              Choose the translation profile that matches your Grafana builder and
              field-exploration needs. Loki-safe underscore labels can coexist
              with dotted field metadata for OTel-backed schemas.
            </p>
          </div>
          <div className={styles.card}>
            <h3 className={styles.cardTitle}>Metadata endpoints</h3>
            <p>
              Labels, label values, detected fields, detected field values, and
              patterns are all part of the user experience. They need to be
              validated as first-class surfaces, not afterthoughts.
            </p>
          </div>
          <div className={styles.card}>
            <h3 className={styles.cardTitle}>Latency split</h3>
            <p>
              Watch downstream client latency, upstream VictoriaLogs latency, and
              measured proxy overhead separately. The observability model is built
              for exactly that split.
            </p>
          </div>
          <div className={styles.card}>
            <h3 className={styles.cardTitle}>Cache efficiency by route</h3>
            <p>
              High-cardinality label browsing and repeated query_range traffic are
              the places where cache hit or miss behavior changes user experience
              and backend cost fastest.
            </p>
          </div>
        </div>
      </section>

      <section className={styles.section}>
        <div className={styles.callout}>
          <Heading as="h2" className={styles.sectionTitle}>
            Recommended docs from here
          </Heading>
          <div className={styles.inlineLinks}>
            <Link to="/docs/getting-started/">Getting Started</Link>
            <Link to="/docs/translation-modes/">Translation Modes</Link>
            <Link to="/docs/compatibility-matrix/">Compatibility Matrix</Link>
            <Link to="/docs/observability/">Observability</Link>
          </div>
        </div>
      </section>
    </MarketingLayout>
  );
}
