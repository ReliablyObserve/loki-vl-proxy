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

const verifyCommands = `curl -sS http://127.0.0.1:3100/ready
curl -sS http://127.0.0.1:3100/loki/api/v1/labels`;

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
            <pre className={styles.codeBlock}>
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
        <div className={styles.cardGrid}>
          <div className={styles.card}>
            <h2 className={styles.cardTitle}>1. Bring the proxy up first</h2>
            <p>
              Do not debug Grafana and backend reachability at the same time.
              Stand up the proxy, make sure `/ready` is healthy, and prove a
              simple labels call before you touch Grafana.
            </p>
            <pre className={styles.codeBlock}>
              <code>{verifyCommands}</code>
            </pre>
          </div>
          <div className={styles.card}>
            <h2 className={styles.cardTitle}>2. Pick translation behavior deliberately</h2>
            <p>
              If VictoriaLogs stores dotted OTel fields, choose the label and
              metadata mode that matches how your users browse data in Grafana.
              This is where most accidental surprises come from.
            </p>
          </div>
          <div className={styles.card}>
            <h2 className={styles.cardTitle}>3. Validate more than query_range</h2>
            <p>
              Users feel label browsing, detected fields, patterns, and service
              buckets just as strongly as they feel line queries. Treat those as
              first-class acceptance checks.
            </p>
          </div>
          <div className={styles.card}>
            <h2 className={styles.cardTitle}>4. Watch route-aware telemetry during cutover</h2>
            <p>
              Downstream latency, upstream latency, status codes, and cache hit
              ratio by route are the fastest way to see whether the new datasource
              path is actually safe for users.
            </p>
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
            <Link to="/run-loki-vl-proxy-for-victorialogs/">Run the proxy</Link>
            <Link to="/install-loki-vl-proxy-with-helm/">Install with Helm</Link>
            <Link to="/monitor-loki-vl-proxy/">Monitoring guide</Link>
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
