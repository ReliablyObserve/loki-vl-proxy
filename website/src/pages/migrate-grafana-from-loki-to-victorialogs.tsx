import type {ReactNode} from 'react';
import Link from '@docusaurus/Link';
import Heading from '@theme/Heading';
import MarketingLayout from '@site/src/components/MarketingLayout';
import styles from '@site/src/components/marketing.module.css';
import {coreFaqs} from '@site/src/data/faqs';

export default function MigrateGrafanaFromLokiToVictoriaLogs(): ReactNode {
  return (
    <MarketingLayout
      path="/migrate-grafana-from-loki-to-victorialogs/"
      title="Migrate Grafana from Loki to VictoriaLogs"
      description="Migrate Grafana read workflows from a Loki backend to VictoriaLogs by inserting Loki-VL-proxy, validating translation and metadata paths, and rolling out with route-aware observability."
      eyebrow="Migration guide"
      headline="Migrate Grafana read workflows from Loki to VictoriaLogs"
      lede="The safest migration path is to keep Grafana on the Loki datasource, stand up Loki-VL-proxy in front of VictoriaLogs, and cut users over only after the read paths, metadata contracts, and operational dashboards are validated."
      primaryCta={{label: 'Start from getting started', to: '/docs/getting-started/'}}
      secondaryCta={{label: 'Open the compatibility matrix', to: '/docs/compatibility-matrix/'}}
      highlights={[
        {
          value: 'Parallel rollout',
          label: 'You can validate a second datasource before changing existing users',
          detail: 'Lower migration risk.',
        },
        {
          value: 'Translation profile choice',
          label: 'Pick the label and metadata mode that fits current dashboards and click paths',
          detail: 'Do not leave this implicit.',
        },
        {
          value: 'Route-aware cutover',
          label: 'Measure client latency, proxy overhead, upstream latency, and cache efficiency',
          detail: 'Good for regression hunts.',
        },
        {
          value: 'Compatibility gates',
          label: 'Use the project docs and CI contracts as the acceptance bar',
          detail: 'Not generic hope.',
        },
      ]}
      faqs={coreFaqs}
    >
      <section className={styles.section}>
        <div className={styles.cardGrid}>
          <div className={styles.card}>
            <h2 className={styles.cardTitle}>1. Inventory the Grafana behaviors you must preserve</h2>
            <p>
              Before you migrate, list the dashboards, Explore flows, Drilldown
              views, rules screens, and label-browse paths people actually use.
              That is the real contract you need to preserve.
            </p>
          </div>
          <div className={styles.card}>
            <h2 className={styles.cardTitle}>2. Stand up a parallel datasource path</h2>
            <p>
              Deploy Loki-VL-proxy against VictoriaLogs and point a non-primary
              Grafana datasource at the proxy. Keep the existing Loki-backed
              datasource until the new path is validated.
            </p>
          </div>
          <div className={styles.card}>
            <h2 className={styles.cardTitle}>3. Choose translation behavior early</h2>
            <p>
              Decide whether label-facing surfaces should stay Loki-first,
              balanced, or OTel-native on the metadata side. This is where dotted
              fields and underscore labels need a deliberate choice.
            </p>
          </div>
          <div className={styles.card}>
            <h2 className={styles.cardTitle}>4. Validate metadata and Drilldown paths</h2>
            <p>
              Query compatibility alone is not enough. Check label APIs, detected
              fields, field values, service buckets, and patterns if users depend
              on Drilldown.
            </p>
          </div>
        </div>
      </section>

      <section className={styles.section}>
        <div className={styles.cardGrid}>
          <div className={styles.card}>
            <h2 className={styles.cardTitle}>5. Cut traffic over with telemetry already on</h2>
            <p>
              Watch downstream, proxy, cache, and upstream metrics separately as
              you move dashboards or teams across. Regressions are easier to catch
              when those splits are already visible before cutover starts.
            </p>
          </div>
          <div className={styles.card}>
            <h2 className={styles.cardTitle}>6. Keep rollback as a datasource choice</h2>
            <p>
              The practical value of the proxy layer is that rollback can stay a
              datasource or dashboard-routing decision instead of a full backend
              rearchitecture in the middle of an incident.
            </p>
          </div>
          <div className={styles.card}>
            <h2 className={styles.cardTitle}>Cutover checklist</h2>
            <ul className={styles.list}>
              <li>Binary, container, or Helm deployment is healthy.</li>
              <li>Grafana secondary datasource points at the proxy.</li>
              <li>Translation mode and field mappings are locked in.</li>
              <li>Explore, dashboards, and Drilldown smoke checks pass.</li>
              <li>Operational dashboard shows healthy route latency and cache behavior.</li>
            </ul>
          </div>
          <div className={styles.card}>
            <h2 className={styles.cardTitle}>Migration acceptance bar</h2>
            <ul className={styles.list}>
              <li>Hot routes have acceptable downstream p95 and error rate.</li>
              <li>Upstream latency is understood separately from proxy overhead.</li>
              <li>Cache hit ratio is reasonable on repeated dashboard and metadata paths.</li>
              <li>Users can still perform their real Explore and Drilldown workflows.</li>
            </ul>
          </div>
        </div>
      </section>

      <section className={styles.section}>
        <div className={styles.callout}>
          <Heading as="h2" className={styles.sectionTitle}>
            The docs to use during migration
          </Heading>
          <div className={styles.inlineLinks}>
            <Link to="/run-loki-vl-proxy-for-victorialogs/">Run guide</Link>
            <Link to="/grafana-loki-datasource-victorialogs/">Grafana datasource setup</Link>
            <Link to="/monitor-loki-vl-proxy/">Monitoring guide</Link>
            <Link to="/docs/getting-started/">Getting Started</Link>
            <Link to="/docs/translation-modes/">Translation Modes</Link>
            <Link to="/docs/compatibility-matrix/">Compatibility Matrix</Link>
            <Link to="/docs/compatibility-drilldown/">Drilldown Compatibility</Link>
            <Link to="/docs/observability/">Observability</Link>
          </div>
        </div>
      </section>
    </MarketingLayout>
  );
}
