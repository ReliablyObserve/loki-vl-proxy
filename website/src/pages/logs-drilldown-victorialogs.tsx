import type {ReactNode} from 'react';
import Link from '@docusaurus/Link';
import Heading from '@theme/Heading';
import MarketingLayout from '@site/src/components/MarketingLayout';
import styles from '@site/src/components/marketing.module.css';
import {coreFaqs} from '@site/src/data/faqs';

export default function LogsDrilldownVictoriaLogs(): ReactNode {
  return (
    <MarketingLayout
      path="/logs-drilldown-victorialogs/"
      title="Logs Drilldown with VictoriaLogs"
      description="Use Grafana Logs Drilldown with VictoriaLogs through Loki-VL-proxy, including patterns, detected fields, service buckets, and the compatibility contracts the app depends on."
      eyebrow="Grafana Logs Drilldown"
      headline="Keep Grafana Logs Drilldown working with VictoriaLogs"
      lede="Logs Drilldown is stricter than generic Loki clients. It depends on specific datasource resources, service buckets, detected fields, field values, and pattern grouping. Loki-VL-proxy treats those as explicit compatibility contracts rather than hoping generic query compatibility is enough."
      primaryCta={{label: 'Read Drilldown compatibility', to: '/docs/compatibility-drilldown/'}}
      secondaryCta={{label: 'Read patterns support', to: '/docs/patterns/'}}
      highlights={[
        {
          value: 'Patterns endpoint',
          label: 'The Loki-compatible /patterns path is part of the supported surface',
          detail: 'Runtime-gated and documented.',
        },
        {
          value: 'Detected fields',
          label: 'Field discovery and field values are treated as first-class contracts',
          detail: 'Critical for Drilldown.',
        },
        {
          value: 'Service buckets',
          label: 'Volume and service detail screens need stable service_name behavior',
          detail: 'Validated in compatibility tests.',
        },
        {
          value: 'Current app families',
          label: 'The project explicitly tracks current and previous Drilldown families',
          detail: 'Not a generic best-effort promise.',
        },
      ]}
      faqs={coreFaqs}
    >
      <section className={styles.section}>
        <div className={styles.cardGrid}>
          <div className={styles.card}>
            <h2 className={styles.cardTitle}>What Drilldown needs beyond basic queries</h2>
            <ul className={styles.list}>
              <li>Datasource resource endpoints used by the app scenes.</li>
              <li>Service-selection buckets and service-detail volume views.</li>
              <li>Detected fields and detected field values that do not leak the wrong label surfaces.</li>
              <li>Pattern grouping responses with non-empty grouped payloads.</li>
            </ul>
          </div>
          <div className={styles.card}>
            <h2 className={styles.cardTitle}>Why this is a separate compatibility track</h2>
            <p>
              Logs Drilldown is opinionated around its own runtime and contract
              expectations. The project therefore keeps a dedicated compatibility
              track rather than assuming a generic Loki datasource pass means the
              Drilldown experience is safe.
            </p>
          </div>
        </div>
      </section>

      <section className={styles.section}>
        <div className={styles.sectionHeader}>
          <Heading as="h2" className={styles.sectionTitle}>
            The operational view you want during rollout
          </Heading>
          <p className={styles.sectionLead}>
            Drilldown regressions often show up in metadata and pattern paths
            before they show up in raw query results. That is why route-specific
            rates, latency, errors, and cache efficiency matter here as much as
            generic request totals.
          </p>
        </div>
        <div className={styles.routeStrip}>
          <span>/loki/api/v1/patterns</span>
          <span>/loki/api/v1/index/volume</span>
          <span>/loki/api/v1/index/volume_range</span>
          <span>/loki/api/v1/detected_fields</span>
          <span>/loki/api/v1/detected_field/&lt;name&gt;/values</span>
        </div>
        <div className={styles.cardGrid}>
          <div className={styles.card}>
            <h3 className={styles.cardTitle}>Patterns latency and errors</h3>
            <p>
              Patterns are a separate contract now, with explicit runtime gating.
              Watch them like a product feature, not an optional side path.
            </p>
          </div>
          <div className={styles.card}>
            <h3 className={styles.cardTitle}>Field discovery cache efficiency</h3>
            <p>
              Detected fields and field values should be warm enough that the app
              does not repeatedly force expensive scans for the same service or
              filter combinations.
            </p>
          </div>
          <div className={styles.card}>
            <h3 className={styles.cardTitle}>Service-name correctness</h3>
            <p>
              Volume endpoints and service buckets depend on consistent service
              naming. That behavior is explicitly tested because wrong service
              buckets degrade the whole app flow.
            </p>
          </div>
          <div className={styles.card}>
            <h3 className={styles.cardTitle}>Upstream versus proxy cost</h3>
            <p>
              If Drilldown feels slow, split the blame. The observability model can
              distinguish backend latency from proxy-side shaping or metadata work.
            </p>
          </div>
        </div>
      </section>

      <section className={styles.section}>
        <div className={styles.callout}>
          <Heading as="h2" className={styles.sectionTitle}>
            Related docs
          </Heading>
          <div className={styles.inlineLinks}>
            <Link to="/docs/compatibility-drilldown/">Drilldown Compatibility</Link>
            <Link to="/docs/patterns/">Patterns</Link>
            <Link to="/docs/observability/">Observability</Link>
            <Link to="/docs/translation-reference/">Translation Reference</Link>
          </div>
        </div>
      </section>
    </MarketingLayout>
  );
}
