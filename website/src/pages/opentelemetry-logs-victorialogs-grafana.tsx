import type {ReactNode} from 'react';
import Link from '@docusaurus/Link';
import Heading from '@theme/Heading';
import MarketingLayout from '@site/src/components/MarketingLayout';
import styles from '@site/src/components/marketing.module.css';
import {coreFaqs} from '@site/src/data/faqs';

const otelCollectorConfig = `# OTel Collector: export logs to VictoriaLogs via Loki push endpoint
exporters:
  loki:
    endpoint: http://loki-vl-proxy:3100/loki/api/v1/push
    labels:
      resource_attributes:
        - service.name
        - k8s.namespace.name
        - k8s.pod.name
        - k8s.container.name

# Or use the VictoriaLogs native OTLP endpoint directly:
exporters:
  otlphttp:
    endpoint: http://victorialogs:9428/insert/opentelemetry`;

const fieldRows = [
  {
    otel: 'service.name',
    loki: 'service_name',
    exposed: 'detected_fields + label (translated mode)',
  },
  {
    otel: 'k8s.pod.name',
    loki: 'k8s_pod_name',
    exposed: 'detected_fields + label (translated mode)',
  },
  {
    otel: 'k8s.namespace.name',
    loki: 'k8s_namespace_name',
    exposed: 'detected_fields + label (translated mode)',
  },
  {
    otel: 'k8s.container.name',
    loki: 'k8s_container_name',
    exposed: 'detected_fields + label (translated mode)',
  },
  {
    otel: 'severity_text',
    loki: 'severity_text',
    exposed: 'detected_fields (no dot — no translation needed)',
  },
  {
    otel: 'trace_id',
    loki: 'trace_id',
    exposed: 'detected_fields (high-cardinality, not promoted to label)',
  },
];

export default function OpenTelemetryLogsVictoriaLogsGrafana(): ReactNode {
  return (
    <MarketingLayout
      path="/opentelemetry-logs-victorialogs-grafana/"
      title="OpenTelemetry Logs with VictoriaLogs and Grafana Explore"
      description="Collect OpenTelemetry logs into VictoriaLogs and explore them in Grafana Explore and Logs Drilldown through loki-vl-proxy — with full detected_fields and dotted-label support."
      eyebrow="OpenTelemetry logs"
      headline="OpenTelemetry logs into VictoriaLogs, browsable in Grafana Explore"
      lede="The OTel Collector sends logs to VictoriaLogs. Loki-VL-proxy translates the Loki read API transparently so Grafana Explore, Logs Drilldown, and the native Loki datasource stay fully functional — including dotted field detection (service.name, k8s.pod.name) and label translation for the Grafana query builder."
      primaryCta={{label: 'Read the translation modes guide', to: '/docs/translation-modes/'}}
      secondaryCta={{label: 'See Grafana Explore details', to: '/grafana-explore-victorialogs/'}}
      highlights={[
        {
          value: 'Dotted label handling',
          label: 'service.name, k8s.pod.name, and similar OTel fields are detected automatically',
          detail: 'Exposed via detected_fields in Explore and Logs Drilldown.',
        },
        {
          value: 'Underscore translation',
          label: 'Dotted OTel fields translated to Loki-safe underscore labels for the query builder',
          detail: 'service.name → service_name. Choose native, translated, or hybrid mode.',
        },
        {
          value: 'detected_fields',
          label: 'All OTel resource and log attributes surfaced in Grafana field browser',
          detail: 'Including high-cardinality fields like trace_id and user_id as field values, not labels.',
        },
        {
          value: 'Four delivery modes',
          label: 'Loki push (dotted), VL JSON, OTel structured, or pre-translated underscore',
          detail: 'Test coverage for all four delivery mechanisms.',
        },
      ]}
      faqs={coreFaqs}
    >
      <section className={styles.section}>
        <div className={styles.sectionHeader}>
          <Heading as="h2" className={styles.sectionTitle}>
            OTel Collector to VictoriaLogs to Grafana
          </Heading>
          <p className={styles.sectionLead}>
            The full pipeline: OTel Collector exports logs, VictoriaLogs stores
            them with all fields indexed, the proxy translates the Loki read API,
            and Grafana Explore and Logs Drilldown work without any custom plugin.
          </p>
        </div>
        <div className={styles.card}>
          <pre className={styles.codeBlock}><code>{otelCollectorConfig}</code></pre>
          <p>
            When using the Loki push endpoint, resource attributes that contain
            dots (OTel convention) are stored in VictoriaLogs as-is. The proxy
            detects these at query time and handles the label/field translation
            for Grafana automatically.
          </p>
        </div>
      </section>

      <section className={styles.section}>
        <div className={styles.sectionHeader}>
          <Heading as="h2" className={styles.sectionTitle}>
            Dotted OTel field mapping
          </Heading>
          <p className={styles.sectionLead}>
            The proxy exposes OTel resource attributes both as <code>detected_fields</code>{' '}
            (for Explore field browser) and as translated underscore labels (for the
            Grafana query builder) depending on the configured translation mode.
          </p>
        </div>
        <div className={styles.tableWrap}>
          <table className={styles.comparisonTable}>
            <thead>
              <tr>
                <th>OTel field</th>
                <th>Loki-safe label (translated mode)</th>
                <th>How it is exposed</th>
              </tr>
            </thead>
            <tbody>
              {fieldRows.map((row) => (
                <tr key={row.otel}>
                  <td><code>{row.otel}</code></td>
                  <td><code>{row.loki}</code></td>
                  <td>{row.exposed}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      <section className={styles.section}>
        <div className={styles.cardGrid}>
          <div className={styles.card}>
            <h2 className={styles.cardTitle}>Three translation modes</h2>
            <ul className={styles.list}>
              <li><strong>Native mode:</strong> dotted OTel fields surfaced as-is via <code>detected_fields</code>. Best when all dashboards use field filters, not label matchers.</li>
              <li><strong>Translated (underscore) mode:</strong> dots replaced with underscores — <code>service.name</code> becomes <code>service_name</code>. Grafana query builder label dropdowns work normally.</li>
              <li><strong>Hybrid mode:</strong> both dotted and underscore forms are available simultaneously. Useful during migrations when some dashboards use each convention.</li>
            </ul>
          </div>
          <div className={styles.card}>
            <h2 className={styles.cardTitle}>detected_fields in Explore and Drilldown</h2>
            <ul className={styles.list}>
              <li>The proxy translates VictoriaLogs field metadata into the <code>/loki/api/v1/detected_fields</code> response Grafana Explore expects.</li>
              <li>OTel resource attributes (service.name, k8s.*) appear in the field browser alongside log-level fields like severity_text.</li>
              <li>High-cardinality OTel fields (trace_id, span_id, user_id) are surfaced as field values, not promoted to stream labels — matching Loki&apos;s own OTel behavior.</li>
              <li>The proxy exposes <code>service</code> and <code>service.name</code> (not Loki&apos;s <code>_extracted</code> suffix) for OTel data — stamped Loki-compatible.</li>
            </ul>
          </div>
          <div className={styles.card}>
            <h2 className={styles.cardTitle}>Four OTel delivery mechanisms</h2>
            <ul className={styles.list}>
              <li><strong>Loki push (dotted):</strong> OTel Collector pushes to the Loki endpoint with dotted resource attribute labels.</li>
              <li><strong>VL JSON:</strong> direct VictoriaLogs JSON ingest via the VL HTTP API.</li>
              <li><strong>OTel structured (mixed):</strong> OTLP endpoint with mixed field naming conventions.</li>
              <li><strong>Pre-translated (underscore):</strong> OTel Collector rewrites dots to underscores before push — no proxy-side translation needed.</li>
            </ul>
          </div>
          <div className={styles.card}>
            <h2 className={styles.cardTitle}>Why VictoriaLogs handles OTel well</h2>
            <ul className={styles.list}>
              <li>All fields are indexed — OTel resource attributes are searchable without being promoted to stream labels.</li>
              <li>High-cardinality fields like trace_id work fine as log fields; Loki warns that the same fields as labels hurt performance.</li>
              <li>54.9× real-tested compression ratio: OTel logs with repeated resource attributes compress very well in VictoriaLogs&apos; column-oriented storage.</li>
              <li>Full-text search across all OTel log bodies and attributes without the stream-selection + line-filter two-step.</li>
            </ul>
          </div>
        </div>
      </section>

      <section className={styles.section}>
        <div className={styles.callout}>
          <Heading as="h2" className={styles.sectionTitle}>
            Related docs
          </Heading>
          <div className={styles.inlineLinks}>
            <Link to="/docs/translation-modes/">Translation modes</Link>
            <Link to="/docs/compatibility-victorialogs/">VictoriaLogs compatibility</Link>
            <Link to="/grafana-explore-victorialogs/">Grafana Explore guide</Link>
            <Link to="/victorialogs-kubernetes-loki/">Kubernetes deployment</Link>
            <Link to="/victorialogs-vs-loki-cost-and-performance/">Cost comparison</Link>
            <Link to="/docs/getting-started/">Getting started</Link>
          </div>
        </div>
      </section>
    </MarketingLayout>
  );
}
