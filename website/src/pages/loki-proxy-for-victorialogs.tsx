import type {ReactNode} from 'react';
import Link from '@docusaurus/Link';
import Heading from '@theme/Heading';
import MarketingLayout from '@site/src/components/MarketingLayout';
import styles from '@site/src/components/marketing.module.css';
import {coreFaqs} from '@site/src/data/faqs';

export default function LokiProxyForVictoriaLogs(): ReactNode {
  return (
    <MarketingLayout
      path="/loki-proxy-for-victorialogs/"
      title="Loki Proxy for VictoriaLogs"
      description="Run a Loki-compatible read proxy in front of VictoriaLogs so Grafana and Loki API clients can keep using the Loki read path without a custom plugin."
      eyebrow="Core deployment pattern"
      headline="Use a Loki-compatible read proxy in front of VictoriaLogs"
      lede="Loki-VL-proxy sits between Grafana or other Loki clients and VictoriaLogs. It keeps the read-side Loki API shape, translates metadata and query behavior where needed, and exposes route-aware telemetry so the middle layer stays operable."
      primaryCta={{label: 'Read the getting started guide', to: '/docs/getting-started/'}}
      secondaryCta={{label: 'See architecture details', to: '/docs/architecture/'}}
      highlights={[
        {
          value: 'Read compatibility',
          label: 'Grafana and Loki API tooling keep the Loki read path',
          detail: 'Datasource, Explore, and Drilldown stay in scope.',
        },
        {
          value: 'VictoriaLogs backend',
          label: 'The backend remains VictoriaLogs plus optional rules or alerts backends',
          detail: 'No change to the storage target.',
        },
        {
          value: 'Translation controls',
          label: 'Label style, metadata field mode, and field mappings are explicit',
          detail: 'Useful for dotted OTel fields.',
        },
        {
          value: 'Operational visibility',
          label: 'Metrics and logs split downstream, proxy, cache, and upstream work',
          detail: 'Prometheus scrape or OTLP push.',
        },
      ]}
      faqs={coreFaqs}
    >
      <section className={styles.section}>
        <div className={styles.columns}>
          <div className={styles.card}>
            <h2 className={styles.cardTitle}>What the proxy actually does</h2>
            <ul className={styles.list}>
              <li>Accepts Loki-compatible read and metadata requests.</li>
              <li>Translates query and metadata paths toward VictoriaLogs and optional rules backends.</li>
              <li>Shapes responses into the tuple and field contracts Grafana expects.</li>
              <li>Adds protective caching, coalescing, fanout limits, and circuit-breaking.</li>
            </ul>
          </div>
          <div className={styles.card}>
            <h2 className={styles.cardTitle}>What it intentionally does not do</h2>
            <ul className={styles.list}>
              <li>It does not become a generic log ingestion system.</li>
              <li>It does not ask Grafana to switch to a custom datasource plugin.</li>
              <li>It does not hide its own latency or cache behavior from operators.</li>
              <li>It does not open write ownership for Loki push paths.</li>
            </ul>
          </div>
        </div>
      </section>

      <section className={styles.section}>
        <div className={styles.sectionHeader}>
          <Heading as="h2" className={styles.sectionTitle}>
            Why this pattern works for Grafana
          </Heading>
          <p className={styles.sectionLead}>
            Grafana is already deeply opinionated around the Loki datasource and
            the Loki-style query path. Loki-VL-proxy keeps that client shape on
            the left while mapping requests to VictoriaLogs-aware internals on
            the right.
          </p>
        </div>
        <div className={styles.cardGrid}>
          <div className={styles.card}>
            <h3 className={styles.cardTitle}>Labels and fields stay manageable</h3>
            <p>
              The proxy can expose underscore labels for Loki-safe query builder
              behavior while keeping dotted field semantics available through
              metadata modes and explicit mappings.
            </p>
          </div>
          <div className={styles.card}>
            <h3 className={styles.cardTitle}>Patterns and Drilldown stay explicit</h3>
            <p>
              The Loki-compatible patterns endpoint is part of the supported
              read surface, with runtime gating and compatibility coverage for
              Grafana Logs Drilldown behavior.
            </p>
          </div>
          <div className={styles.card}>
            <h3 className={styles.cardTitle}>Rules and alerts can stay visible</h3>
            <p>
              Read views for rules and alerts can be bridged through vmalert so
              Grafana does not lose those operational screens during the move.
            </p>
          </div>
          <div className={styles.card}>
            <h3 className={styles.cardTitle}>Operators get their own control plane</h3>
            <p>
              The project ships route-aware metrics, structured logs, a packaged
              dashboard, and runbook-oriented docs instead of treating the proxy
              as a black box.
            </p>
          </div>
        </div>
      </section>

      <section className={styles.section}>
        <div className={styles.callout}>
          <Heading as="h2" className={styles.sectionTitle}>
            Where to go next
          </Heading>
          <p className={styles.sectionLead}>
            If this is the problem you are trying to solve, the next useful docs
            are the deployment guide, translation behavior, and observability
            model.
          </p>
          <div className={styles.inlineLinks}>
            <Link to="/docs/getting-started/">Getting Started</Link>
            <Link to="/docs/translation-modes/">Translation Modes</Link>
            <Link to="/docs/compatibility-victorialogs/">VictoriaLogs Compatibility</Link>
            <Link to="/docs/observability/">Observability Guide</Link>
          </div>
        </div>
      </section>
    </MarketingLayout>
  );
}
