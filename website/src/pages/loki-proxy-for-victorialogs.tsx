import type {ReactNode} from 'react';
import Link from '@docusaurus/Link';
import Heading from '@theme/Heading';
import MarketingLayout from '@site/src/components/MarketingLayout';
import QualityVsLoki from '@site/src/components/QualityVsLoki';
import quality from '@site/src/data/quality-vs-loki.json';
import styles from '@site/src/components/marketing.module.css';
import {coreFaqs} from '@site/src/data/faqs';

const firstRequests = quality.distribution.find(
  (row) => row.profile === quality.profile && row.request === 'first',
);

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
          value: `${firstRequests?.below ?? 0} of ${firstRequests?.rows ?? 0}`,
          label: `First requests below Loki v${quality.loki} in the latest daily A/B (${quality.date}, ${quality.profile})`,
          detail: `Above 2× Loki: ${firstRequests?.above ?? 0}. Same data and hardware for both; generated daily.`,
        },
        {
          value: '~14 MB',
          label: 'Static Go binary — no runtime dependencies',
          detail: 'Sits in front of any VictoriaLogs single-node or cluster deployment.',
        },
        {
          value: '4-tier cache',
          label: 'Tier0, L1 memory (256 MB default), L2 disk (bbolt), L3 peer (consistent hash + zstd)',
          detail: 'Circuit breaker and request coalescer protect the backend.',
        },
        {
          value: '~15–30 ms',
          label: 'Added latency on a cold cache miss — near-zero on a warm hit',
          detail: 'Prefilter eliminates ~81.6% of empty-window backend calls.',
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
              <li>Adds a 4-tier cache stack (Tier0, L1 memory, L2 bbolt disk, L3 peer) plus request coalescing and a sliding-window circuit breaker (30s window, 5 failure threshold) to protect the backend.</li>
              <li>Prefilters long-range queries and eliminates ~81.6% of empty-window backend calls.</li>
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

      <QualityVsLoki />

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
