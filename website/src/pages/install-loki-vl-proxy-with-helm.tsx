import type {ReactNode} from 'react';
import Link from '@docusaurus/Link';
import Heading from '@theme/Heading';
import MarketingLayout from '@site/src/components/MarketingLayout';
import styles from '@site/src/components/marketing.module.css';
import {coreFaqs} from '@site/src/data/faqs';

const basicInstall = `helm upgrade --install loki-vl-proxy oci://ghcr.io/reliablyobserve/charts/loki-vl-proxy \\
  --version <release> \\
  --set extraArgs.backend=http://victorialogs:9428`;

const statefulInstall = `helm upgrade --install loki-vl-proxy oci://ghcr.io/reliablyobserve/charts/loki-vl-proxy \\
  --version <release> \\
  --set workload.kind=StatefulSet \\
  --set persistence.enabled=true \\
  --set persistence.size=20Gi \\
  --set extraArgs.backend=http://victorialogs:9428 \\
  --set extraArgs.disk-cache-max-bytes=20Gi`;

const peerInstall = `helm upgrade --install loki-vl-proxy oci://ghcr.io/reliablyobserve/charts/loki-vl-proxy \\
  --version <release> \\
  --set replicaCount=3 \\
  --set peerCache.enabled=true \\
  --set extraArgs.backend=http://victorialogs:9428`;

const otlpInstall = `helm upgrade --install loki-vl-proxy oci://ghcr.io/reliablyobserve/charts/loki-vl-proxy \\
  --version <release> \\
  --set extraArgs.backend=http://victorialogs:9428 \\
  --set-string extraArgs.otlp-endpoint=http://otel-collector.monitoring.svc.cluster.local:4318/v1/metrics \\
  --set-string extraArgs.server\\.register-instrumentation=false`;

export default function InstallLokiVLProxyWithHelm(): ReactNode {
  return (
    <MarketingLayout
      path="/install-loki-vl-proxy-with-helm/"
      title="Install Loki-VL-proxy with Helm"
      description="Install Loki-VL-proxy with the Helm chart for basic proxying, persistent disk cache, peer-cache fleets, and OTLP push metrics."
      eyebrow="Helm deployment"
      headline="Install Loki-VL-proxy with Helm and pick the right topology on day one"
      lede="The chart already covers the practical topologies most teams need: a basic Deployment, a StatefulSet with persistent disk cache, a multi-replica peer-cache fleet, and collector-first OTLP export."
      primaryCta={{label: 'Open the chart recipe docs', to: '/docs/getting-started/'}}
      secondaryCta={{label: 'Read operations guidance', to: '/docs/operations/'}}
      highlights={[
        {
          value: 'Deployment first',
          label: 'Start with the smallest topology that proves the path',
          detail: 'Do not overbuild before traffic exists.',
        },
        {
          value: 'Stateful cache when needed',
          label: 'Persistent disk cache is the first real scaling lever for larger hotsets',
          detail: 'Useful for repeated metadata and query reads.',
        },
        {
          value: 'Peer cache fleet',
          label: 'Multiple replicas can reuse results instead of refetching them independently',
          detail: 'Good for overlapping dashboard traffic.',
        },
        {
          value: 'Scrape or OTLP',
          label: 'The chart supports both metrics operating models',
          detail: 'Dashboards stay aligned across both.',
        },
      ]}
      faqs={coreFaqs}
    >
      <section className={styles.section}>
        <div className={styles.cardGrid}>
          <div className={styles.card}>
            <h2 className={styles.cardTitle}>Basic Deployment</h2>
            <p>
              Start here when you want a single replica proving the Loki read
              path into VictoriaLogs and you do not need persistent cache yet.
            </p>
            <pre className={styles.codeBlock}>
              <code>{basicInstall}</code>
            </pre>
          </div>
          <div className={styles.card}>
            <h2 className={styles.cardTitle}>StatefulSet with persistent disk cache</h2>
            <p>
              Use this when repeated reads are too large for memory-only cache
              and you want useful cache survival across pod restarts.
            </p>
            <pre className={styles.codeBlock}>
              <code>{statefulInstall}</code>
            </pre>
          </div>
          <div className={styles.card}>
            <h2 className={styles.cardTitle}>Multi-replica fleet with peer cache</h2>
            <p>
              Use this when several replicas will receive similar traffic and you
              want the fleet to reuse results through the peer-cache ring.
            </p>
            <pre className={styles.codeBlock}>
              <code>{peerInstall}</code>
            </pre>
          </div>
          <div className={styles.card}>
            <h2 className={styles.cardTitle}>Collector-first OTLP metrics export</h2>
            <p>
              Use this when your environment standardizes on OTLP push rather
              than Prometheus scrape for application telemetry.
            </p>
            <pre className={styles.codeBlock}>
              <code>{otlpInstall}</code>
            </pre>
          </div>
        </div>
      </section>

      <section className={styles.section}>
        <div className={styles.columns}>
          <div className={styles.card}>
            <h2 className={styles.cardTitle}>Pick Deployment when</h2>
            <ul className={styles.list}>
              <li>You are still validating translation and datasource behavior.</li>
              <li>Cache persistence is not yet part of the runtime requirement.</li>
              <li>You want the simplest rollout and rollback story.</li>
            </ul>
          </div>
          <div className={styles.card}>
            <h2 className={styles.cardTitle}>Pick StatefulSet when</h2>
            <ul className={styles.list}>
              <li>Repeated query and metadata traffic benefits from larger retained cache.</li>
              <li>You need predictable disk-backed cache sizing with `disk-cache-max-bytes`.</li>
              <li>You want the proxy to come back warm instead of rebuilding cache only from memory.</li>
            </ul>
          </div>
        </div>
        <div className={styles.columns}>
          <div className={styles.card}>
            <h2 className={styles.cardTitle}>Add peer cache when</h2>
            <ul className={styles.list}>
              <li>Several replicas serve overlapping dashboard or Explore traffic.</li>
              <li>You want one warm pod to reduce repeated VictoriaLogs work for the fleet.</li>
              <li>You are already operating 2+ replicas with PDB and sane scaling rules.</li>
            </ul>
          </div>
          <div className={styles.card}>
            <h2 className={styles.cardTitle}>Keep the chart opinionated, not magical</h2>
            <ul className={styles.list}>
              <li>You still need to choose label translation and metadata modes deliberately.</li>
              <li>You still need to validate Grafana Explore, Drilldown, and hot dashboards.</li>
              <li>You should watch route-aware cache and latency signals before aggressive cutover.</li>
            </ul>
          </div>
        </div>
      </section>

      <section className={styles.section}>
        <div className={styles.callout}>
          <Heading as="h2" className={styles.sectionTitle}>
            Helm follow-up docs
          </Heading>
          <div className={styles.inlineLinks}>
            <Link to="/monitor-loki-vl-proxy/">Monitoring guide</Link>
            <Link to="/cache-tiers-and-fleet-cache-for-victorialogs/">Cache and fleet-cache guide</Link>
            <Link to="/docs/scaling/">Scaling</Link>
            <Link to="/docs/fleet-cache/">Fleet Cache</Link>
          </div>
        </div>
      </section>
    </MarketingLayout>
  );
}
