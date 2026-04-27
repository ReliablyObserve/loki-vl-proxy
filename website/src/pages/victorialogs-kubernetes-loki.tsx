import type {ReactNode} from 'react';
import Link from '@docusaurus/Link';
import Heading from '@theme/Heading';
import MarketingLayout from '@site/src/components/MarketingLayout';
import styles from '@site/src/components/marketing.module.css';
import {coreFaqs} from '@site/src/data/faqs';

const helmInstall = `helm repo add loki-vl-proxy https://reliablyobserve.github.io/loki-vl-proxy
helm repo update

helm install loki-vl-proxy loki-vl-proxy/loki-vl-proxy \\
  --namespace logging \\
  --create-namespace \\
  --set victorialogs.url=http://victorialogs:9428 \\
  --set cache.l1.maxBytes=268435456 \\
  --set cache.l2.enabled=true \\
  --set multitenancy.enabled=true`;

const resourceRows = [
  {
    component: 'Loki (3–30 TB/day)',
    cpu: '431 vCPU',
    memory: '857 GiB',
    nodes: '27 × c7i.4xlarge',
    cost: '~$13,403 / month',
  },
  {
    component: 'VictoriaLogs + proxy (same workload)',
    cpu: '~33 vCPU',
    memory: '~70 GiB',
    nodes: '4–6 × c7i.2xlarge',
    cost: '~$1,100–2,400 / month',
  },
];

export default function VictoriaLogsKubernetesLoki(): ReactNode {
  return (
    <MarketingLayout
      path="/victorialogs-kubernetes-loki/"
      title="Deploy VictoriaLogs with Loki Proxy on Kubernetes"
      description="Replace Loki with VictoriaLogs on Kubernetes while keeping Grafana Explore, Logs Drilldown, and the native Loki datasource fully working — using loki-vl-proxy and Helm."
      eyebrow="Kubernetes deployment"
      headline="Replace Loki with VictoriaLogs on Kubernetes, keep Grafana working"
      lede="Switch your Kubernetes log stack from Loki to VictoriaLogs without touching Grafana dashboards, datasources, or alert rules. Loki-VL-proxy sits between your Grafana deployment and VictoriaLogs, translating the Loki read API transparently — no custom plugin, no dashboard rewrites."
      primaryCta={{label: 'Read the getting started guide', to: '/docs/getting-started/'}}
      secondaryCta={{label: 'See resource sizing', to: '/victorialogs-vs-loki-cost-and-performance/'}}
      highlights={[
        {
          value: '~14 MB binary',
          label: 'Static Go binary — negligible resource footprint in a sidecar or standalone pod',
          detail: 'No JVM, no Python runtime, no extra dependencies.',
        },
        {
          value: '78–92% lower compute',
          label: 'VL + proxy vs Loki at the same ingest tier — based on Grafana published sizing',
          detail: '~33 vCPU / 70 GiB vs 431 vCPU / 857 GiB at 3–30 TB/day.',
        },
        {
          value: '54.9× compression',
          label: 'Real-tested: 800M entries, 310 GiB ingested → 40.5 GiB on disk',
          detail: '7.14-day retention observed on production workload.',
        },
        {
          value: 'Helm install',
          label: 'Single chart covers proxy, VictoriaLogs, and optional vmalert for rule bridging',
          detail: 'Drop-in Loki datasource replacement.',
        },
      ]}
      faqs={coreFaqs}
    >
      <section className={styles.section}>
        <div className={styles.sectionHeader}>
          <Heading as="h2" className={styles.sectionTitle}>
            Quick Helm install
          </Heading>
          <p className={styles.sectionLead}>
            Add the chart repository and install the proxy in front of an
            existing VictoriaLogs instance. Point your Grafana Loki datasource
            at the proxy URL — nothing else changes in Grafana.
          </p>
        </div>
        <div className={styles.card}>
          <pre className={styles.codeBlock}><code>{helmInstall}</code></pre>
          <p>
            The proxy listens on port 3100 (same as Loki) by default. Update
            your Grafana datasource URL from the Loki service address to the
            proxy service address — no other Grafana config changes required.
          </p>
        </div>
      </section>

      <section className={styles.section}>
        <div className={styles.sectionHeader}>
          <Heading as="h2" className={styles.sectionTitle}>
            Resource sizing: VL + proxy vs Loki
          </Heading>
          <p className={styles.sectionLead}>
            Based on Grafana&apos;s published Loki sizing guide (c7i.4xlarge
            on-demand, us-east-1) and real-tested VictoriaLogs process envelopes.
          </p>
        </div>
        <div className={styles.tableWrap}>
          <table className={styles.comparisonTable}>
            <thead>
              <tr>
                <th>Stack</th>
                <th>CPU</th>
                <th>Memory</th>
                <th>Illustrative EC2 nodes</th>
                <th>Monthly compute floor</th>
              </tr>
            </thead>
            <tbody>
              {resourceRows.map((row) => (
                <tr key={row.component}>
                  <td>{row.component}</td>
                  <td>{row.cpu}</td>
                  <td>{row.memory}</td>
                  <td>{row.nodes}</td>
                  <td>{row.cost}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <p>
          The proxy itself adds a small read-side tax: approximately 15–30 ms
          on a cold cache miss, near-zero on a warm hit. The large compute
          difference comes from VictoriaLogs&apos; all-field indexing model
          versus Loki&apos;s label-indexed stream model.
        </p>
      </section>

      <section className={styles.section}>
        <div className={styles.cardGrid}>
          <div className={styles.card}>
            <h2 className={styles.cardTitle}>Key Helm flags for Kubernetes</h2>
            <ul className={styles.list}>
              <li><code>victorialogs.url</code> — VictoriaLogs service URL (required).</li>
              <li><code>cache.l1.maxBytes</code> — L1 memory cache size (default 256 MB).</li>
              <li><code>cache.l2.enabled</code> — enable bbolt disk cache for persistence across restarts.</li>
              <li><code>cache.l3.enabled</code> — enable peer cache for fleet-wide reuse across replicas.</li>
              <li><code>multitenancy.enabled</code> — enable X-Scope-OrgID tenant isolation.</li>
              <li><code>circuitBreaker.failureThreshold</code> — sliding 30s window threshold (default 5).</li>
            </ul>
          </div>
          <div className={styles.card}>
            <h2 className={styles.cardTitle}>What Grafana keeps working</h2>
            <ul className={styles.list}>
              <li>Native Loki datasource — no plugin swap required.</li>
              <li>Explore with label discovery, <code>detected_fields</code>, and <code>query_range</code>.</li>
              <li>Logs Drilldown with patterns, volume, and field browsing.</li>
              <li>Alert rules bridged through optional vmalert integration.</li>
              <li>Multi-tenant dashboards via <code>X-Scope-OrgID</code> header passthrough.</li>
            </ul>
          </div>
          <div className={styles.card}>
            <h2 className={styles.cardTitle}>Kubernetes-specific considerations</h2>
            <ul className={styles.list}>
              <li>Run 2–3 proxy replicas; L3 peer cache (consistent hash) automatically distributes work across them.</li>
              <li>The proxy supports HPA: it is stateless on the read path when L3 peer cache handles distribution.</li>
              <li>PodDisruptionBudget: keep at least 1 replica warm to avoid cold-start latency spikes.</li>
              <li>VictoriaLogs can run single-node for smaller clusters or as a 3-component cluster (vlinsert, vlselect, vlstorage) for HA.</li>
            </ul>
          </div>
          <div className={styles.card}>
            <h2 className={styles.cardTitle}>OTel logs on Kubernetes</h2>
            <ul className={styles.list}>
              <li>OTel Collector ships logs to VictoriaLogs via the Loki push endpoint or VL JSON endpoint.</li>
              <li>Dotted labels like <code>service.name</code>, <code>k8s.pod.name</code>, and <code>k8s.namespace.name</code> are detected and exposed via <code>detected_fields</code> in Explore.</li>
              <li>Use <code>translation_mode: underscore</code> to surface them as Loki-safe <code>service_name</code> labels in the query builder.</li>
              <li>See the <Link to="/opentelemetry-logs-victorialogs-grafana/">OTel logs guide</Link> for field mapping details.</li>
            </ul>
          </div>
        </div>
      </section>

      <section className={styles.section}>
        <div className={styles.callout}>
          <Heading as="h2" className={styles.sectionTitle}>
            Next steps
          </Heading>
          <div className={styles.inlineLinks}>
            <Link to="/docs/getting-started/">Helm install guide</Link>
            <Link to="/docs/getting-started/">Getting started</Link>
            <Link to="/docs/translation-modes/">Translation modes</Link>
            <Link to="/victorialogs-vs-loki-cost-and-performance/">Cost comparison</Link>
            <Link to="/opentelemetry-logs-victorialogs-grafana/">OTel logs guide</Link>
            <Link to="/docs/fleet-cache/">Fleet cache</Link>
          </div>
        </div>
      </section>
    </MarketingLayout>
  );
}
