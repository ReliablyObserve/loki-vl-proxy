import type {ReactNode} from 'react';
import Link from '@docusaurus/Link';
import Heading from '@theme/Heading';
import MarketingLayout from '@site/src/components/MarketingLayout';
import styles from '@site/src/components/marketing.module.css';
import {coreFaqs} from '@site/src/data/faqs';

const downstreamP95 = `histogram_quantile(
  0.95,
  sum by (le, route) (
    rate(loki_vl_proxy_request_duration_seconds_bucket{
      system="loki",
      direction="downstream"
    }[5m])
  )
)`;

const upstreamP95 = `histogram_quantile(
  0.95,
  sum by (le, route) (
    rate(loki_vl_proxy_backend_duration_seconds_bucket{
      system="vl",
      direction="upstream"
    }[5m])
  )
)`;

const cacheRatio = `sum by (route) (
  rate(loki_vl_proxy_cache_hits_by_endpoint{
    system="loki",
    direction="downstream"
  }[5m])
)
/
clamp_min(
  sum by (route) (
    rate(loki_vl_proxy_cache_hits_by_endpoint{
      system="loki",
      direction="downstream"
    }[5m]) +
    rate(loki_vl_proxy_cache_misses_by_endpoint{
      system="loki",
      direction="downstream"
    }[5m])
  ),
  1
)`;

const metricRows = [
  {
    metric: '`loki_vl_proxy_requests_total`',
    use: 'Request rate and error rate by `system`, `direction`, `endpoint`, `route`, and `status`.',
  },
  {
    metric: '`loki_vl_proxy_request_duration_seconds`',
    use: 'Client-visible end-to-end latency on each normalized downstream route.',
  },
  {
    metric: '`loki_vl_proxy_backend_duration_seconds`',
    use: 'VictoriaLogs or rules-backend latency on the upstream side, split by route.',
  },
  {
    metric: '`loki_vl_proxy_cache_hits_by_endpoint` / `loki_vl_proxy_cache_misses_by_endpoint`',
    use: 'Route-aware cache efficiency for label browsing, metadata, patterns, and query paths.',
  },
  {
    metric: '`loki_vl_proxy_window_*`',
    use: 'Long-range `query_range` window cache, prefilter, merge, and adaptive parallelism behavior.',
  },
  {
    metric: '`loki_vl_proxy_peer_cache_*`',
    use: 'Fleet-cache hits, misses, peer failures, and cluster member counts in multi-replica topologies.',
  },
  {
    metric: '`loki_vl_proxy_process_*`',
    use: 'Runtime CPU, memory, disk, network, PSI, and file-descriptor health for the proxy itself.',
  },
];

export default function MonitorLokiVLProxy(): ReactNode {
  return (
    <MarketingLayout
      path="/monitor-loki-vl-proxy/"
      title="Monitor Loki-VL-proxy"
      description="Monitor Loki-VL-proxy as Client -> Proxy -> VictoriaLogs with route-aware latency, rate, errors, cache efficiency, fleet-cache health, and runtime resource metrics."
      eyebrow="Monitoring guide"
      headline="Monitor Loki-VL-proxy as Client -> Proxy -> VictoriaLogs"
      lede="The right monitoring model is not a flat pile of proxy counters. It is a flow: downstream client demand on the left, proxy translation and cache behavior in the middle, and upstream VictoriaLogs latency or errors on the right."
      primaryCta={{label: 'Open the observability docs', to: '/docs/observability/'}}
      secondaryCta={{label: 'See the operations guide', to: '/docs/operations/'}}
      highlights={[
        {
          value: 'Route-aware labels',
          label: 'The main request metrics split by `system`, `direction`, `endpoint`, `route`, and `status`',
          detail: 'Use route, not only endpoint, in dashboards.',
        },
        {
          value: 'Client vs upstream',
          label: 'End-to-end and backend latency are separate histograms',
          detail: 'This is how you isolate proxy overhead from VictoriaLogs slowness.',
        },
        {
          value: 'Cache layers visible',
          label: 'Hits, misses, window cache, peer cache, and resource metrics are all exported',
          detail: 'Useful for cost and latency tuning.',
        },
        {
          value: 'Logs fill the gap',
          label: 'Structured request logs carry `proxy.overhead_ms`, `proxy.duration_ms`, and `upstream.duration_ms`',
          detail: 'Per-request decomposition belongs in logs.',
        },
      ]}
      faqs={coreFaqs}
    >
      <section className={styles.section}>
        <div className={styles.routeStrip}>
          <span>Clients and Grafana</span>
          <span>Downstream rate, status, latency</span>
          <span>Proxy translation and cache behavior</span>
          <span>Upstream VictoriaLogs latency and errors</span>
          <span>Proxy resources and fleet health</span>
        </div>
      </section>

      <section className={styles.section}>
        <div className={styles.tableWrap}>
          <table className={styles.comparisonTable}>
            <thead>
              <tr>
                <th>Metric family</th>
                <th>What it answers operationally</th>
              </tr>
            </thead>
            <tbody>
              {metricRows.map((row) => (
                <tr key={row.metric}>
                  <td>{row.metric}</td>
                  <td>{row.use}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      <section className={styles.section}>
        <div className={styles.cardGrid}>
          <div className={styles.card}>
            <h2 className={styles.cardTitle}>Downstream p95 by route</h2>
            <p>
              Start here when users say Grafana feels slow. This is the
              client-visible latency across normalized Loki routes.
            </p>
            <pre className={styles.codeBlock}>
              <code>{downstreamP95}</code>
            </pre>
          </div>
          <div className={styles.card}>
            <h2 className={styles.cardTitle}>Upstream p95 by route</h2>
            <p>
              Use this to see whether the pain is really VictoriaLogs or rules
              backend slowness rather than the proxy path itself.
            </p>
            <pre className={styles.codeBlock}>
              <code>{upstreamP95}</code>
            </pre>
          </div>
          <div className={styles.card}>
            <h2 className={styles.cardTitle}>Cache hit ratio by route</h2>
            <p>
              Use this on metadata-heavy and repeated dashboard paths to see
              whether the cache stack is actually reducing backend work.
            </p>
            <pre className={styles.codeBlock}>
              <code>{cacheRatio}</code>
            </pre>
          </div>
          <div className={styles.card}>
            <h2 className={styles.cardTitle}>What logs add that metrics do not</h2>
            <ul className={styles.list}>
              <li>`http.route` for the normalized path.</li>
              <li>`loki.api.system` and `proxy.direction` for path orientation.</li>
              <li>`proxy.overhead_ms` for proxy-only time on a request.</li>
              <li>`upstream.duration_ms` when the backend call exists.</li>
            </ul>
          </div>
        </div>
      </section>

      <section className={styles.section}>
        <div className={styles.columns}>
          <div className={styles.card}>
            <h2 className={styles.cardTitle}>The questions operators should answer quickly</h2>
            <ul className={styles.list}>
              <li>Which downstream routes are slow or erroring right now?</li>
              <li>Is VictoriaLogs slow on the same routes or only the proxy path?</li>
              <li>Which routes are missing cache and forcing backend work repeatedly?</li>
              <li>Are peer-cache failures or resource saturation pushing more traffic upstream?</li>
            </ul>
          </div>
          <div className={styles.card}>
            <h2 className={styles.cardTitle}>Resource view that stays consistent</h2>
            <ul className={styles.list}>
              <li>Use `loki_vl_proxy_process_*` families for CPU, memory, disk, network, and PSI.</li>
              <li>Read network and disk as up/down time-series, not only point-in-time stats.</li>
              <li>Watch file descriptors and resident memory by pod for slow leak or churn patterns.</li>
              <li>Keep runtime charts beside route health so regressions are easier to correlate.</li>
            </ul>
          </div>
        </div>
      </section>

      <section className={styles.section}>
        <div className={styles.callout}>
          <Heading as="h2" className={styles.sectionTitle}>
            Monitoring follow-up docs
          </Heading>
          <div className={styles.inlineLinks}>
            <Link to="/cache-tiers-and-fleet-cache-for-victorialogs/">Cache and fleet-cache guide</Link>
            <Link to="/docs/runbooks/alerts/">Runbooks</Link>
            <Link to="/docs/runbooks/loki-vl-proxy-high-latency/">High latency runbook</Link>
            <Link to="/docs/runbooks/loki-vl-proxy-system-resources/">System resources runbook</Link>
          </div>
        </div>
      </section>
    </MarketingLayout>
  );
}
