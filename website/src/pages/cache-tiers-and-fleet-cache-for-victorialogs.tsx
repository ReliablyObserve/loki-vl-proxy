import type {ReactNode} from 'react';
import Link from '@docusaurus/Link';
import Heading from '@theme/Heading';
import MarketingLayout from '@site/src/components/MarketingLayout';
import styles from '@site/src/components/marketing.module.css';
import {coreFaqs} from '@site/src/data/faqs';

const benchmarkRows = [
  {
    path: 'query_range',
    slow: '4.58 ms cold miss with delayed backend',
    fast: '0.64-0.67 us warm cache hit',
    meaning: 'Repeated dashboards stop behaving like backend-bound requests.',
  },
  {
    path: 'detected_field_values',
    slow: '2.76 ms without Tier0',
    fast: '0.71 us with Tier0',
    meaning: 'Drilldown metadata becomes effectively instant after warm-up.',
  },
  {
    path: 'L2 disk cache',
    slow: 'backend refill path',
    fast: '0.45 us uncompressed read, 3.9 us compressed read',
    meaning: 'Persistent cache stays cheap enough to matter on hot paths.',
  },
  {
    path: 'L3 peer cache',
    slow: 'backend or owner refetch',
    fast: '52 ns warm shadow-copy hit',
    meaning: 'A warm fleet can reuse work instead of repeating it.',
  },
];

export default function CacheTiersAndFleetCacheForVictoriaLogs(): ReactNode {
  return (
    <MarketingLayout
      path="/cache-tiers-and-fleet-cache-for-victorialogs/"
      title="Cache Tiers and Fleet Cache for VictoriaLogs"
      description="Understand how Tier0, L1, L2, L3, and long-range query window cache reduce repeated VictoriaLogs work and improve user-visible latency on Loki-compatible read paths."
      eyebrow="Cache and cost control"
      headline="Use cache tiers and fleet cache to suppress repeated backend work"
      lede="The strongest practical efficiency story in Loki-VL-proxy is not a generic head-to-head marketing claim. It is the concrete read-path work the proxy can eliminate with its 4-tier cache stack: Tier0 (compat), L1 in-memory (256 MB default), L2 disk (bbolt), and L3 peer cache (consistent hash + zstd) — plus a circuit breaker and request coalescer that protect the backend under load."
      primaryCta={{label: 'Open the performance docs', to: '/docs/performance/'}}
      secondaryCta={{label: 'Read fleet-cache architecture', to: '/docs/fleet-cache/'}}
      highlights={[
        {
          value: 'Tier0 compat cache',
          label: 'Safe GET Loki-shaped responses can return before most compatibility work runs',
          detail: 'Best for hot repeated read paths.',
        },
        {
          value: 'L1: 256 MB in-memory',
          label: 'Hot in-process LRU cache with near-zero overhead on repeated dashboard hits',
          detail: 'Default 256 MB, tunable per deployment.',
        },
        {
          value: 'L2: bbolt disk cache',
          label: 'Persistent local cache survives RAM pressure and pod restarts',
          detail: '0.45 µs uncompressed read, 3.9 µs compressed read.',
        },
        {
          value: 'L3: peer cache (consistent hash + zstd)',
          label: 'Fleet-wide reuse across replicas via consistent-hash ownership and zstd-compressed transfer',
          detail: '52 ns warm shadow-copy hit after first owner fetch.',
        },
      ]}
      faqs={coreFaqs}
    >
      <section className={styles.section}>
        <div className={styles.tableWrap}>
          <table className={styles.comparisonTable}>
            <thead>
              <tr>
                <th>Layer</th>
                <th>Plain-English role</th>
                <th>What it buys you</th>
              </tr>
            </thead>
            <tbody>
              <tr>
                <td>Tier0 (compat)</td>
                <td>Fast answer cache at the Loki-compatible frontend — keyed on exact request shape.</td>
                <td>Repeated Grafana reads can return before most proxy logic runs. <code>query_range</code> warm hit: 0.64–0.67 µs.</td>
              </tr>
              <tr>
                <td>L1 in-memory (256 MB default)</td>
                <td>Hot LRU cache inside the local process.</td>
                <td>Best-case latency for repeated dashboards and Explore refreshes. Tunable per deployment.</td>
              </tr>
              <tr>
                <td>L2 disk (bbolt)</td>
                <td>Persistent local cache backed by bbolt B-tree.</td>
                <td>Survives RAM pressure and pod restarts. 0.45 µs uncompressed, 3.9 µs compressed read.</td>
              </tr>
              <tr>
                <td>L3 peer cache (consistent hash + zstd)</td>
                <td>Fleet-wide reuse between replicas using consistent-hash ownership and zstd-compressed transfer.</td>
                <td>52 ns warm shadow-copy hit. One warm pod serves the rest of the fleet without backend round-trips.</td>
              </tr>
            </tbody>
          </table>
        </div>
      </section>

      <section className={styles.section}>
        <div className={styles.tableWrap}>
          <table className={styles.comparisonTable}>
            <thead>
              <tr>
                <th>Path</th>
                <th>Slow path</th>
                <th>Fast path</th>
                <th>Why it matters</th>
              </tr>
            </thead>
            <tbody>
              {benchmarkRows.map((row) => (
                <tr key={row.path}>
                  <td>{row.path}</td>
                  <td>{row.slow}</td>
                  <td>{row.fast}</td>
                  <td>{row.meaning}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      <section className={styles.section}>
        <div className={styles.cardGrid}>
          <div className={styles.card}>
            <h2 className={styles.cardTitle}>Circuit breaker and request coalescer</h2>
            <ul className={styles.list}>
              <li>Sliding 30s window circuit breaker opens after 5 failures, shielding VictoriaLogs from cascading retries.</li>
              <li>Request coalescer deduplicates in-flight identical requests — concurrent Grafana panel refreshes become a single backend call.</li>
              <li>Prefilter eliminates ~81.6% of empty-window backend calls on long-range queries before they reach VictoriaLogs.</li>
              <li>These mechanisms work alongside the cache tiers: coalescing prevents parallel requests from causing redundant cache misses.</li>
            </ul>
          </div>
          <div className={styles.card}>
            <h2 className={styles.cardTitle}>Where the proxy can be cheaper than an uncached read path</h2>
            <ul className={styles.list}>
              <li>Repeated dashboards hammering the same `query_range` windows.</li>
              <li>Explore or Drilldown metadata paths that users refresh over and over.</li>
              <li>Replica fleets where the same query otherwise fans out into repeated backend calls.</li>
              <li>Long-range historical reads that benefit from split-window reuse and prefiltering.</li>
            </ul>
          </div>
          <div className={styles.card}>
            <h2 className={styles.cardTitle}>What the project does not claim</h2>
            <ul className={styles.list}>
              <li>It does not publish a blanket native Loki versus VictoriaLogs total-cost benchmark.</li>
              <li>It does not claim every workload is faster through a compatibility layer.</li>
              <li>It does claim explicit cache, coalescing, and route-aware tuning levers on the read path.</li>
              <li>It does publish the benchmark and runtime signals needed to judge those levers honestly.</li>
            </ul>
          </div>
          <div className={styles.card}>
            <h2 className={styles.cardTitle}>Metrics that prove cache value</h2>
            <ul className={styles.list}>
              <li>`loki_vl_proxy_cache_hits_by_endpoint` and `_misses_by_endpoint` by route.</li>
              <li>`loki_vl_proxy_window_cache_hit_total` and `_miss_total` for long-range queries.</li>
              <li>`loki_vl_proxy_window_fetch_seconds` and `_merge_seconds` for range-work cost.</li>
              <li>`loki_vl_proxy_peer_cache_hits_total`, `_misses_total`, and `_errors_total` for fleet behavior.</li>
            </ul>
          </div>
          <div className={styles.card}>
            <h2 className={styles.cardTitle}>How to think about cost control</h2>
            <p>
              The practical cost story is about suppressing repeated backend
              work, not about hiding the backend. When cache hit ratio rises on
              hot routes, VictoriaLogs work per user action goes down and user
              latency usually follows.
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
            <Link to="/monitor-loki-vl-proxy/">Monitoring guide</Link>
            <Link to="/docs/peer-cache-design/">Peer-cache design</Link>
            <Link to="/docs/scaling/">Scaling</Link>
            <Link to="/docs/benchmarks/">Benchmarks</Link>
          </div>
        </div>
      </section>
    </MarketingLayout>
  );
}
