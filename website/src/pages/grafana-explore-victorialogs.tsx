import type {ReactNode} from 'react';
import Link from '@docusaurus/Link';
import Heading from '@theme/Heading';
import MarketingLayout from '@site/src/components/MarketingLayout';
import styles from '@site/src/components/marketing.module.css';
import {coreFaqs} from '@site/src/data/faqs';

export default function GrafanaExploreVictoriaLogs(): ReactNode {
  return (
    <MarketingLayout
      path="/grafana-explore-victorialogs/"
      title="Grafana Explore with VictoriaLogs"
      description="Run Grafana Explore against VictoriaLogs through Loki-VL-proxy so query_range, labels, field discovery, and route-aware telemetry stay visible and operable."
      eyebrow="Grafana Explore"
      headline="Keep Grafana Explore usable on top of VictoriaLogs"
      lede="Grafana Explore depends on more than a single query endpoint. It needs label discovery, route consistency, stable field semantics, and enough visibility to see whether slowness is coming from the client path, the proxy, cache misses, or VictoriaLogs itself."
      primaryCta={{label: 'Read the observability guide', to: '/docs/observability/'}}
      secondaryCta={{label: 'See translation modes', to: '/docs/translation-modes/'}}
      highlights={[
        {
          value: 'query_range + metadata',
          label: 'Explore keeps both search and browsing surfaces',
          detail: 'Labels, values, detected_fields, and queries all matter.',
        },
        {
          value: '4-tier cache',
          label: 'Tier0, L1 memory (256 MB), L2 disk, L3 peer — warm hits at 0.64–0.67 µs on query_range',
          detail: 'Repeated Explore sessions stop being backend-bound.',
        },
        {
          value: 'Per-route visibility',
          label: 'Latency, errors, and rate split by normalized downstream and upstream routes',
          detail: 'Good for regressions.',
        },
        {
          value: 'OTel field support',
          label: 'Dotted fields (service.name, k8s.pod.name) available without breaking Loki-safe label paths',
          detail: 'Choose translated, native, or hybrid modes.',
        },
      ]}
      faqs={coreFaqs}
    >
      <section className={styles.section}>
        <div className={styles.cardGrid}>
          <div className={styles.card}>
            <h2 className={styles.cardTitle}>What Explore users feel first</h2>
            <p>
              Slow query_range calls, missing label values, and inconsistent field
              semantics show up in Explore immediately. The proxy exposes those
              user-facing paths with normalized routes so they can be monitored
              instead of guessed at.
            </p>
          </div>
          <div className={styles.card}>
            <h2 className={styles.cardTitle}>What operators should inspect</h2>
            <p>
              Use the route-aware request metrics to separate downstream client
              latency from upstream VictoriaLogs latency, and use cache-hit or
              cache-miss views to see whether expensive browsing paths are staying
              warm enough.
            </p>
          </div>
        </div>
      </section>

      <section className={styles.section}>
        <div className={styles.sectionHeader}>
          <Heading as="h2" className={styles.sectionTitle}>
            The Explore paths worth watching
          </Heading>
        </div>
        <div className={styles.routeStrip}>
          <span>/loki/api/v1/query_range</span>
          <span>/loki/api/v1/labels</span>
          <span>/loki/api/v1/label/&lt;name&gt;/values</span>
          <span>/loki/api/v1/series</span>
          <span>/loki/api/v1/detected_fields</span>
          <span>/loki/api/v1/index/volume</span>
        </div>
        <div className={styles.cardGrid}>
          <div className={styles.card}>
            <h3 className={styles.cardTitle}>Latency</h3>
            <p>
              Compare end-to-end request latency with backend-only latency. The
              difference is where translation work, shaping, coalescing, and cache
              lookup time live.
            </p>
          </div>
          <div className={styles.card}>
            <h3 className={styles.cardTitle}>Errors</h3>
            <p>
              Separate client-side bad requests from upstream failures. That keeps
              Grafana builder regressions distinct from VictoriaLogs or network
              pain.
            </p>
          </div>
          <div className={styles.card}>
            <h3 className={styles.cardTitle}>Cache efficiency</h3>
            <p>
              Explore tends to stress repeated metadata browsing. Route-aware hit
              and miss metrics are what tell you whether labels, field values, and
              query paths should be tuned or warmed differently.
            </p>
          </div>
          <div className={styles.card}>
            <h3 className={styles.cardTitle}>Translation profile drift</h3>
            <p>
              If users click dotted OTel fields in Explore, validate whether your
              deployment should stay in a hybrid metadata mode or fully translated
              mode. The wrong choice shows up as broken click-to-filter behavior.
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
            <Link to="/docs/observability/">Observability</Link>
            <Link to="/docs/translation-modes/">Translation Modes</Link>
            <Link to="/docs/compatibility-loki/">Loki Compatibility</Link>
            <Link to="/docs/fleet-cache/">Fleet Cache</Link>
          </div>
        </div>
      </section>
    </MarketingLayout>
  );
}
