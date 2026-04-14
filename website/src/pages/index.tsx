import type {ReactNode} from 'react';
import Link from '@docusaurus/Link';
import Heading from '@theme/Heading';
import ThemedImage from '@theme/ThemedImage';
import useBaseUrl from '@docusaurus/useBaseUrl';
import MarketingLayout from '@site/src/components/MarketingLayout';
import styles from '@site/src/components/marketing.module.css';
import {coreFaqs} from '@site/src/data/faqs';

const softwareSchema = {
  '@context': 'https://schema.org',
  '@type': 'SoftwareApplication',
  name: 'Loki-VL-proxy',
  applicationCategory: 'DeveloperApplication',
  operatingSystem: 'Linux, Kubernetes, Docker',
  softwareVersion: '1.x',
  description:
    "Loki-compatible read proxy for VictoriaLogs. Keep Grafana's native Loki datasource, Explore, and Logs Drilldown while querying VictoriaLogs.",
  url: 'https://reliablyobserve.github.io/Loki-VL-proxy/',
  codeRepository: 'https://github.com/ReliablyObserve/Loki-VL-proxy',
  license: 'https://github.com/ReliablyObserve/Loki-VL-proxy/blob/main/LICENSE',
  isAccessibleForFree: true,
};

const practicalGuides = [
  {
    to: '/run-loki-vl-proxy-for-victorialogs/',
    title: 'Run Loki-VL-proxy',
    body: 'Start with a local binary or container, prove the Loki-compatible read path, and verify readiness before any Grafana cutover.',
  },
  {
    to: '/grafana-loki-datasource-victorialogs/',
    title: 'Set Up the Grafana Datasource',
    body: 'Keep Grafana on the native Loki datasource and point it at the proxy, then validate translation, labels, metadata, and latency.',
  },
  {
    to: '/install-loki-vl-proxy-with-helm/',
    title: 'Install with Helm',
    body: 'Choose the right chart topology for day one: basic Deployment, StatefulSet with disk cache, or multi-replica peer-cache fleet.',
  },
  {
    to: '/monitor-loki-vl-proxy/',
    title: 'Monitor the Proxy',
    body: 'See downstream, proxy, cache, and upstream behavior with route-aware metrics, logs, and runtime resource signals.',
  },
  {
    to: '/migrate-grafana-from-loki-to-victorialogs/',
    title: 'Migrate from Loki',
    body: 'Use a parallel datasource path, validate Grafana workflows, and cut over only after route-aware checks are green.',
  },
  {
    to: '/cache-tiers-and-fleet-cache-for-victorialogs/',
    title: 'Use Cache and Fleet Cache',
    body: 'Understand how Tier0, local cache, disk cache, peer cache, and long-range window reuse reduce repeated backend work.',
  },
];

const intentPages = [
  {
    to: '/loki-proxy-for-victorialogs/',
    title: 'Loki Proxy for VictoriaLogs',
    body: 'The core deployment pattern: put a Loki-compatible read proxy in front of VictoriaLogs so Grafana and Loki API clients stay unchanged.',
  },
  {
    to: '/grafana-loki-datasource-victorialogs/',
    title: 'Grafana Loki Datasource for VictoriaLogs',
    body: 'Exact datasource-level answer for teams searching how to use VictoriaLogs without building a custom Grafana plugin.',
  },
  {
    to: '/grafana-explore-victorialogs/',
    title: 'Grafana Explore with VictoriaLogs',
    body: 'How query_range, labels, label values, and latency or cache visibility behave when Grafana Explore sits on top of the proxy.',
  },
  {
    to: '/victorialogs-with-grafana-loki-datasource/',
    title: 'VictoriaLogs with Grafana Loki Datasource',
    body: 'The deployment pattern for keeping Grafana on the Loki datasource while routing reads through Loki-VL-proxy into VictoriaLogs.',
  },
  {
    to: '/logs-drilldown-victorialogs/',
    title: 'Logs Drilldown with VictoriaLogs',
    body: 'Patterns, detected fields, service buckets, and the compatibility contracts that keep Grafana Logs Drilldown usable on VictoriaLogs.',
  },
  {
    to: '/logql-on-victorialogs/',
    title: 'LogQL on VictoriaLogs',
    body: 'Translation modes, dot-versus-underscore semantics, and the limits you need to understand before treating VictoriaLogs as a Loki read backend.',
  },
  {
    to: '/loki-vs-victorialogs-grafana-query-workflows/',
    title: 'Loki vs VictoriaLogs for Grafana Query Workflows',
    body: 'Operational comparison of a native Loki backend versus VictoriaLogs routed through Loki-VL-proxy for Grafana query paths.',
  },
];

const operatorCards = [
  {
    title: 'Compatibility is explicit, not implied',
    body: 'The project keeps separate compatibility tracks for Grafana Loki datasource behavior, Logs Drilldown, and VictoriaLogs runtime bands. CI exercises the contracts instead of relying on vague claims.',
  },
  {
    title: 'Translation stays bounded',
    body: 'Route-aware metrics, structured logs, tuple contracts, and label or metadata translation modes are built so operators can see where the proxy adds work and where it passes VictoriaLogs behavior through safely.',
  },
  {
    title: 'Caching is part of the product surface',
    body: 'Tier0 compatibility cache, in-memory cache, disk cache, and optional peer or fleet cache are first-class operational levers, not hidden implementation details.',
  },
  {
    title: 'Observability follows the runtime path',
    body: 'The packaged dashboard follows Client -> Proxy -> VictoriaLogs and breaks out route, latency, errors, cache efficiency, and operational resources for both Prometheus pull and OTLP push setups.',
  },
];

export default function Home(): ReactNode {
  const lightBrand = useBaseUrl('/img/loki-vl-proxy-logo-white.jpg');
  const darkBrand = useBaseUrl('/img/loki-vl-proxy-logo-black.jpg');

  return (
    <MarketingLayout
      path="/"
      title="Loki Proxy for VictoriaLogs"
      description="Use Grafana's native Loki datasource, Explore, and Logs Drilldown with VictoriaLogs through a Loki-compatible read proxy. No custom plugin required."
      eyebrow="Grafana + VictoriaLogs"
      headline="Loki Proxy for VictoriaLogs"
      lede="Use Grafana's native Loki datasource, Explore, and Logs Drilldown with VictoriaLogs. No plugin required."
      primaryCta={{label: 'Run the proxy first', to: '/run-loki-vl-proxy-for-victorialogs/'}}
      secondaryCta={{label: 'Open the docs', to: '/docs/getting-started/'}}
      highlights={[
        {
          value: 'Native Loki datasource',
          label: 'Grafana keeps the built-in Loki datasource type',
          detail: 'No plugin or UI fork.',
        },
        {
          value: 'Explore + Drilldown',
          label: 'Compatibility is tested against the user-facing Grafana flows',
          detail: 'Datasource, Explore, Drilldown, patterns.',
        },
        {
          value: 'Read-only by design',
          label: 'Query, metadata, patterns, and rules or alerts read views are in scope',
          detail: 'Push remains blocked.',
        },
        {
          value: 'Operator-first telemetry',
          label: 'Metrics and logs expose downstream, proxy, cache, and upstream behavior',
          detail: 'Prometheus pull and OTLP push both work.',
        },
      ]}
      faqs={coreFaqs}
      structuredData={[softwareSchema]}
    >
      <section className={styles.section}>
        <div className={styles.brandShowcase}>
          <ThemedImage
            alt="Loki-VL-proxy marketing logo"
            className={styles.brandArt}
            sources={{
              light: lightBrand,
              dark: darkBrand,
            }}
          />
        </div>
      </section>

      <section className={styles.section}>
        <div className={styles.sectionHeader}>
          <Heading as="h2" className={styles.sectionTitle}>
            What this project actually solves
          </Heading>
          <p className={styles.sectionLead}>
            Loki-VL-proxy exists for teams that want VictoriaLogs as the backend
            while preserving Grafana and Loki client workflows that already
            assume the Loki HTTP API.
          </p>
        </div>
        <div className={styles.cardGrid}>
          <div className={styles.card}>
            <h3 className={styles.cardTitle}>Keep Grafana workflows intact</h3>
            <p>
              Grafana continues to use the native Loki datasource, Explore, and
              Logs Drilldown paths. The proxy handles the read-side compatibility
              and translation work in the middle.
            </p>
          </div>
          <div className={styles.card}>
            <h3 className={styles.cardTitle}>Expose VictoriaLogs safely</h3>
            <p>
              VictoriaLogs remains the source of log data while the proxy shapes
              labels, metadata, patterns, rules, and alerts views into Loki-like
              contracts where those contracts matter.
            </p>
          </div>
          <div className={styles.card}>
            <h3 className={styles.cardTitle}>Handle dotted OTel fields</h3>
            <p>
              The project supports translation profiles for dotted VictoriaLogs
              fields and Loki-safe underscore labels so operators can keep Grafana
              usable without giving up native field semantics upstream.
            </p>
          </div>
          <div className={styles.card}>
            <h3 className={styles.cardTitle}>Make the proxy observable</h3>
            <p>
              Route-aware metrics and structured logs break out client latency,
              proxy overhead, upstream slowness, and cache behavior so the
              translation layer does not become a blind spot.
            </p>
          </div>
        </div>
      </section>

      <section className={styles.section}>
        <div className={styles.sectionHeader}>
          <Heading as="h2" className={styles.sectionTitle}>
            Practical operator guides
          </Heading>
          <p className={styles.sectionLead}>
            These pages are written as real deployment and migration playbooks,
            not just keyword landing pages. They map directly onto the docs,
            chart recipes, cache model, and observability surface the project
            already ships.
          </p>
        </div>
        <div className={styles.cardGrid}>
          {practicalGuides.map((page) => (
            <Link key={page.to} className={`${styles.card} ${styles.linkCard}`} to={page.to}>
              <h3 className={styles.cardTitle}>{page.title}</h3>
              <p>{page.body}</p>
            </Link>
          ))}
        </div>
      </section>

      <section className={styles.section}>
        <div className={styles.sectionHeader}>
          <Heading as="h2" className={styles.sectionTitle}>
            Search-focused entry points
          </Heading>
          <p className={styles.sectionLead}>
            These pages answer the exact discovery queries operators and Grafana
            users search for when they are trying to make VictoriaLogs work
            behind Loki-compatible tooling.
          </p>
        </div>
        <div className={styles.cardGrid}>
          {intentPages.map((page) => (
            <Link key={page.to} className={`${styles.card} ${styles.linkCard}`} to={page.to}>
              <h3 className={styles.cardTitle}>{page.title}</h3>
              <p>{page.body}</p>
            </Link>
          ))}
        </div>
      </section>

      <section className={styles.section}>
        <div className={styles.sectionHeader}>
          <Heading as="h2" className={styles.sectionTitle}>
            The path operators actually run
          </Heading>
          <p className={styles.sectionLead}>
            Keep the path simple: clients and Grafana on the left, the
            compatibility and cache layer in the middle, VictoriaLogs and
            optional rules backends on the right.
          </p>
        </div>
        <div className={styles.routeStrip}>
          <span>Client or Grafana</span>
          <span>Loki-compatible read API</span>
          <span>Translation and shaping</span>
          <span>Tier0 and deeper cache</span>
          <span>VictoriaLogs and vmalert</span>
        </div>
      </section>

      <section className={styles.section}>
        <div className={styles.sectionHeader}>
          <Heading as="h2" className={styles.sectionTitle}>
            Why operators keep this layer visible
          </Heading>
          <p className={styles.sectionLead}>
            The proxy is not just a protocol adapter. It carries compatibility
            contracts, latency, and cache behavior that need to be observed as a
            real production component.
          </p>
        </div>
        <div className={styles.cardGrid}>
          {operatorCards.map((card) => (
            <div key={card.title} className={styles.card}>
              <h3 className={styles.cardTitle}>{card.title}</h3>
              <p>{card.body}</p>
            </div>
          ))}
        </div>
      </section>

      <section className={styles.section}>
        <div className={styles.callout}>
          <Heading as="h2" className={styles.sectionTitle}>
            Start from the practical guides, then drop into the deep docs
          </Heading>
          <p className={styles.sectionLead}>
            The landing pages should get you to the right operating model fast.
            The deep docs still cover exact flags, compatibility matrices,
            benchmarks, scaling decisions, and runbooks.
          </p>
          <div className={styles.inlineLinks}>
            <Link to="/docs/getting-started/">Getting Started</Link>
            <Link to="/docs/operations/">Operations</Link>
            <Link to="/docs/observability/">Observability</Link>
            <Link to="/docs/performance/">Performance</Link>
            <Link to="/docs/fleet-cache/">Fleet Cache</Link>
          </div>
        </div>
      </section>
    </MarketingLayout>
  );
}
