import type {ReactNode} from 'react';
import Link from '@docusaurus/Link';
import Heading from '@theme/Heading';
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
  {
    to: '/migrate-grafana-from-loki-to-victorialogs/',
    title: 'Migrate Grafana from Loki to VictoriaLogs',
    body: 'Step-by-step migration path for moving Grafana read workflows from Loki to VictoriaLogs with route-aware validation and rollback control.',
  },
];

const operatorCards = [
  {
    title: 'Compatibility is explicit, not implied',
    body: 'The project keeps separate compatibility tracks for Grafana Loki datasource behavior, Logs Drilldown, and VictoriaLogs runtime bands. CI exercises the contracts instead of relying on hand-wavy claims.',
  },
  {
    title: 'Translation stays bounded',
    body: 'Route-aware metrics, structured logs, tuple contracts, and label or metadata translation modes are built so operators can see where the proxy adds work and where it passes VictoriaLogs behavior through safely.',
  },
  {
    title: 'Caching is part of the product surface',
    body: 'Tier-0 compatibility cache, in-memory cache, disk cache, and optional peer or fleet cache are first-class operational levers, not hidden implementation details.',
  },
  {
    title: 'Observability already matches the path',
    body: 'The packaged dashboard follows Client -> Proxy -> VictoriaLogs and breaks out route, latency, errors, cache efficiency, and operational resources for both Prometheus pull and OTLP push setups.',
  },
];

const journeySteps = [
  {
    title: '1. Stand up the proxy',
    body: 'Use the binary, container image, or Helm chart. The proxy stays read-only and points to VictoriaLogs plus optional rules or alerts backends.',
  },
  {
    title: '2. Keep Grafana on the Loki datasource',
    body: 'Grafana keeps its native Loki datasource type. The proxy translates the read paths, tuple contracts, and metadata surfaces Grafana expects.',
  },
  {
    title: '3. Validate with route-aware visibility',
    body: 'Observe downstream client latency, upstream VictoriaLogs latency, translation costs, and cache hit or miss behavior before cutting traffic over.',
  },
];

export default function Home(): ReactNode {
  return (
    <MarketingLayout
      path="/"
      title="Loki Proxy for VictoriaLogs"
      description="Use Grafana's native Loki datasource, Explore, and Logs Drilldown with VictoriaLogs through a Loki-compatible read proxy. No custom plugin required."
      eyebrow="Grafana + VictoriaLogs"
      headline="Loki Proxy for VictoriaLogs"
      lede="Use Grafana's native Loki datasource, Explore, and Logs Drilldown with VictoriaLogs. No plugin required."
      primaryCta={{label: 'Start with the docs', to: '/docs/getting-started/'}}
      secondaryCta={{label: 'See the deployment pattern', to: '/loki-proxy-for-victorialogs/'}}
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
          label: 'Query, metadata, patterns, and rules/alerts read views are in scope',
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
            Search-focused entry points
          </Heading>
          <p className={styles.sectionLead}>
            These pages answer the exact questions operators, Grafana users, and
            migration teams search for when they are trying to make VictoriaLogs
            work behind Loki-compatible tooling.
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
            The product is easiest to reason about when you keep the path simple:
            clients and Grafana on the left, the compatibility layer in the
            middle, VictoriaLogs and optional rules backends on the right.
          </p>
        </div>
        <div className={styles.cardGrid}>
          {journeySteps.map((step) => (
            <div key={step.title} className={styles.card}>
              <h3 className={styles.cardTitle}>{step.title}</h3>
              <p>{step.body}</p>
            </div>
          ))}
        </div>
        <div className={styles.routeStrip}>
          <span>Client or Grafana</span>
          <span>Loki-compatible read API</span>
          <span>Translation and shaping</span>
          <span>Tiered cache</span>
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
            Start from the deep docs when you need implementation detail
          </Heading>
          <p className={styles.sectionLead}>
            The landing pages are meant to answer intent quickly. The deeper docs
            stay in the repository and cover deployment, configuration, field
            translation, compatibility matrices, patterns, observability, and
            operations in detail.
          </p>
          <div className={styles.inlineLinks}>
            <Link to="/docs/getting-started/">Getting Started</Link>
            <Link to="/docs/compatibility-matrix/">Compatibility Matrix</Link>
            <Link to="/docs/observability/">Observability</Link>
            <Link to="/docs/operations/">Operations</Link>
            <Link to="/docs/translation-modes/">Translation Modes</Link>
            <Link to="/docs/patterns/">Patterns</Link>
          </div>
        </div>
      </section>
    </MarketingLayout>
  );
}
