import type {ReactNode} from 'react';
import Link from '@docusaurus/Link';
import Heading from '@theme/Heading';
import MarketingLayout from '@site/src/components/MarketingLayout';
import styles from '@site/src/components/marketing.module.css';
import {coreFaqs} from '@site/src/data/faqs';

const localRun = `go build -o loki-vl-proxy ./cmd/proxy
./loki-vl-proxy -backend=http://127.0.0.1:9428`;

const dockerRun = `docker run --rm -p 3100:3100 ghcr.io/reliablyobserve/loki-vl-proxy:<release> \\
  -backend=http://host.docker.internal:9428`;

const verifyRun = `curl -sS http://127.0.0.1:3100/ready
curl -sS http://127.0.0.1:3100/loki/api/v1/labels`;

export default function RunLokiVLProxyForVictoriaLogs(): ReactNode {
  return (
    <MarketingLayout
      path="/run-loki-vl-proxy-for-victorialogs/"
      title="How to Run Loki-VL-proxy for VictoriaLogs"
      description="Run Loki-VL-proxy against VictoriaLogs with a local binary, container image, or Helm chart, then verify the Loki-compatible read path before wiring Grafana."
      eyebrow="Run guide"
      headline="Run Loki-VL-proxy against VictoriaLogs without changing Grafana yet"
      lede="The safest first step is simple: stand up the read-only proxy, point it at VictoriaLogs, confirm the Loki-compatible routes are healthy, then wire Grafana or other clients only after the basic path is proven."
      primaryCta={{label: 'Open the getting started guide', to: '/docs/getting-started/'}}
      secondaryCta={{label: 'Install with Helm', to: '/install-loki-vl-proxy-with-helm/'}}
      highlights={[
        {
          value: 'Binary or container',
          label: 'The first boot path can stay minimal',
          detail: 'You only need the backend URL to prove the read path.',
        },
        {
          value: 'Read-only surface',
          label: 'The proxy is built for query and metadata compatibility, not ingest ownership',
          detail: 'Push remains blocked.',
        },
        {
          value: ':3100 frontend',
          label: 'Grafana and Loki clients talk to the proxy on the Loki-facing side',
          detail: 'VictoriaLogs stays on the backend side.',
        },
        {
          value: 'Health first',
          label: 'Validate /ready and a simple labels call before doing anything more elaborate',
          detail: 'This catches the obvious wiring failures early.',
        },
      ]}
      faqs={coreFaqs}
    >
      <section className={styles.section}>
        <div className={styles.cardGrid}>
          <div className={styles.card}>
            <h2 className={styles.cardTitle}>Start with a local binary when you want the shortest feedback loop</h2>
            <p>
              This is the best path for quickly proving backend reachability,
              checking translation behavior, and reading logs directly on a
              workstation or throwaway VM.
            </p>
            <pre className={styles.codeBlock}>
              <code>{localRun}</code>
            </pre>
          </div>
          <div className={styles.card}>
            <h2 className={styles.cardTitle}>Use the container image when you want the exact packaged runtime</h2>
            <p>
              This is the cleanest way to validate the release image before you
              move into Kubernetes, Helm, or a managed deployment target.
            </p>
            <pre className={styles.codeBlock}>
              <code>{dockerRun}</code>
            </pre>
          </div>
        </div>
      </section>

      <section className={styles.section}>
        <div className={styles.columns}>
          <div className={styles.card}>
            <h2 className={styles.cardTitle}>What to verify before Grafana points at it</h2>
            <ul className={styles.list}>
              <li>The proxy answers `/ready` with `ok`.</li>
              <li>`/loki/api/v1/labels` returns data instead of backend or translation errors.</li>
              <li>Proxy logs show clean downstream and upstream routing for simple calls.</li>
              <li>You know whether the backend schema needs underscore label translation.</li>
            </ul>
            <pre className={styles.codeBlock}>
              <code>{verifyRun}</code>
            </pre>
          </div>
          <div className={styles.card}>
            <h2 className={styles.cardTitle}>When to stop treating it as a demo</h2>
            <ul className={styles.list}>
              <li>Move to Helm when the path needs repeatable deployment and resource controls.</li>
              <li>Add persistent disk cache when repeated read traffic is larger than RAM hotset.</li>
              <li>Add peer cache when multiple replicas will see overlapping query traffic.</li>
              <li>Enable OTLP push when your metrics path is collector-first instead of scrape-first.</li>
            </ul>
          </div>
        </div>
      </section>

      <section className={styles.section}>
        <div className={styles.callout}>
          <Heading as="h2" className={styles.sectionTitle}>
            Next practical steps
          </Heading>
          <div className={styles.inlineLinks}>
            <Link to="/grafana-loki-datasource-victorialogs/">Set up the Grafana datasource</Link>
            <Link to="/install-loki-vl-proxy-with-helm/">Install with Helm</Link>
            <Link to="/monitor-loki-vl-proxy/">Monitor the proxy</Link>
            <Link to="/docs/configuration/">Configuration reference</Link>
          </div>
        </div>
      </section>
    </MarketingLayout>
  );
}
