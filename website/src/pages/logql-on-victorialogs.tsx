import type {ReactNode} from 'react';
import Link from '@docusaurus/Link';
import Heading from '@theme/Heading';
import MarketingLayout from '@site/src/components/MarketingLayout';
import styles from '@site/src/components/marketing.module.css';
import {coreFaqs} from '@site/src/data/faqs';

export default function LogqlOnVictoriaLogs(): ReactNode {
  return (
    <MarketingLayout
      path="/logql-on-victorialogs/"
      title="LogQL on VictoriaLogs"
      description="Run LogQL-style read workflows against VictoriaLogs through Loki-VL-proxy, with explicit translation modes for dotted fields, labels, and structured metadata surfaces."
      eyebrow="Query semantics"
      headline="Run LogQL-style read workflows on VictoriaLogs"
      lede="Loki-VL-proxy does not claim that VictoriaLogs is natively Loki. It provides a controlled read-side translation layer so LogQL-oriented clients can keep working while operators stay explicit about field naming, metadata surfaces, and compatibility limits."
      primaryCta={{label: 'Read translation modes', to: '/docs/translation-modes/'}}
      secondaryCta={{label: 'Open the API reference', to: '/docs/api-reference/'}}
      highlights={[
        {
          value: 'Label style control',
          label: 'Choose underscore or passthrough label behavior on label-facing APIs',
          detail: 'Useful for Loki-safe UX.',
        },
        {
          value: 'Metadata field mode',
          label: 'Native, translated, or hybrid field surfaces are explicit',
          detail: 'Important for dotted OTel fields.',
        },
        {
          value: 'Custom mapping',
          label: 'Field mappings let you pin non-standard schemas to stable aliases',
          detail: 'Good for dashboards and alerts.',
        },
        {
          value: 'Known limits stay documented',
          label: 'The project documents the surfaces it translates and the ones it leaves out',
          detail: 'No hidden magic.',
        },
      ]}
      faqs={coreFaqs}
    >
      <section className={styles.section}>
        <div className={styles.cardGrid}>
          <div className={styles.card}>
            <h2 className={styles.cardTitle}>How to think about compatibility</h2>
            <p>
              Treat the proxy as a read compatibility layer, not as proof that the
              storage engines are identical. The right operational question is
              whether the specific query, label, field, and metadata surfaces your
              clients use are covered and observable.
            </p>
          </div>
          <div className={styles.card}>
            <h2 className={styles.cardTitle}>Where translation shows up</h2>
            <p>
              Translation affects stream labels, label APIs, detected fields,
              structured metadata, and alias resolution for dotted and underscore
              field names. Those surfaces can be tuned independently.
            </p>
          </div>
        </div>
      </section>

      <section className={styles.section}>
        <div className={styles.sectionHeader}>
          <Heading as="h2" className={styles.sectionTitle}>
            The translation profiles that matter most
          </Heading>
        </div>
        <div className={styles.cardGrid}>
          <div className={styles.card}>
            <h3 className={styles.cardTitle}>Loki-first</h3>
            <p>
              Use underscore label style and translated metadata fields when users
              mainly live in Grafana builder paths and want the most Loki-like
              experience.
            </p>
          </div>
          <div className={styles.card}>
            <h3 className={styles.cardTitle}>Balanced compatibility</h3>
            <p>
              Use underscore labels with hybrid metadata fields when you need both
              Grafana-friendly label ergonomics and dotted OTel correlation.
            </p>
          </div>
          <div className={styles.card}>
            <h3 className={styles.cardTitle}>OTel-native field workflows</h3>
            <p>
              Keep label surfaces Loki-safe while exposing dotted field names on
              field-oriented APIs if your operators reason in OTel-native terms.
            </p>
          </div>
          <div className={styles.card}>
            <h3 className={styles.cardTitle}>Custom schema mapping</h3>
            <p>
              Use explicit field mappings when your backend schema does not follow
              common OTel conventions and you need stable aliases for dashboards,
              alerts, or client tooling.
            </p>
          </div>
        </div>
      </section>

      <section className={styles.section}>
        <div className={styles.sectionHeader}>
          <Heading as="h2" className={styles.sectionTitle}>
            Current differences to plan for
          </Heading>
          <p className={styles.sectionLead}>
            The current codebase supports the read path broadly, but a few
            boundaries are still important if you want predictable Grafana and
            Loki-client behavior.
          </p>
        </div>
        <div className={styles.cardGrid}>
          <div className={styles.card}>
            <h3 className={styles.cardTitle}>Read compatibility, not generic Loki identity</h3>
            <p>
              The proxy keeps Loki-facing reads usable on VictoriaLogs. It does
              not turn VictoriaLogs into a native Loki backend, and it does not
              open the normal Loki write path.
            </p>
          </div>
          <div className={styles.card}>
            <h3 className={styles.cardTitle}>Dotted fields still need explicit UX choices</h3>
            <p>
              Underscore labels and dotted field APIs can coexist, but Grafana
              builder flows are still safer when operators use the underscore
              aliases for click-to-filter paths.
            </p>
          </div>
          <div className={styles.card}>
            <h3 className={styles.cardTitle}>Some compatibility work stays in the proxy</h3>
            <p>
              Parser compatibility, formatting helpers, alias resolution, and
              parts of binary or subquery behavior still run in the proxy. They
              are supported, but they are not zero-cost native VictoriaLogs
              operations.
            </p>
          </div>
          <div className={styles.card}>
            <h3 className={styles.cardTitle}>Patterns and parsed fields have warm-state caveats</h3>
            <p>
              `/patterns` is optional and capped per request, and parsed-only
              field discovery can still rely on bounded sampling when there is no
              safe indexed metadata path.
            </p>
          </div>
        </div>
      </section>

      <section className={styles.section}>
        <div className={styles.callout}>
          <Heading as="h2" className={styles.sectionTitle}>
            Deep references
          </Heading>
          <div className={styles.inlineLinks}>
            <Link to="/docs/translation-modes/">Translation Modes</Link>
            <Link to="/docs/translation-reference/">Translation Reference</Link>
            <Link to="/docs/compatibility-loki/">Loki Compatibility</Link>
            <Link to="/docs/api-reference/">API Reference</Link>
            <Link to="/docs/KNOWN_ISSUES/">Known Differences</Link>
          </div>
        </div>
      </section>
    </MarketingLayout>
  );
}
