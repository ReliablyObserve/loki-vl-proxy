import type {ReactNode} from 'react';
import Link from '@docusaurus/Link';
import Layout from '@theme/Layout';
import Heading from '@theme/Heading';
import SeoHead from '@site/src/components/SeoHead';
import type {FAQItem} from '@site/src/data/faqs';
import styles from './marketing.module.css';

export type Cta = {
  label: string;
  to: string;
};

export type Highlight = {
  value: string;
  label: string;
  detail: string;
};

type MarketingLayoutProps = {
  path: string;
  title: string;
  description: string;
  eyebrow: string;
  headline: string;
  lede: string;
  primaryCta: Cta;
  secondaryCta?: Cta;
  highlights: Highlight[];
  faqs?: FAQItem[];
  structuredData?: Array<Record<string, unknown>>;
  children: ReactNode;
};

export default function MarketingLayout({
  path,
  title,
  description,
  eyebrow,
  headline,
  lede,
  primaryCta,
  secondaryCta,
  highlights,
  faqs,
  structuredData,
  children,
}: MarketingLayoutProps) {
  return (
    <Layout title={title} description={description}>
      <SeoHead
        title={title}
        description={description}
        path={path}
        faqs={faqs}
        structuredData={structuredData}
      />
      <main className={styles.page}>
        <section className={styles.hero}>
          <div className={`container ${styles.heroContainer}`}>
            <div className={styles.heroGrid}>
              <div className={styles.heroCopy}>
                <p className={styles.eyebrow}>{eyebrow}</p>
                <Heading as="h1" className={styles.heroTitle}>
                  {headline}
                </Heading>
                <p className={styles.heroLead}>{lede}</p>
                <div className={styles.actions}>
                  <Link className="button button--primary button--lg" to={primaryCta.to}>
                    {primaryCta.label}
                  </Link>
                  {secondaryCta ? (
                    <Link className="button button--secondary button--lg" to={secondaryCta.to}>
                      {secondaryCta.label}
                    </Link>
                  ) : null}
                </div>
              </div>
              <div className={styles.signalPanel}>
                {highlights.map((highlight) => (
                  <div key={highlight.value + highlight.label} className={styles.signalCard}>
                    <div className={styles.signalValue}>{highlight.value}</div>
                    <div className={styles.signalLabel}>{highlight.label}</div>
                    <div className={styles.signalDetail}>{highlight.detail}</div>
                  </div>
                ))}
              </div>
            </div>
          </div>
        </section>
        <div className={`container ${styles.content}`}>{children}</div>
      </main>
    </Layout>
  );
}
