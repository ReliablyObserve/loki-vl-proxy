import Head from '@docusaurus/Head';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import type {FAQItem} from '@site/src/data/faqs';

function buildCanonical(siteUrl: string, baseUrl: string, path: string): string {
  const base = new URL(baseUrl, siteUrl);
  const normalizedPath = path === '/' ? '' : path.replace(/^\//, '');
  return new URL(normalizedPath, base).toString();
}

type SeoHeadProps = {
  title: string;
  description: string;
  path: string;
  keywords?: string[];
  faqs?: FAQItem[];
  structuredData?: Array<Record<string, unknown>>;
};

export default function SeoHead({
  title,
  description,
  path,
  keywords,
  faqs,
  structuredData,
}: SeoHeadProps) {
  const {siteConfig} = useDocusaurusContext();
  const canonical = buildCanonical(siteConfig.url, siteConfig.baseUrl, path);
  const schemas = [...(structuredData ?? [])];

  if (faqs && faqs.length > 0) {
    schemas.push({
      '@context': 'https://schema.org',
      '@type': 'FAQPage',
      mainEntity: faqs.map((faq) => ({
        '@type': 'Question',
        name: faq.question,
        acceptedAnswer: {
          '@type': 'Answer',
          text: faq.answer,
        },
      })),
    });
  }

  return (
    <Head>
      <title>{title}</title>
      <meta name="description" content={description} />
      <meta property="og:type" content="website" />
      <meta property="og:title" content={title} />
      <meta property="og:description" content={description} />
      <meta property="og:url" content={canonical} />
      <meta name="twitter:card" content="summary_large_image" />
      <meta name="twitter:title" content={title} />
      <meta name="twitter:description" content={description} />
      <link rel="canonical" href={canonical} />
      {keywords && keywords.length > 0 ? (
        <meta name="keywords" content={keywords.join(', ')} />
      ) : null}
      {schemas.map((schema, index) => (
        <script
          key={`schema-${index}`}
          type="application/ld+json"
          dangerouslySetInnerHTML={{__html: JSON.stringify(schema)}}
        />
      ))}
    </Head>
  );
}
