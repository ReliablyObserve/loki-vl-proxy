import {themes as prismThemes} from 'prism-react-renderer';
import type {Config} from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';

const siteTitle = 'Loki Proxy for VictoriaLogs';
const siteDescription =
  "Use Grafana's native Loki datasource, Explore, and Logs Drilldown with VictoriaLogs through a Loki-compatible read proxy. No custom plugin required.";

const config: Config = {
  title: siteTitle,
  tagline: siteDescription,
  future: {
    v4: true,
  },
  url: 'https://reliablyobserve.github.io',
  baseUrl: '/loki-vl-proxy/',
  trailingSlash: true,
  organizationName: 'ReliablyObserve',
  projectName: 'Loki-VL-proxy',
  onBrokenLinks: 'throw',
  markdown: {
    mermaid: true,
    hooks: {
      onBrokenMarkdownLinks: 'warn',
    },
  },
  themes: ['@docusaurus/theme-mermaid'],
  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },
  presets: [
    [
      'classic',
      {
        docs: {
          path: '../docs',
          routeBasePath: 'docs',
          sidebarPath: './sidebars.ts',
          editUrl: 'https://github.com/ReliablyObserve/Loki-VL-proxy/tree/main/',
          showLastUpdateTime: true,
        },
        blog: false,
        theme: {
          customCss: './src/css/custom.css',
        },
        sitemap: {
          changefreq: 'weekly',
          priority: 0.7,
          ignorePatterns: ['/tags/**'],
          filename: 'sitemap.xml',
        },
      } satisfies Preset.Options,
    ],
  ],
  themeConfig: {
    metadata: [
      {name: 'description', content: siteDescription},
      {
        name: 'keywords',
        content:
          'VictoriaLogs Loki proxy, Grafana Loki datasource VictoriaLogs, Grafana Explore VictoriaLogs, Logs Drilldown VictoriaLogs, LogQL VictoriaLogs',
      },
    ],
    colorMode: {
      defaultMode: 'light',
      respectPrefersColorScheme: true,
    },
    navbar: {
      title: 'Loki-VL-proxy',
      logo: {
        alt: 'Loki-VL-proxy logo',
        src: 'img/logo.svg',
        srcDark: 'img/logo.svg',
      },
      items: [
        {to: '/', label: 'Overview', position: 'left'},
        {
          type: 'docSidebar',
          sidebarId: 'docsSidebar',
          position: 'left',
          label: 'Docs',
        },
        {
          to: '/loki-proxy-for-victorialogs/',
          label: 'Use Cases',
          position: 'left',
        },
        {
          to: '/migrate-grafana-from-loki-to-victorialogs/',
          label: 'Migration',
          position: 'left',
        },
        {
          href: 'https://github.com/ReliablyObserve/Loki-VL-proxy',
          label: 'GitHub',
          position: 'right',
        },
      ],
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: 'Use Cases',
          items: [
            {
              label: 'Loki Proxy for VictoriaLogs',
              to: '/loki-proxy-for-victorialogs/',
            },
            {
              label: 'Grafana Loki Datasource',
              to: '/grafana-loki-datasource-victorialogs/',
            },
            {
              label: 'Grafana Explore',
              to: '/grafana-explore-victorialogs/',
            },
            {
              label: 'VictoriaLogs with Grafana Loki Datasource',
              to: '/victorialogs-with-grafana-loki-datasource/',
            },
            {
              label: 'Logs Drilldown',
              to: '/logs-drilldown-victorialogs/',
            },
            {
              label: 'LogQL on VictoriaLogs',
              to: '/logql-on-victorialogs/',
            },
          ],
        },
        {
          title: 'Compare And Migrate',
          items: [
            {
              label: 'Loki vs VictoriaLogs',
              to: '/loki-vs-victorialogs-grafana-query-workflows/',
            },
            {
              label: 'Migrate Grafana to VictoriaLogs',
              to: '/migrate-grafana-from-loki-to-victorialogs/',
            },
          ],
        },
        {
          title: 'Docs',
          items: [
            {label: 'Getting Started', to: '/docs/getting-started/'},
            {label: 'Compatibility Matrix', to: '/docs/compatibility-matrix/'},
            {label: 'Observability', to: '/docs/observability/'},
            {label: 'Operations', to: '/docs/operations/'},
          ],
        },
        {
          title: 'Project',
          items: [
            {
              label: 'Repository',
              href: 'https://github.com/ReliablyObserve/Loki-VL-proxy',
            },
            {
              label: 'Releases',
              href: 'https://github.com/ReliablyObserve/Loki-VL-proxy/releases',
            },
            {
              label: 'Helm Chart',
              href: 'https://github.com/ReliablyObserve/Loki-VL-proxy/tree/main/charts/loki-vl-proxy',
            },
          ],
        },
      ],
      copyright: `Copyright © ${new Date().getFullYear()} ReliablyObserve. Built with Docusaurus.`,
    },
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.vsDark,
    },
  } satisfies Preset.ThemeConfig,
};

export default config;
