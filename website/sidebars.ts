import type {SidebarsConfig} from '@docusaurus/plugin-content-docs';

const sidebars: SidebarsConfig = {
  docsSidebar: [
    // ── Start Here ───────────────────────────────────────────────────
    {
      type: 'category',
      label: '🚀 Start Here',
      collapsible: false,
      items: ['getting-started'],
    },

    // ── Architecture ─────────────────────────────────────────────────
    {
      type: 'category',
      label: 'Architecture',
      collapsed: false,
      items: [
        'architecture',
        'translation-modes',
        'translation-reference',
      ],
    },

    // ── Configuration ─────────────────────────────────────────────────
    {
      type: 'category',
      label: 'Configuration',
      collapsed: true,
      items: ['configuration'],
    },

    // ── Cost & Comparison ─────────────────────────────────────────────
    {
      type: 'category',
      label: 'Cost & Comparison',
      collapsed: false,
      items: [
        'cost-and-comparison',
        'cost-model',
        'benchmarks',
        'performance',
        'scaling',
      ],
    },

    // ── Compatibility ────────────────────────────────────────────────
    {
      type: 'category',
      label: 'Compatibility',
      collapsed: false,
      items: [
        'compatibility-matrix',
        'compatibility-loki',
        'compatibility-victorialogs',
        'compatibility-grafana-datasource',
        'compatibility-drilldown',
        'otel-compatibility',
      ],
    },

    // ── Operations ────────────────────────────────────────────────────
    {
      type: 'category',
      label: 'Operations',
      collapsed: false,
      items: [
        'operations',
        'security',
        'patterns',
      ],
    },

    // ── Caching ───────────────────────────────────────────────────────
    {
      type: 'category',
      label: 'Caching',
      collapsed: true,
      items: [
        'fleet-cache',
        'peer-cache-design',
      ],
    },

    // ── Observability ─────────────────────────────────────────────────
    {
      type: 'category',
      label: 'Observability',
      collapsed: true,
      items: ['observability'],
    },

    // ── Testing ───────────────────────────────────────────────────────
    {
      type: 'category',
      label: 'Testing',
      collapsed: true,
      items: [
        'testing',
        'testing-e2e-guide',
        'performance-testing-guide',
      ],
    },

    // ── Reference ────────────────────────────────────────────────────
    {
      type: 'category',
      label: 'Reference',
      collapsed: true,
      items: [
        'api-reference',
        'KNOWN_ISSUES',
        'rules-alerts-migration',
        'browser-automation-alternatives',
        'release-info',
        'roadmap',
      ],
    },

    // ── Runbooks ──────────────────────────────────────────────────────
    {
      type: 'category',
      label: 'Runbooks',
      collapsed: true,
      items: [
        'runbooks/deployment-best-practices',
        'runbooks/alerts',
        {
          type: 'category',
          label: 'Alert Runbooks',
          collapsed: true,
          items: [
            'runbooks/loki-vl-proxy-down',
            'runbooks/loki-vl-proxy-backend-unreachable',
            'runbooks/loki-vl-proxy-backend-high-latency',
            'runbooks/loki-vl-proxy-high-latency',
            'runbooks/loki-vl-proxy-high-error-rate',
            'runbooks/loki-vl-proxy-circuit-breaker-open',
            'runbooks/loki-vl-proxy-client-bad-request-burst',
            'runbooks/loki-vl-proxy-rate-limiting',
            'runbooks/loki-vl-proxy-system-resources',
            'runbooks/loki-vl-proxy-grafana-tuple-contract',
            'runbooks/loki-vl-proxy-tenant-high-error-rate',
          ],
        },
      ],
    },
  ],
};

export default sidebars;
