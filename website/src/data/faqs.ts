export type FAQItem = {
  question: string;
  answer: string;
};

export const coreFaqs: FAQItem[] = [
  {
    question: 'Can Grafana use VictoriaLogs with the native Loki datasource?',
    answer:
      "Yes. Loki-VL-proxy exposes a Loki-compatible read API in front of VictoriaLogs so Grafana can keep using its built-in Loki datasource while the backend is VictoriaLogs.",
  },
  {
    question: 'Do I need a custom plugin?',
    answer:
      'No. The project is specifically built to avoid a custom Grafana datasource plugin. Grafana talks to the proxy as if it were talking to Loki.',
  },
  {
    question: 'Does it support Explore and Logs Drilldown?',
    answer:
      'Yes. The project keeps explicit compatibility tracks for Grafana Explore, the Loki datasource, Logs Drilldown, and VictoriaLogs-backed behavior, including the Loki-compatible patterns endpoint used by Drilldown.',
  },
  {
    question: 'Is it read-only?',
    answer:
      'Yes. Loki-VL-proxy is intentionally a read/query proxy. Query, metadata, patterns, and rules or alerts read views are in scope. Push remains blocked.',
  },
];
