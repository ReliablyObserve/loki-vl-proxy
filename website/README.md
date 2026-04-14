# Website

This Docusaurus site publishes the public docs and SEO landing pages for Loki-VL-proxy.

## Local development

```bash
cd website
npm install
npm start
```

## Production build

```bash
cd website
npm install
npm run build
```

The deep technical docs stay in the repository root `docs/` directory. The Docusaurus docs plugin reads that directory directly, while `website/src/pages/` holds the search-oriented landing pages.
