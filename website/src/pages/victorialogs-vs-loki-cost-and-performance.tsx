import type {ReactNode} from 'react';
import Link from '@docusaurus/Link';
import Heading from '@theme/Heading';
import MarketingLayout from '@site/src/components/MarketingLayout';
import styles from '@site/src/components/marketing.module.css';
import {coreFaqs} from '@site/src/data/faqs';

const comparisonRows = [
  {
    dimension: 'Indexing strategy',
    loki:
      'Loki docs: labels index streams, but the content of each log line is not indexed.',
    victorialogs:
      'VictoriaLogs docs: all fields are indexed and the query model supports full-text search across fields.',
    proxy:
      'The proxy keeps the Loki read contract in front of that backend so Grafana can stay on the native Loki datasource.',
  },
  {
    dimension: 'High-cardinality behavior',
    loki:
      'Loki docs recommend low-cardinality labels and warn that high cardinality hurts performance and cost-effectiveness.',
    victorialogs:
      'VictoriaLogs docs say high-cardinality values such as `trace_id`, `user_id`, and `ip` work fine as fields as long as they are not used as stream fields.',
    proxy:
      'The proxy lets Grafana keep Loki-safe label surfaces while VictoriaLogs keeps the richer field model underneath.',
  },
  {
    dimension: 'Search-heavy workloads',
    loki:
      'Broad or text-heavy searches can devolve into stream selection plus line filtering because line content is not indexed.',
    victorialogs:
      'VictoriaLogs publishes fast full-text search as a core capability, and third-party benchmarks report materially faster broad-search latency on large datasets.',
    proxy:
      'Tier0, L1/L2/L3, and window cache can further suppress repeated read work after the first expensive search path.',
  },
  {
    dimension: 'Operational shape',
    loki:
      'Loki can run single-binary, but its scalable architecture is microservices-based with multiple components.',
    victorialogs:
      'VictoriaLogs docs position the backend as a simple single executable on the easy path, but they also document cluster mode with `vlinsert`, `vlselect`, `vlstorage`, replication, multi-level cluster setup, and HA patterns across independent availability zones.',
    proxy:
      'The proxy adds one small read-side compatibility layer with route-aware metrics and structured logs instead of hiding translation work inside clients, and can sit in front of either single-node or clustered VictoriaLogs.',
  },
  {
    dimension: 'Published resource claims',
    loki:
      'Grafana docs do not market a universal fixed savings ratio; they emphasize label strategy, storage, and deployment architecture.',
    victorialogs:
      'VictoriaLogs docs publish up to `30x` less RAM and up to `15x` less disk than Loki or Elasticsearch, while TrueFoundry reports `≈40%` less storage and much lower CPU and RAM on its workload.',
    proxy:
      'The proxy adds its own small runtime cost, but published project benchmarks show it remains CPU-light and can sharply reduce repeated backend work through caching.',
  },
  {
    dimension: 'Published large-workload sizing',
    loki:
      'Grafana\'s own sizing guide reaches `431 vCPU / 857 Gi` at `3-30 TB/day` and `1221 vCPU / 2235 Gi` around `30 TB/day` before query spikes.',
    victorialogs:
      'VictoriaLogs docs do not publish an equivalent distributed tier matrix on the same shape; the safer claim is lower-resource posture plus stronger compression and search behavior on published comparisons.',
    proxy:
      'The proxy does not change backend ingest economics by itself, but it keeps the read side small and can cut repeated backend work through tiered caches and route-aware control.',
  },
  {
    dimension: 'Cross-AZ traffic posture',
    loki:
      'Loki docs say distributors forward writes to a replication factor that is generally `3`, queriers query all ingesters for in-memory data, and the zone-aware replication design explicitly lists minimizing cross-zone traffic costs as a non-goal.',
    victorialogs:
      'VictoriaLogs cluster docs support independent clusters in separate availability zones plus advanced multi-level cluster setup, which lets operators keep most normal reads local and reserve cross-AZ fanout for HA or global queries.',
    proxy:
      'The proxy can stay AZ-local on the read path and adds `zstd`/`gzip` compression on the hops it controls, but it does not invent backend replication savings that the VictoriaLogs docs do not quantify.',
  },
];

const lokiSizingRows = [
  {
    tier: '<3 TB/day',
    request: '38 vCPU / 59 Gi',
    nodes: '3 x c7i.4xlarge',
    cost: '$1,489.20 / month',
  },
  {
    tier: '3-30 TB/day',
    request: '431 vCPU / 857 Gi',
    nodes: '27 x c7i.4xlarge',
    cost: '$13,402.80 / month',
  },
  {
    tier: '~30 TB/day',
    request: '1221 vCPU / 2235 Gi',
    nodes: '77 x c7i.4xlarge',
    cost: '$38,222.80 / month',
  },
];

const scenarioRows = [
  {
    scenario: 'Small',
    users: '100',
    ingest: '100k lines/s',
    daily: '2.16 TB/day',
    loki: '$1,681.20',
    combo: '$369.16',
    delta: '$1,312.04',
    savings: '78.0%',
  },
  {
    scenario: 'Medium',
    users: '1,000',
    ingest: '500k lines/s',
    daily: '10.8 TB/day',
    loki: '$14,362.80',
    combo: '$1,101.20',
    delta: '$13,261.60',
    savings: '92.3%',
  },
  {
    scenario: 'Large',
    users: '10,000',
    ingest: '1M lines/s',
    daily: '21.6 TB/day',
    loki: '$15,322.80',
    combo: '$2,388.55',
    delta: '$12,934.25',
    savings: '84.4%',
  },
];

const observedRows = [
  {
    scale: '1x',
    ingest: '0.333 TB/day',
    vl: '40.5 GiB',
    loki: '64.3 GiB',
    vlCost: '$3.24',
    lokiCost: '$5.14',
    tier: '<3 TB/day',
    compute: '$1,489.20 / month',
  },
  {
    scale: '10x',
    ingest: '3.33 TB/day',
    vl: '405 GiB',
    loki: '642.9 GiB',
    vlCost: '$32.40',
    lokiCost: '$51.43',
    tier: '3-30 TB/day',
    compute: '$13,402.80 / month',
  },
  {
    scale: '30x',
    ingest: '9.99 TB/day',
    vl: '1,215 GiB',
    loki: '1,928.6 GiB',
    vlCost: '$97.20',
    lokiCost: '$154.29',
    tier: '3-30 TB/day',
    compute: '$13,402.80 / month',
  },
  {
    scale: '100x',
    ingest: '33.29 TB/day',
    vl: '4,050 GiB',
    loki: '6,428.6 GiB',
    vlCost: '$324.00',
    lokiCost: '$514.29',
    tier: '~30 TB/day',
    compute: '$38,222.80 / month',
  },
];

const networkRows = [
  {
    scale: '1x',
    ingest: '310 GiB/day',
    payload: '620 GiB/day',
    cost: '$372.00 / month',
  },
  {
    scale: '10x',
    ingest: '3,100 GiB/day',
    payload: '6,200 GiB/day',
    cost: '$3,720.00 / month',
  },
  {
    scale: '30x',
    ingest: '9,300 GiB/day',
    payload: '18,600 GiB/day',
    cost: '$11,160.00 / month',
  },
  {
    scale: '100x',
    ingest: '31,000 GiB/day',
    payload: '62,000 GiB/day',
    cost: '$37,200.00 / month',
  },
];

const computeRows = [
  {
    scale: '1x',
    ingest: '0.333 TB/day',
    envelope: '1.2 cores / 5.85 GiB',
    floor: '1 x c7i.xlarge',
    vlCost: '$124.10 / month',
    lokiCost: '$1,489.20 / month',
    cpu: '31.7x',
    memory: '10.1x',
  },
  {
    scale: '10x',
    ingest: '3.33 TB/day',
    envelope: '12 cores / 58.5 GiB',
    floor: '4 x c7i.2xlarge',
    vlCost: '$992.80 / month',
    lokiCost: '$13,402.80 / month',
    cpu: '35.9x',
    memory: '14.6x',
  },
  {
    scale: '30x',
    ingest: '9.99 TB/day',
    envelope: '36 cores / 175.5 GiB',
    floor: '6 x c7i.4xlarge',
    vlCost: '$2,978.40 / month',
    lokiCost: '$13,402.80 / month',
    cpu: '12.0x',
    memory: '4.9x',
  },
  {
    scale: '100x',
    ingest: '33.29 TB/day',
    envelope: '120 cores / 585 GiB',
    floor: '19 x c7i.4xlarge',
    vlCost: '$9,431.60 / month',
    lokiCost: '$38,222.80 / month',
    cpu: '10.2x',
    memory: '3.8x',
  },
];

const highLoadRows = [
  {
    scenario: 'Real-life tested steady-state high load',
    ingest: '0.56 TB/day',
    vl: '68.3 GiB',
    loki: '108.4 GiB',
    envelope: '2.0 cores / 9.9 GiB',
    floor: '1 x c7i.2xlarge',
    tier: '<3 TB/day',
    compute: '$1,489.20 / month',
    payload: '1,046 GiB/day',
    network: '$627.60 / month',
  },
];

const topologyRows = [
  {
    topology: '3 x vlstorage per AZ',
    components: '3 x vlinsert, 3 x vlselect, 9 x vlstorage',
    treatment: 'keep the combined compute envelope used in the main tables',
  },
  {
    topology: '4 x vlstorage per AZ',
    components: '3 x vlinsert, 3 x vlselect, 12 x vlstorage',
    treatment: 'keep the combined compute envelope used in the main tables',
  },
];

export default function VictoriaLogsVsLokiCostAndPerformance(): ReactNode {
  return (
    <MarketingLayout
      path="/victorialogs-vs-loki-cost-and-performance/"
      title="VictoriaLogs vs Loki Cost and Performance"
      description="Source-backed comparison of Loki and VictoriaLogs for cost and search performance, plus what Loki-VL-proxy adds on the Grafana read path with cache tiers, route-aware telemetry, and migration control."
      eyebrow="Cost and performance"
      headline="Compare VictoriaLogs and Loki with a source-backed cost lens"
      lede="The cost story is not that a proxy magically makes every logging stack cheap. The defensible argument is narrower: Loki\'s own docs describe a label-indexed system sensitive to high-cardinality labels, VictoriaLogs publishes an all-field index and lower-resource claims, and Loki-VL-proxy adds a 4-tier cache, circuit breaker, and request coalescer so repeated Grafana traffic can cost significantly less. Real-tested: 800M log entries, 310 GiB ingested, 40.5 GiB on disk at 54.9× compression."
      primaryCta={{label: 'Read the cache guide', to: '/cache-tiers-and-fleet-cache-for-victorialogs/'}}
      secondaryCta={{label: 'Read the monitoring guide', to: '/monitor-loki-vl-proxy/'}}
      highlights={[
        {
          value: '78–92% cost reduction',
          label: 'VL + proxy vs Loki EC2 compute floor at the same ingest tier',
          detail: '~33 vCPU / 70 GiB vs 431 vCPU / 857 GiB at 3–30 TB/day.',
        },
        {
          value: '54.9× compression',
          label: 'Real-tested: 800M entries, 310 GiB ingested → 40.5 GiB on disk (7.14-day retention)',
          detail: 'Observed on production workload, not a synthetic benchmark.',
        },
        {
          value: '1,006–1,717×',
          label: 'Proxy throughput vs Loki on heavy aggregation workloads (warm and cold)',
          detail: '30-second bench, Apple M5 Pro. Prefilter cuts ~81.6% of empty backend calls.',
        },
        {
          value: '$13,403/month',
          label: 'Loki EC2 compute floor at 3–30 TB/day (Grafana published sizing, c7i.4xlarge on-demand)',
          detail: 'VL + proxy at same workload: ~$1,100–$2,400/month depending on scale.',
        },
        {
          value: 'Workload dependent',
          label: 'The right answer depends on retention, search mix, and dashboard repetition',
          detail: 'This page separates vendor claims from project benchmarks and third-party reports.',
        },
      ]}
      faqs={coreFaqs}
    >
      <section className={styles.section}>
        <div className={styles.tableWrap}>
          <table className={styles.comparisonTable}>
            <thead>
              <tr>
                <th>Dimension</th>
                <th>What official Loki docs say</th>
                <th>What VictoriaLogs docs or published reports say</th>
                <th>What Loki-VL-proxy adds</th>
              </tr>
            </thead>
            <tbody>
              {comparisonRows.map((row) => (
                <tr key={row.dimension}>
                  <td>{row.dimension}</td>
                  <td>{row.loki}</td>
                  <td>{row.victorialogs}</td>
                  <td>{row.proxy}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      <section className={styles.section}>
        <div className={styles.cardGrid}>
          <div className={styles.card}>
            <h2 className={styles.cardTitle}>Why the Loki cost floor matters</h2>
            <ul className={styles.list}>
              <li>Grafana already publishes large distributed Loki sizing floors by ingest throughput, so the high-end compute side is not a vague anti-Loki argument.</li>
              <li>At `3-30 TB/day`, the published Loki floor is `431 vCPU / 857 Gi` before storage and before the `10x` querier-spike warning in the same docs.</li>
              <li>That is why this project\'s cost page converts Loki\'s own sizing guide into on-demand EC2 floors before comparing it with a smaller `VictoriaLogs + Loki-VL-proxy` reference pack.</li>
              <li>The proxy layer is intentionally modeled as a small read-path tax, not as the source of backend ingest savings.</li>
            </ul>
          </div>
          <div className={styles.card}>
            <h2 className={styles.cardTitle}>Where the savings argument is strongest</h2>
            <ul className={styles.list}>
              <li>Search-heavy workloads where users often scan broad time ranges for words, phrases, or IDs.</li>
              <li>Data models with many useful fields and high-cardinality values that should stay as fields rather than labels.</li>
              <li>Repeated Grafana dashboard, Explore, or Drilldown reads that can hit Tier0, local cache, disk cache, or peer cache.</li>
              <li>Migrations where you want VictoriaLogs economics without forcing Grafana and Loki API clients to change first.</li>
            </ul>
          </div>
          <div className={styles.card}>
            <h2 className={styles.cardTitle}>Where to be precise instead of hype-driven</h2>
            <ul className={styles.list}>
              <li>Do not present the proxy as a generic ingestion benchmark; standard Loki push stays blocked.</li>
              <li>Do not treat third-party workload numbers as universal truths for every cluster.</li>
              <li>Do not attribute VictoriaLogs backend savings to the proxy itself; the proxy adds read-path suppression and migration control.</li>
              <li>Do compare end-to-end client latency with upstream latency so you can see whether the proxy or the backend owns the cost.</li>
            </ul>
          </div>
          <div className={styles.card}>
            <h2 className={styles.cardTitle}>What the proxy measurably contributes</h2>
            <ul className={styles.list}>
              <li>Throughput vs Loki: 13.7× warm / 78× cold on small/metadata workloads; 1,006× warm / 1,717× cold on heavy aggregations; 101× cold at c=100 on compute workloads (5.3× faster than VL native).</li>
              <li>`query_range` warm hits in the published project benchmark land at `0.64–0.67 µs` versus `4.58 ms` on the cold delayed path.</li>
              <li>`detected_field_values` warm hits land at `0.71 µs` versus `2.76 ms` without Tier0.</li>
              <li>Peer-cache warm shadow-copy hits land at `52 ns` after the first owner fetch.</li>
              <li>Long-range prefiltering cut backend query calls by about `81.6%` on the published benchmark shape.</li>
            </ul>
          </div>
          <div className={styles.card}>
            <h2 className={styles.cardTitle}>How to verify the savings in another environment</h2>
            <ul className={styles.list}>
              <li>Track `loki_vl_proxy_requests_total` and `loki_vl_proxy_request_duration_seconds` by `endpoint` and `route`.</li>
              <li>Compare `loki_vl_proxy_backend_duration_seconds` with downstream latency to isolate proxy overhead from VictoriaLogs slowness.</li>
              <li>Watch `loki_vl_proxy_cache_hits_by_endpoint` and `_misses_by_endpoint` to see whether repeated reads are really being suppressed.</li>
              <li>Use structured logs with `proxy.overhead_ms` and `upstream.duration_ms` for exact per-request decomposition.</li>
            </ul>
          </div>
        </div>
      </section>

      <section className={styles.section}>
        <div className={styles.columns}>
          <div className={styles.card}>
            <h2 className={styles.cardTitle}>Loki published sizing converted to EC2</h2>
            <div className={styles.tableWrap}>
              <table className={styles.comparisonTable}>
                <thead>
                  <tr>
                    <th>Loki docs ingest tier</th>
                    <th>Published base request</th>
                    <th>Illustrative EC2 floor</th>
                    <th>Monthly compute floor</th>
                  </tr>
                </thead>
                <tbody>
                  {lokiSizingRows.map((row) => (
                    <tr key={row.tier}>
                      <td>{row.tier}</td>
                      <td>{row.request}</td>
                      <td>{row.nodes}</td>
                      <td>{row.cost}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <p>
              This uses simple `c7i.4xlarge` on-demand packing in `us-east-1` to
              turn Grafana&apos;s published CPU and memory requests into an
              operator-readable monthly floor. These AWS rows are pure
              calculations to put `$$` around the comparison, not observed cloud
              bills.
            </p>
          </div>
          <div className={styles.card}>
            <h2 className={styles.cardTitle}>Illustrative monthly cost scenarios</h2>
            <div className={styles.tableWrap}>
              <table className={styles.comparisonTable}>
                <thead>
                  <tr>
                    <th>Scenario</th>
                    <th>Active users</th>
                    <th>Ingest</th>
                    <th>Raw ingest/day</th>
                    <th>Loki total</th>
                    <th>Proxy + VL total</th>
                    <th>Monthly delta</th>
                    <th>Savings</th>
                  </tr>
                </thead>
                <tbody>
                  {scenarioRows.map((row) => (
                    <tr key={row.scenario}>
                      <td>{row.scenario}</td>
                      <td>{row.users}</td>
                      <td>{row.ingest}</td>
                      <td>{row.daily}</td>
                      <td>{row.loki}</td>
                      <td>{row.combo}</td>
                      <td>{row.delta}</td>
                      <td>{row.savings}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <p>
              These scenarios assume `7d` retention, `250 B` average raw line
              size, and a conservative VictoriaLogs storage factor of `10x`,
              even though some real deployments observe much higher
              data-block-only compression ratios.
            </p>
          </div>
        </div>
      </section>

      <section className={styles.section}>
        <div className={styles.columns}>
          <div className={styles.card}>
            <h2 className={styles.cardTitle}>Real-life tested VictoriaLogs baseline</h2>
            <ul className={styles.list}>
              <li>Real snapshot: `800 M` total entries, `112 M` ingested in `24h`, `310 GiB` ingested in `24h`, and `40.5 GiB` on disk.</li>
              <li>The observed compression ratio is `54.9`, which implies about `5.65 GiB/day` of compressed data blocks.</li>
              <li>`800 M / 112 M per day` implies about `7.14d` of retained data, which matches the `40.5 GiB` disk footprint closely.</li>
              <li>Average raw event size in this tested setup is about `2.9 KiB`, which is far larger than the earlier generic `250 B` planning model.</li>
              <li>This is a write-heavy calibration point because observed read traffic is `0 rps`, so it is useful for storage and ingest-tier math, not for proving read-path cache savings by itself.</li>
              <li>`available CPU = 43` and `available memory = 43 GiB` are cluster headroom signals, not service consumption, so they are not used as the VictoriaLogs compute baseline.</li>
            </ul>
          </div>
          <div className={styles.card}>
            <h2 className={styles.cardTitle}>Scaling the real-life tested baseline to Loki floors</h2>
            <div className={styles.tableWrap}>
              <table className={styles.comparisonTable}>
                <thead>
                  <tr>
                    <th>Scale</th>
                    <th>Raw ingest/day</th>
                    <th>VictoriaLogs retained `~7.1d`</th>
                    <th>Estimated Loki retained `~7.1d`</th>
                    <th>VictoriaLogs gp3</th>
                    <th>Loki gp3</th>
                    <th>Loki published tier</th>
                    <th>Loki compute floor</th>
                  </tr>
                </thead>
                <tbody>
                  {observedRows.map((row) => (
                    <tr key={row.scale}>
                      <td>{row.scale}</td>
                      <td>{row.ingest}</td>
                      <td>{row.vl}</td>
                      <td>{row.loki}</td>
                      <td>{row.vlCost}</td>
                      <td>{row.lokiCost}</td>
                      <td>{row.tier}</td>
                      <td>{row.compute}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <p>
              This uses the real-life tested `40.5 GiB` retained VictoriaLogs
              footprint as the base, then applies the same conservative
              `VL = 63% of Loki` retained-bytes assumption used in the docs
              cost model.
            </p>
          </div>
        </div>
      </section>

      <section className={styles.section}>
        <div className={styles.columns}>
          <div className={styles.card}>
            <h2 className={styles.cardTitle}>Real-life tested compute envelope vs Loki floor</h2>
            <div className={styles.tableWrap}>
              <table className={styles.comparisonTable}>
                <thead>
                  <tr>
                    <th>Scale</th>
                    <th>Raw ingest/day</th>
                    <th>Scaled VL envelope</th>
                    <th>Illustrative VL EC2 floor</th>
                    <th>VL compute</th>
                    <th>Loki compute</th>
                    <th>Loki / VL CPU</th>
                    <th>Loki / VL memory</th>
                  </tr>
                </thead>
                <tbody>
                  {computeRows.map((row) => (
                    <tr key={row.scale}>
                      <td>{row.scale}</td>
                      <td>{row.ingest}</td>
                      <td>{row.envelope}</td>
                      <td>{row.floor}</td>
                      <td>{row.vlCost}</td>
                      <td>{row.lokiCost}</td>
                      <td>{row.cpu}</td>
                      <td>{row.memory}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <p>
              This uses the measured VictoriaLogs process envelope from the same
              real-life tested setup: about `1.2` cores and `5.85 GiB` total
              across `vlstorage`, `vlinsert`, and `vlselect`.
            </p>
          </div>
          <div className={styles.card}>
            <h2 className={styles.cardTitle}>What this comparison means</h2>
            <ul className={styles.list}>
              <li>At the exact real-life tested baseline, the VictoriaLogs service envelope is small enough to fit on a single `c7i.xlarge`, while Loki\'s published throughput floor for the same ingest tier is already `3 x c7i.4xlarge`.</li>
              <li>Even when the measured VictoriaLogs envelope is scaled linearly, Loki\'s published floor stays materially larger on both CPU and memory.</li>
              <li>This does not prove that VictoriaLogs scales perfectly linearly; it shows that the real-life tested baseline is far below Loki\'s published distributed floor at the same ingest tier.</li>
              <li>That is the right way to compare here: a real-life tested VictoriaLogs envelope versus Loki\'s own published cluster-sizing floor, not marketing slogans versus marketing slogans.</li>
            </ul>
          </div>
        </div>
      </section>

      <section className={styles.section}>
        <div className={styles.card}>
          <h2 className={styles.cardTitle}>Real-life tested steady-state high-load envelope</h2>
          <div className={styles.tableWrap}>
            <table className={styles.comparisonTable}>
              <thead>
                <tr>
                  <th>Scenario</th>
                  <th>Raw ingest/day</th>
                  <th>VictoriaLogs retained `~7.1d`</th>
                  <th>Estimated Loki retained `~7.1d`</th>
                  <th>Scaled VL envelope</th>
                  <th>Illustrative VL EC2 floor</th>
                  <th>Loki published tier</th>
                  <th>Loki compute floor</th>
                  <th>Loki cross-AZ write payload/day</th>
                  <th>Effective inter-AZ monthly cost</th>
                </tr>
              </thead>
              <tbody>
                {highLoadRows.map((row) => (
                  <tr key={row.scenario}>
                    <td>{row.scenario}</td>
                    <td>{row.ingest}</td>
                    <td>{row.vl}</td>
                    <td>{row.loki}</td>
                    <td>{row.envelope}</td>
                    <td>{row.floor}</td>
                    <td>{row.tier}</td>
                    <td>{row.compute}</td>
                    <td>{row.payload}</td>
                    <td>{row.network}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <ul className={styles.list}>
            <li>This row uses the higher real-life tested envelope of about `2.5k` events per second and about `6.5 MB/s` raw ingest bandwidth.</li>
            <li>It is intentionally separate from the daily average snapshot so the page shows both the average storage baseline and the heavier sustained operating shape.</li>
            <li>Even at this higher steady-state envelope, the tested VictoriaLogs setup remains far below Loki\'s first published distributed compute floor.</li>
          </ul>
        </div>
      </section>

      <section className={styles.section}>
        <div className={styles.card}>
          <h2 className={styles.cardTitle}>3-AZ VictoriaLogs topology note</h2>
          <div className={styles.tableWrap}>
            <table className={styles.comparisonTable}>
              <thead>
                <tr>
                  <th>Topology</th>
                  <th>Minimum pod shape</th>
                  <th>Cost-model treatment</th>
                </tr>
              </thead>
              <tbody>
                {topologyRows.map((row) => (
                  <tr key={row.topology}>
                    <td>{row.topology}</td>
                    <td>{row.components}</td>
                    <td>{row.treatment}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <ul className={styles.list}>
            <li>This captures the normal production pod shape for a 3-AZ cluster with one `vlinsert` and one `vlselect` per AZ plus `3-4` `vlstorage` pods per AZ.</li>
            <li>The cost worksheet still uses combined compute in the main tables so the comparison stays about total service envelope rather than node-placement policy.</li>
            <li>The measured `vlstorage` footprint used elsewhere is for the tested `vlstorage` service envelope as a whole, not per storage pod.</li>
          </ul>
        </div>
      </section>

      <section className={styles.section}>
        <div className={styles.columns}>
          <div className={styles.card}>
            <h2 className={styles.cardTitle}>Inter-AZ write replication cost floor</h2>
            <div className={styles.tableWrap}>
              <table className={styles.comparisonTable}>
                <thead>
                  <tr>
                    <th>Scale</th>
                    <th>Raw ingest/day</th>
                    <th>Loki cross-AZ write payload/day</th>
                    <th>Illustrative monthly inter-AZ cost</th>
                  </tr>
                </thead>
                <tbody>
                  {networkRows.map((row) => (
                    <tr key={row.scale}>
                      <td>{row.scale}</td>
                      <td>{row.ingest}</td>
                      <td>{row.payload}</td>
                      <td>{row.cost}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <p>
              This models AWS inter-AZ transfer at an effective `$0.02/GB`
              crossed once because EC2 pricing charges `$0.01/GB` in and
              `$0.01/GB` out across Availability Zones in the same Region. For a
              3-AZ Loki cluster with replication factor `3`, the simple write
              floor is one local replica plus two remote replicas. These
              network-dollar rows are also worksheet calculations, not observed
              AWS billing lines.
            </p>
          </div>
          <div className={styles.card}>
            <h2 className={styles.cardTitle}>Why the VictoriaLogs shape can differ</h2>
            <ul className={styles.list}>
              <li>VictoriaLogs cluster docs support independent clusters in separate AZs and advanced multi-level cluster setup.</li>
              <li>That lets operators keep normal reads AZ-local and reserve cross-AZ fanout for explicit global or failover queries.</li>
              <li>The proxy adds `zstd` and `gzip` on the read path it controls, which reduces client and peer-cache transport bytes for repeated reads.</li>
              <li>I did not attach a hard VictoriaLogs inter-AZ dollar figure because the docs do not publish a stable per-hop replication compression ratio, and inventing one would make the model less honest.</li>
              <li>In the tested setup, `0 rps` reads means the measurable network bill is dominated by write replication, not by query fanout.</li>
            </ul>
          </div>
        </div>
      </section>

      <section className={styles.section}>
        <div className={styles.columns}>
          <div className={styles.card}>
            <h2 className={styles.cardTitle}>Published numbers worth citing carefully</h2>
            <ul className={styles.list}>
              <li>VictoriaLogs docs: up to `30x` less RAM and up to `15x` less disk than Loki or Elasticsearch.</li>
              <li>VictoriaLogs docs: all fields are indexed and high-cardinality values work unless promoted to stream fields.</li>
              <li>Some real deployments observe `50-60x` VictoriaLogs compression ratios on the data-block metric, but that excludes `indexdb` and should be treated as a lower-bound, not the full storage bill.</li>
              <li>TrueFoundry `500 GB / 7 day` benchmark: `≈40%` less storage and materially lower CPU and RAM than Loki on its workload.</li>
              <li>TrueFoundry broad-search results: VictoriaLogs was faster on its needle-in-haystack and negative-match tests.</li>
              <li>Grafana\'s own Loki sizing guide publishes a `3-30 TB/day` base cluster at `431 vCPU / 857 Gi` and a `~30 TB/day` cluster at `1221 vCPU / 2235 Gi` before query spikes, which makes the compute side of the cost story concrete.</li>
            </ul>
          </div>
          <div className={styles.card}>
            <h2 className={styles.cardTitle}>Published Loki behaviors worth keeping in mind</h2>
            <ul className={styles.list}>
              <li>Loki docs: labels are for low-cardinality values and line content is not indexed.</li>
              <li>Loki docs: high-cardinality labels build a huge index, flush tiny chunks, and reduce performance and cost-effectiveness.</li>
              <li>Loki docs: scalable deployments are multi-component and query-frontend based.</li>
              <li>Loki docs: OTel resource attributes promoted to labels are rewritten from dots to underscores, which the proxy can mirror on the Grafana side.</li>
              <li>Loki docs: unoptimized queries can need `10x` the suggested querier resources, so the published tier tables are a floor, not a worst case.</li>
              <li>Loki costs grow fast when the workload crosses published ingest tiers, because those tiers already assume a sizeable distributed footprint before storage and object-transfer overhead.</li>
            </ul>
          </div>
        </div>
      </section>

      <section className={styles.section}>
        <div className={styles.callout}>
          <Heading as="h2" className={styles.sectionTitle}>
            Follow-up docs and sources
          </Heading>
          <div className={styles.inlineLinks}>
            <Link to="/docs/cost-model/">Docs cost model</Link>
            <Link to="/docs/cost-and-comparison/">Docs comparison matrix</Link>
            <a href="https://grafana.com/docs/loki/latest/get-started/labels/">Loki labels docs</a>
            <a href="https://grafana.com/docs/loki/latest/get-started/architecture/">Loki architecture docs</a>
            <a href="https://docs.victoriametrics.com/victorialogs/">VictoriaLogs overview</a>
            <a href="https://docs.victoriametrics.com/victorialogs/keyconcepts/">VictoriaLogs key concepts</a>
            <a href="https://www.truefoundry.com/blog/victorialogs-vs-loki">TrueFoundry benchmark report</a>
            <Link to="/docs/performance/">Project performance docs</Link>
            <Link to="/docs/benchmarks/">Project benchmarks</Link>
          </div>
        </div>
      </section>
    </MarketingLayout>
  );
}
