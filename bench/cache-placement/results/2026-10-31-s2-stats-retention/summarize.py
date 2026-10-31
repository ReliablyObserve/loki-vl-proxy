"""Audit retained fleet cells and summarize measured requests separately from priming."""
import csv
import gzip
import hashlib
import json
from pathlib import Path

root = Path(__file__).parent
rows = []
request_paths = {}
for source in ['main', 'pr']:
    index = json.loads((root/'fleet'/f'{source}-index.json').read_text())
    seen = set()
    for entry in index['cell_files']:
        raw = gzip.decompress((root/'fleet'/entry['path']).read_bytes())
        assert hashlib.sha256(raw).hexdigest() == entry['sha256']
        cell = json.loads(raw)
        assert not cell['Errors']
        assert cell['Summary']['equal_reference'] == 20
        assert cell['Summary']['successful_valid_200'] == 20
        batches = cell['Measurements']
        key = (cell['Cell']['State'], cell['Cell']['RF'])
        assert key not in seen
        seen.add(key)
        responses = [row for batch in batches for row in batch['Rows']]
        assert len(responses) == 20
        assert all(row['Equal'] and row['ProxyStatus'] == 200 and row['LokiStatus'] == 200
                   and not row['ComparisonError'] for row in responses)
        request_paths[source, key] = [row['Path'] for row in responses]
        observations = [b['Observation'] for b in batches]
        assert all(o['Valid'] for o in observations)
        def backend(key):
            return sum(o['Backend'][key] for o in observations)
        def metric(layer, metric_name, tier=None):
            label = f'layer="{layer}",component="primary"'
            if tier:
                label += f',tier="{tier}"'
            key = f'loki_vl_proxy_cache_layer_{metric_name}{{{label}}}'
            values = []
            for obs in observations:
                view = obs['ProxyLoki']['OtherCacheLayerMetrics'] if layer == 'loki' else obs['ProxyVL']['CacheLayerMetrics']
                values.append(view['CounterDeltas'].get(key))
            return None if any(v is None for v in values) else sum(values)
        lat = cell['Summary']['latency_by_success_class']['success_nonempty']
        row = {'source':source, 'state':cell['Cell']['State'], 'rf':cell['Cell']['RF'],
               'responses':cell['Summary']['responses'], 'equal_loki':cell['Summary']['equal_reference'],
               'proxy_p50_ms':lat['proxy_p50_ms'], 'proxy_p95_ms':lat['proxy_p95_ms'],
               'loki_p50_ms':lat['loki_p50_ms'], 'loki_p95_ms':lat['loki_p95_ms'],
               'phase_proxy_cpu_ms':sum(o['ProxyCPUSeconds'] for o in observations)*1000,
               'vl_calls':backend('VLCalls'), 'vl_body_bytes':backend('VLBytes'),
               'peer_calls':backend('PeerCalls'), 'peer_response_body_bytes':backend('PeerBytes'),
               'peer_push_body_bytes':backend('PeerPushBytes'),
               'sampled_alive_cache_files_max_bytes':max(o['After']['FileBytes'] for o in observations)}
        for layer in ['loki','vl']:
            for tier in ['l1_memory','l2_disk','l3_peer']:
                row[f'{layer}_{tier}_hits'] = metric(layer,'hits_total',tier)
                row[f'{layer}_{tier}_lookups'] = metric(layer,'requests_total',tier)
                row[f'{layer}_{tier}_evictions'] = metric(layer,'evictions_total',tier)
            for name in ['disk_encoded_read_bytes_total','disk_encoded_write_bytes_total']:
                row[layer+'_'+name] = metric(layer,name)
        rows.append(row)
    assert seen == {(state, rf) for state in ['exact','shifted','refresh','restart','owner-loss'] for rf in [1,2]}
    wire = gzip.decompress((root/'fleet'/index['wire_log']).read_bytes())
    assert hashlib.sha256(wire).hexdigest() == index['wire_uncompressed_sha256']
for source, key in request_paths:
    assert request_paths['main', key] == request_paths['pr', key]
with (root/'fleet-summary.csv').open('w',newline='') as file:
    writer=csv.DictWriter(file,fieldnames=list(rows[0]))
    writer.writeheader()
    writer.writerows(rows)
(root/'fleet-summary.json').write_text(json.dumps(rows,indent=2)+'\n')
lines=['| State | RF | Main p50 / p95 ms | PR p50 / p95 ms | VL calls main / PR | Peer calls main / PR |',
       '|---|---:|---:|---:|---:|---:|']
for base in [r for r in rows if r['source']=='main']:
    candidate=next(r for r in rows if r['source']=='pr' and r['state']==base['state'] and r['rf']==base['rf'])
    lines.append(f'| {base["state"]} | {base["rf"]} | {base["proxy_p50_ms"]:.3f} / {base["proxy_p95_ms"]:.3f} | '
                 f'{candidate["proxy_p50_ms"]:.3f} / {candidate["proxy_p95_ms"]:.3f} | {base["vl_calls"]} / {candidate["vl_calls"]} | '
                 f'{base["peer_calls"]} / {candidate["peer_calls"]} |')
(root/'fleet-matrix.md').write_text('\n'.join(lines)+'\n')
print('\n'.join(lines))
print('Verified',sum(r['responses'] for r in rows),'nonempty HTTP200 answers equal Loki; all20cells valid.')
