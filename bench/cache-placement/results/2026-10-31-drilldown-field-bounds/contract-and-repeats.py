import json
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).parent
def get(base, path, params=None, tagged=False):
    url = base + path + ('?' + urllib.parse.urlencode(params) if params else '')
    headers = {'X-Scope-OrgID': '0'}
    if tagged: headers['X-Query-Tags'] = 'Source=grafana-lokiexplore-app'
    started = time.monotonic()
    try:
        with urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=45) as r:
            status, body = r.status, r.read(8 * 1024 * 1024 + 1)
    except urllib.error.HTTPError as e:
        status, body = e.code, e.read(10000)
    try: parsed = json.loads(body)
    except json.JSONDecodeError: parsed = {'error': body.decode(errors='replace')}
    return {'status': status, 'elapsed_ms': (time.monotonic()-started)*1000, 'bytes': len(body), 'body': parsed}

def metrics(base):
    text = urllib.request.urlopen(base + '/metrics', timeout=5).read().decode()
    out = {}
    for line in text.splitlines():
        if not line or line.startswith('#'): continue
        try:
            key, value = line.rsplit(' ', 1); value = float(value)
        except ValueError: continue
        if key.startswith(('process_', 'go_memstats_', 'loki_vl_proxy_cache_layer_')) or key.startswith('loki_vl_proxy_requests_total{system="vl"'):
            out[key] = value
    return out

def canonical(body):
    return sorted((json.dumps(s['metric'], sort_keys=True), s.get('values', [])) for s in body['data']['result'])

reference = 'http://127.0.0.1:14101'
now = time.time_ns()
names = get(reference, '/loki/api/v1/label/service_name/values', {'start': str(now-48*3600*10**9), 'end': str(now)})['body']['data']
service = sorted(n for n in names if n.startswith('field-bounds-ui-') and n.endswith('-48'))[-1]
stamp = int(service.split('-')[-2])
end = (stamp // 60000) * 60 - 600
reports = []
for hours in (24, 48):
    query = f'sum by (trace_id) (count_over_time({{service_name="{service}"}} | json | drop __error__, __error_details__ | trace_id!="" [5m]))'
    params = {'query': query, 'start': str((end-hours*3600)*10**9), 'end': str(end*10**9), 'step': '300'}
    loki_plain = get(reference, '/loki/api/v1/query_range', params)
    loki_partial = get(reference, '/loki/api/v1/query_range', params, True)
    assert loki_plain['status'] == 400, loki_plain
    assert loki_partial['status'] == 200 and len(loki_partial['body']['data']['result']) == 500, loki_partial
    warning = loki_partial['body']['warnings']
    for name, base in [('main', 'http://127.0.0.1:14100'), ('pr', 'http://127.0.0.1:14103')]:
        plain = get(base, '/loki/api/v1/query_range', params)
        primed = get(base, '/loki/api/v1/query_range', params, True)
        assert plain['status'] == 400, (name, plain)
        assert primed['status'] == 200 and len(primed['body']['data']['result']) == 500, (name, primed)
        assert primed['body']['warnings'] == warning, (name, primed['body'].get('warnings'), warning)
        selected = sorted(s['metric']['trace_id'] for s in primed['body']['data']['result'])
        filtered = dict(params, query=query.replace('trace_id!=""', 'trace_id=~"' + '|'.join(selected) + '"'))
        oracle = get(reference, '/loki/api/v1/query_range', filtered)
        assert oracle['status'] == 200 and canonical(oracle['body']) == canonical(primed['body']), (name, hours, 'selected points differ from Loki')
        before = metrics(base)
        samples = []
        for i in range(20):
            r = get(base, '/loki/api/v1/query_range', params, True)
            assert r['status'] == 200 and canonical(r['body']) == canonical(primed['body']), (name, hours, i)
            samples.append(r['elapsed_ms'])
        after = metrics(base)
        deltas = {k: v-before.get(k,0) for k,v in after.items() if v != before.get(k,0)}
        ordered = sorted(samples)
        reports.append({'source': name, 'hours': hours, 'query': query, 'range_end': end, 'samples': 20, 'samples_ms': samples, 'p50_ms': (ordered[9]+ordered[10])/2, 'p95_ms': ordered[18], 'bytes': primed['bytes'], 'series': 500, 'warning': warning, 'selected_points_equal_real_loki': True, 'plain_status_proxy': plain['status'], 'plain_status_loki': loki_plain['status'], 'loki_partial_series': 500, 'priming_elapsed_ms': primed['elapsed_ms'], 'metrics_before': before, 'metrics_after': after, 'metric_deltas': deltas})
        print(json.dumps({k:v for k,v in reports[-1].items() if k not in ('query','metrics_before','metrics_after','metric_deltas','samples_ms')}), flush=True)
(ROOT / 'contract-and-repeats.json').write_text(json.dumps({'reference': 'Official Loki 3.7.9, configured500; same dual-written unique fixture', 'service': service, 'scope': 'Both proxy sources configured500; this does not attribute playground limit-change benefit to code change. Dedicated process counter deltas include background work; VL CPU/device I/O not attributable.', 'cases': reports}, indent=2))
