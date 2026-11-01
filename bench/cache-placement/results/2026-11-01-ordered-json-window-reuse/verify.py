import gzip
import hashlib
import json
import re
from datetime import datetime
from pathlib import Path

OUT = Path(__file__).resolve().parent
OUT.mkdir(parents=True,exist_ok=True)

def digest(data): return hashlib.sha256(data).hexdigest()
def metric_sum(metrics,name,layer=None,tier=None):
    values=[v for k,v in metrics.items() if k.split('{')[0]==name and (layer is None or f'layer="{layer}"' in k) and (tier is None or f'tier="{tier}"' in k)]
    return sum(values) if values else None
def difference(before,after,name,layer=None,tier=None):
    a,b=metric_sum(before,name,layer,tier),metric_sum(after,name,layer,tier)
    return b-a if a is not None and b is not None and b>=a else None
def stamp(raw):
    if raw.replace('.','',1).isdigit():
        value=float(raw)
        return datetime.fromtimestamp(value/1e9 if value>1e14 else value)
    return datetime.fromisoformat(raw.replace('Z','+00:00'))
def summarize(cell):
    rows=[r for b in cell['Measurements'] for r in b['Rows']]
    observations=[b['Observation'] for b in cell['Measurements']]
    assert rows and all(r['Successful'] and r['Equal'] and r['ProxyStatus']==200 and r['ReferenceClass']=='success_nonempty' for r in rows),cell['Cell']['ID']
    assert all(o['Valid'] for o in observations),cell['Cell']['ID']
    assert not cell['Errors'],cell['Errors']
    bulk=[]
    for o in observations:
        for r in o['BackendRequests'] or []:
            if r['Path']=='/select/logsql/query' and not re.search(r'\| limit 1(?:\s|$)',r['Query'].get('query',[''])[0]):
                bulk.append((stamp(r['Query']['end'][0])-stamp(r['Query']['start'][0])).total_seconds())
    first,last=observations[0]['Before'],observations[-1]['After']
    before,after=first['Metrics'],last['Metrics']
    def phase_difference(name,layer=None,tier=None):
        deltas=[difference(o['Before']['Metrics'],o['After']['Metrics'],name,layer,tier) for o in observations]
        return sum(deltas) if all(d is not None for d in deltas) else None
    layers={}
    for layer in ['loki','vl','unknown']:
        layers[layer]={'l1_logical_bytes':metric_sum(after,'loki_vl_proxy_cache_layer_bytes',layer,'l1_memory'),
                       'l2_logical_bytes':metric_sum(after,'loki_vl_proxy_cache_layer_bytes',layer,'l2_disk')}
        for suffix in ['requests_total','hits_total','read_bytes_total','evictions_total','rejections_total','disk_encoded_write_bytes_total']:
            layers[layer][suffix]=phase_difference('loki_vl_proxy_cache_layer_'+suffix,layer)
    return {'samples':len(rows),'equal_reference':sum(r['Equal'] for r in rows),'latency':cell['Summary']['latency_by_success_class']['success_nonempty'],
            'vl_calls':sum(o['Backend']['VLCalls'] for o in observations),'vl_body_bytes':sum(o['Backend']['VLBytes'] for o in observations),
            'bulk_reads':len(bulk),'bulk_max_seconds':max(bulk,default=0),'bulk_total_seconds':sum(bulk),
            'peer_fetches':sum(o['Backend']['PeerFetches'] for o in observations),'peer_pushes':sum(o['Backend']['PeerPushes'] for o in observations),
            'peer_push_request_bytes':sum(o['Backend']['PeerPushBytes'] for o in observations),
            'production_backend_decoded_bytes':phase_difference('loki_vl_proxy_backend_read_bytes_total'),
            'peer_http_body_bytes':sum(o['Backend']['PeerBytes'] for o in observations),
            'phase_cpu_seconds':sum(o['ProxyCPUSeconds'] for o in observations) if all(o['ProxyCPUSeconds'] is not None for o in observations) else None,
            'go_memory_gauge_bytes':metric_sum(after,'loki_vl_proxy_process_resident_memory_bytes'), 'memory_gauge_scope':last['MemoryMetricKind'],
            'max_individual_lifetime_kernel_rss_bytes':max(x['peak_rss_bytes'] for x in cell['ProcessLifetimes']),
            'bbolt_file_bytes':last['FileBytes'],'bbolt_allocated_bytes':last['AllocatedBytes'], 'layers':layers,
            'priming':[{'name':p['Name'],'first_proxy_ms':p['Rows'][0]['ProxyMS'],'parity':all(r['Equal'] for r in p['Rows']),'backend':p['Observation']['Backend'],'phase_cpu_seconds':p['Observation']['ProxyCPUSeconds']} for p in cell['Priming']],
            'process_lifetimes':cell['ProcessLifetimes']}

if __name__ == "__main__":
    provenance = json.loads((OUT / "provenance.json").read_text())
    actual = {}
    for label in ("main", "pr"):
        cells = {}
        for item in provenance[label]["index"]:
            raw = gzip.decompress((OUT / item["file"]).read_bytes())
            assert digest(raw) == item["uncompressed_sha256"], item["file"]
            cell = json.loads(raw)
            key = cell["Cell"]["ID"]
            assert key not in cells, key
            cells[key] = summarize(cell)
        for item in provenance[label]["runs"]:
            raw = gzip.decompress((OUT / item["file"]).read_bytes())
            assert digest(raw) == item["uncompressed_sha256"], item["file"]
        raw = gzip.decompress((OUT / f"{label}-run.json.gz").read_bytes())
        assert digest(raw) == provenance[label]["report_sha256"], label
        actual[label] = cells
    assert actual["main"].keys() == actual["pr"].keys()
    assert all(cell["production_backend_decoded_bytes"] == cell["vl_body_bytes"] for cell in actual["pr"].values()), "Production bytes differ from observer"
    rows = [{"cell": key, "main": actual["main"][key], "pr": actual["pr"][key]} for key in sorted(actual["main"])]
    retained = sorted(json.loads((OUT / "comparison.json").read_text()), key=lambda row: row["cell"])
    assert rows == retained, "Derived summaries differ"
    for trial in json.loads((OUT / "control.json").read_text()):
        raw = gzip.decompress((OUT / trial["file"]).read_bytes())
        assert digest(raw) == trial["sha256"], trial["file"]
        cell = json.loads(raw)
        summary = summarize(cell)
        assert summary["equal_reference"] == 20
        assert cell["Summary"] == trial["summary"]
    print(f"Verified {len(rows)} paired cells, parity, resources, hashes and alternating-order controls.")
