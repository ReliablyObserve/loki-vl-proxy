#!/usr/bin/env python3
"""Scoped Docker memory and Loki parity proof; standard library only."""

import argparse
import concurrent.futures
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

FIELDS = ('trace_id', 'span_id', 'request_id', 'user_id', 'version', 'total')
METRICS = ('go_memstats_alloc_bytes', 'go_memstats_heap_inuse_bytes',
           'go_memstats_sys_bytes', 'process_resident_memory_bytes',
           'loki_vl_proxy_uptime_seconds')
SELECTOR = '{app="range-memory",env="production"}'


def sha256(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def write_json(path, value):
    Path(path).write_text(json.dumps(value, indent=2) + '\n')


def http(url, params=None, data=None, timeout=180):
    if params:
        url += '?' + urllib.parse.urlencode(params)
    headers = {'X-Scope-OrgID': '0', 'X-Query-Tags': 'Source=grafana-lokiexplore-app'}
    if data is not None:
        headers['Content-Type'] = 'application/json'
        data = json.dumps(data, separators=(',', ':')).encode()
    request = urllib.request.Request(url, data=data, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status, response.read()
    except urllib.error.HTTPError as error:
        return error.code, error.read()


def timestamp(fixture, index):
    rows, start, end = fixture['rows'], fixture['start'], fixture['end']
    if fixture['recent_heavy']:
        older = rows // 5
        second = (start + index * (45 * 3600) // older if index < older else
                  end - 3 * 3600 + (index - older) * (3 * 3600 - 20) // (rows - older))
    else:
        second = start + index * (48 * 3600 - 20) // rows
    return second * 10**9 + 20_500_000_000


def query(fixture, field, hours):
    selector = SELECTOR
    if field in ('request_id', 'user_id', 'total'):
        selector += (f' | json {field}="[\\"{field}\\"]"'
                     f' | drop __error__, __error_details__ | {field}!=""')
    else:
        selector += f' | {field}!=""'
    return {'query': f'sum by ({field}) (count_over_time({selector} [1m]))',
            'start': str((fixture['end'] - hours * 3600) * 10**9),
            'end': str(fixture['end'] * 10**9 + 554_000_000), 'step': '60000ms'}


def canonical(body):
    obj = json.loads(body)
    if obj.get('status') != 'success' or obj.get('warnings'):
        raise ValueError('result is not successful and warning-free')
    if obj.get('data', {}).get('resultType') != 'matrix':
        raise ValueError('result is not a matrix')
    result, labels_seen = [], set()
    for series in obj['data']['result']:
        labels = tuple(sorted(series['metric'].items()))
        if labels in labels_seen:
            raise ValueError('duplicate series')
        labels_seen.add(labels)
        values, times = [], set()
        for stamp, value in series['values']:
            stamp, value = Decimal(str(stamp)), Decimal(str(value))
            if not stamp.is_finite() or not value.is_finite() or stamp in times:
                raise ValueError('nonfinite value or duplicate timestamp')
            times.add(stamp)
            values.append((stamp, value))
        result.append((labels, tuple(sorted(values))))
    return sorted(result)


def expected_counts(fixture, field, hours):
    # A range evaluates at the inclusive start, then once per minute. Each
    # count_over_time window is (evaluation - 1m, evaluation], including the
    # minute preceding start. The final evaluation is the integer end second.
    lower = (fixture['end'] - hours * 3600 - 60) * 10**9
    upper = fixture['end'] * 10**9
    indices = [i for i in range(fixture['rows']) if lower < timestamp(fixture, i) <= upper
               and (field not in ('request_id', 'user_id', 'total') or i % 2 == 0)]
    if field == 'version':
        series = len({i % 2 for i in indices})
    elif field == 'user_id':
        series = len({i % 10 for i in indices})
    else:
        series = len(indices)
    return series, len(indices)


def validate_reference(body, fixture, field, hours):
    values = canonical(body)
    series, events = expected_counts(fixture, field, hours)
    total = sum((point[1] for _, points in values for point in points), Decimal(0))
    if len(values) != series or total != events:
        raise ValueError(f'incomplete Loki {field}/{hours}h: '
                         f'series {len(values)}/{series}, events {total}/{events}')
    if any(dict(labels).keys() != {field} for labels, _ in values):
        raise ValueError('unexpected reference labels')
    lower = Decimal(fixture['end'] - hours * 3600)
    upper = Decimal(fixture['end'])
    if any(not lower <= stamp <= upper or (stamp - lower) % 60 for _, points in values
           for stamp, _ in points):
        raise ValueError('reference timestamps do not match the query axis')
    return values


class Harness:
    def __init__(self, args):
        self.args = args
        self.home = Path(args.output).expanduser().resolve()
        self.ports = {'loki': args.loki_port, 'victorialogs': args.vl_port, 'proxy': args.proxy_port}

    def docker(self, *args, check=True):
        result = subprocess.run(['docker', '--context', self.args.context, *args],
                                capture_output=True, text=True)
        if check and result.returncode:
            raise RuntimeError(result.stderr[:2000])
        return result

    def compose(self, *args):
        return self.docker('compose', '-p', self.args.project, '-f',
                           str(self.home / 'compose.json'), *args)

    def url(self, service, path):
        return f'http://127.0.0.1:{self.ports[service]}{path}'

    def fixture(self):
        fixture = json.loads((self.home / 'fixture.json').read_text())
        if fixture['rows'] < 10 or fixture['rows'] % 10:
            raise ValueError('fixture rows must be a positive multiple of ten')
        return fixture

    def owned(self):
        marker = json.loads((self.home / 'owner.json').read_text())
        if marker != {'project': self.args.project, 'context': self.args.context}:
            raise RuntimeError('output belongs to a different Docker project/context')

    def existing(self):
        if (self.home / 'owner.json').exists():
            self.owned()
        elif not self.args.existing_stack:
            raise RuntimeError('unowned output; --existing-stack permits reference/measure only')
        # Never inspect or clean a container outside the exact Compose project.
        for cid in self.compose('ps', '-aq').stdout.split():
            labels = json.loads(self.docker('inspect', cid).stdout)[0]['Config']['Labels']
            if labels.get('com.docker.compose.project') != self.args.project:
                raise RuntimeError('Compose project label mismatch')

    def wait(self, service, path):
        deadline = time.monotonic() + self.args.ready_timeout
        while time.monotonic() < deadline:
            try:
                status, _ = http(self.url(service, path), timeout=2)
                if status == 200:
                    return
            except (OSError, urllib.error.URLError):
                pass
            threading.Event().wait(.25)
        raise RuntimeError(service + ' not ready')

    def setup(self):
        self.home.mkdir(parents=True, exist_ok=True)
        if any(self.home.iterdir()):
            raise RuntimeError('setup requires an empty output directory')
        # Refuse to adopt an existing project with a coincidentally matching name.
        if self.docker('ps', '-aq', '--filter',
                       f'label=com.docker.compose.project={self.args.project}').stdout.strip():
            raise RuntimeError('Docker project already has containers')
        if self.docker('volume', 'ls', '-q', '--filter',
                       f'label=com.docker.compose.project={self.args.project}').stdout.strip():
            raise RuntimeError('Docker project already has volumes')
        config = Path(__file__).with_name('loki.yaml').read_text()
        (self.home / 'loki.yaml').write_text(config)
        spec = {'services': {
            'loki': {'image': 'grafana/loki:3.7.9', 'mem_limit': '3g', 'user': '0:0',
                     'ports': [f'127.0.0.1:{self.ports["loki"]}:3100'],
                     'command': ['-config.file=/etc/loki/local-config.yaml'],
                     'volumes': [{'type': 'bind', 'source': str(self.home / 'loki.yaml'),
                                  'target': '/etc/loki/local-config.yaml', 'read_only': True},
                                 'loki-data:/tmp/loki']},
            'victorialogs': {'image': 'victoriametrics/victoria-logs:v1.52.0', 'mem_limit': '2g',
                            'ports': [f'127.0.0.1:{self.ports["victorialogs"]}:9428'],
                            'volumes': ['vl-data:/vlogs'],
                            'command': ['-storageDataPath=/vlogs', '-retentionPeriod=3d',
                                        '-memory.allowedBytes=512MiB', '-search.maxConcurrentRequests=4',
                                        '-search.maxQueryDuration=90s']}},
                'volumes': {'loki-data': {}, 'vl-data': {}}}
        write_json(self.home / 'compose.json', spec)
        write_json(self.home / 'owner.json', {'project': self.args.project, 'context': self.args.context})
        self.compose('up', '-d', '--no-build')
        self.wait('victorialogs', '/health')
        self.wait('loki', '/ready')

    def seed(self):
        self.owned()
        # Persist before the first push: even a partially failed seed must never
        # duplicate VictoriaLogs rows on retry. Cleanup and create a fresh stack.
        lock = self.home / 'seed-started.json'
        with lock.open('x') as stream:
            json.dump({'started': time.time()}, stream)
        if (self.home / 'fixture.json').exists():
            raise RuntimeError('refuse fixture reseeding')
        end = (int(time.time()) // 60) * 60 - 600
        fixture = {'rows': self.args.rows, 'start': end - 48 * 3600, 'end': end,
                   'metadata': 'trace_id/span_id', 'kinds': ['json', 'logfmt'], 'recent_heavy': True}
        digest = hashlib.sha256()
        for first in range(0, fixture['rows'], 2000):
            streams, vlstreams = [], []
            for kind in ('json', 'logfmt'):
                values, vlvalues = [], []
                for index in range(first, min(first + 2000, fixture['rows'])):
                    if ('json' if index % 2 == 0 else 'logfmt') != kind:
                        continue
                    stamp = str(timestamp(fixture, index))
                    metadata = {'trace_id': f'{index:032x}', 'span_id': f'{index:016x}'}
                    if kind == 'json':
                        entry = {'message': 'fixed fixture', 'request_id': f'req-{index:08x}',
                                 'user_id': f'u-{index % 10}', 'total': index,
                                 'throughput_rps': 1.25, 'level': 'info'}
                        line = json.dumps(entry, separators=(',', ':'))
                        # VL's Loki ingestion maps JSON keys to fields. Keep the
                        # original line in _msg so both backends return it intact.
                        vlline = json.dumps({**entry, '_msg': line}, separators=(',', ':'))
                    else:
                        line = f'level=info ttl={index % 86400}s success=true'
                        vlline = line
                    values.append([stamp, line, metadata])
                    vlvalues.append([stamp, vlline, metadata])
                    digest.update(json.dumps([stamp, line, metadata], sort_keys=True).encode())
                labels = {'env': 'production', 'namespace': 'proof', 'service_name': 'range-memory',
                          'app': 'range-memory', 'version': 'v1' if kind == 'json' else 'v2', 'format': kind}
                streams.append({'stream': labels, 'values': values})
                vlstreams.append({'stream': labels, 'values': vlvalues})
            for service, path, payload in (
                ('loki', '/loki/api/v1/push', streams),
                ('victorialogs', '/insert/loki/api/v1/push', vlstreams),
            ):
                status, body = http(self.url(service, path), data={'streams': payload})
                if status not in (200, 204):
                    raise RuntimeError(f'{service} seed failed: {status} {body[:400]!r}')
        fixture['sha256'] = digest.hexdigest()
        write_json(self.home / 'fixture.json', fixture)
        self.flush()

    def flush(self):
        for service, path in (('loki', '/flush'), ('victorialogs', '/internal/force_flush')):
            status, body = http(self.url(service, path), data={})
            if status not in (200, 204):
                raise RuntimeError(f'{service} flush failed: {status} {body[:400]!r}')

    def binary(self):
        value = getattr(self.args, self.args.source + '_binary')
        return Path(value).expanduser().resolve() if value else self.home / ('proxy-' + self.args.source)

    def start(self):
        self.owned()
        binary = self.binary()
        if not binary.is_file():
            raise RuntimeError('missing binary: ' + str(binary))
        spec = json.loads((self.home / 'compose.json').read_text())
        spec['services']['proxy'] = {
            'image': self.args.runtime_image, 'mem_limit': self.args.memory,
            'entrypoint': ['/usr/local/bin/loki-vl-proxy'],
            'environment': {'GOMEMLIMIT': self.args.go_memory_limit},
            'ports': [f'127.0.0.1:{self.ports["proxy"]}:3100'],
            'volumes': [{'type': 'bind', 'source': str(binary),
                         'target': '/usr/local/bin/loki-vl-proxy', 'read_only': True}],
            'command': ['-listen=:3100', '-backend=http://victorialogs:9428',
                        '-max-stats-query-series=1000000', '-max-concurrent=0',
                        '-rate-limit-per-second=0', '-rate-limit-burst=0', '-backend-timeout=90s',
                        '-label-style=underscores', '-metadata-field-mode=translated',
                        '-emit-structured-metadata=true', '-patterns-enabled=true',
                        '-patterns-autodetect-from-queries=true', '-cache-ttl=5s',
                        '-metadata-default-lookback=1h', '-log-level=info',
                        '-server.register-instrumentation=true']}
        write_json(self.home / 'compose.json', spec)
        self.compose('up', '-d', '--no-deps', '--force-recreate', 'proxy')
        self.wait('proxy', '/ready')

    def reference(self):
        self.existing()
        self.flush()
        fixture = self.fixture()
        for field in FIELDS:
            params = query(fixture, field, self.args.hours)
            deadline = time.monotonic() + self.args.reference_wait
            while True:
                try:
                    status, body = http(self.url('loki', '/loki/api/v1/query_range'), params)
                    if status != 200:
                        raise ValueError(f'Loki HTTP {status}: {body[:400]!r}')
                    values = validate_reference(body, fixture, field, self.args.hours)
                    break
                except (ValueError, OSError, urllib.error.URLError):
                    if time.monotonic() >= deadline:
                        raise
                    threading.Event().wait(1)
            path = self.home / f'loki-{field}-{self.args.hours}h.body'
            path.write_bytes(body)
            write_json(path.with_suffix('.json'), {'query': params, 'fixture': fixture,
                       'sha256': sha256(path), 'series': len(values), 'oracle_validated': True})
            print(json.dumps({'field': field, 'series': len(values), 'bytes': len(body)}), flush=True)

    def measure(self):
        self.existing()
        fixture = self.fixture()
        # Validate even references produced by an earlier harness. Missing or
        # incomplete references fail before applying pressure to the proxy.
        for field in FIELDS:
            validate_reference((self.home / f'loki-{field}-{self.args.hours}h.body').read_bytes(),
                               fixture, field, self.args.hours)
        run = self.home / f'{self.args.source}-{self.args.hours}h-c6-{time.time_ns()}'
        run.mkdir()
        binary = self.binary()
        spec = json.loads((self.home / 'compose.json').read_text())
        cid = self.compose('ps', '-aq', 'proxy').stdout.strip()
        if not cid or len(cid.split()) != 1:
            raise RuntimeError('expected exactly one proxy container')
        before = json.loads(self.docker('inspect', cid).stdout)[0]
        mounts = [m for m in before['Mounts'] if m['Destination'] == '/usr/local/bin/loki-vl-proxy']
        if len(mounts) != 1 or mounts[0]['RW'] or Path(mounts[0]['Source']).resolve() != binary:
            raise RuntimeError('running proxy does not mount the selected binary read-only')
        binary_hash = sha256(binary)
        containers = self.compose('ps', '-aq').stdout.split()
        stack = [json.loads(self.docker('inspect', container).stdout)[0] for container in containers]
        write_json(run / 'metadata.json', {'fixture': fixture, 'binary_sha256': binary_hash,
                   'source': self.args.source, 'context': self.args.context, 'project': self.args.project,
                   'queries': {field: query(fixture, field, self.args.hours) for field in FIELDS},
                   'compose': spec, 'loki_config': (self.home / 'loki.yaml').read_text(),
                   'containers_before': stack, 'docker_version': self.docker('version', '--format', '{{json .}}').stdout,
                   'sample_interval_seconds': self.args.sample_interval})
        stop, samples = threading.Event(), []

        def sample():
            while not stop.is_set():
                try:
                    status, body = http(self.url('proxy', '/metrics'), timeout=2)
                    metrics = {line.split()[0]: float(line.split()[-1])
                               for line in body.decode().splitlines() if line.split() and line.split()[0] in METRICS}
                    samples.append({'time': time.time(), 'status': status, 'metrics': metrics})
                except (OSError, ValueError, urllib.error.URLError) as error:
                    samples.append({'time': time.time(), 'error': str(error)})
                stop.wait(self.args.sample_interval)

        barrier = threading.Barrier(len(FIELDS))

        def run_one(item):
            index, field = item
            params = query(fixture, field, self.args.hours)
            barrier.wait()
            started = time.monotonic()
            result = {'field': field, 'query': params, 'matches_loki': False}
            try:
                status, body = http(self.url('proxy', '/loki/api/v1/query_range'), params)
                result['elapsed'] = time.monotonic() - started
                (run / f'{index}-{field}.body').write_bytes(body)
                result.update(status=status, bytes=len(body), sha256=hashlib.sha256(body).hexdigest())
                if status != 200:
                    raise ValueError(f'HTTP {status}: {body[:400]!r}')
            except (OSError, ValueError, urllib.error.URLError) as error:
                result['error'] = str(error)
            result.setdefault('elapsed', time.monotonic() - started)
            return result

        monitor = threading.Thread(target=sample, daemon=True)
        monitor.start()
        results, ready_error = [], None
        try:
            self.wait('proxy', '/ready')
            with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
                results = list(pool.map(run_one, enumerate(FIELDS)))
        except RuntimeError as error:
            ready_error = str(error)
        finally:
            stop.set()
            monitor.join()
            write_json(run / 'memory.json', samples)
        state = json.loads(self.docker('inspect', cid).stdout)[0]['State']
        write_json(run / 'container-state.json', state)
        logs = self.compose('logs', '--no-color', 'proxy')
        (run / 'proxy.log').write_text(logs.stdout + logs.stderr)
        # Parse large matrices sequentially after pressure/sampling ends. The
        # proof client must not inflate host memory with six decoded references
        # while measuring the container's response path.
        for index, result in enumerate(results):
            if result.get('status') != 200:
                continue
            field = result['field']
            try:
                actual = canonical((run / f'{index}-{field}.body').read_bytes())
                reference = canonical((self.home / f'loki-{field}-{self.args.hours}h.body').read_bytes())
                result.update(series=len(actual), points=sum(len(points) for _, points in actual),
                              matches_loki=actual == reference)
                del actual, reference
            except ValueError as error:
                result['error'] = str(error)
        has_memory = any(all(key in sample.get('metrics', {}) for key in
                            ('go_memstats_alloc_bytes', 'process_resident_memory_bytes')) for sample in samples)
        passed = (not ready_error and has_memory and len(results) == 6 and all(r['matches_loki'] for r in results)
                  and state['Running'] and not state['OOMKilled'] and sha256(binary) == binary_hash)
        expected_failure = (self.args.source == 'baseline' and self.args.allow_baseline_oom
                            and not passed and state['OOMKilled'])
        summary = {'source': self.args.source, 'hours': self.args.hours, 'concurrency': 6,
                   'fixture': fixture, 'binary_sha256': binary_hash, 'requests': results,
                   'state': state, 'samples': len(samples), 'passed': passed,
                   'expected_baseline_oom': expected_failure, 'ready_error': ready_error,
                   'has_memory_samples': has_memory,
                   'peaks': {key: max((s['metrics'][key] for s in samples if key in s.get('metrics', {})),
                                     default=None) for key in METRICS}}
        write_json(run / 'summary.json', summary)
        print(json.dumps({'run': str(run), 'summary': summary}), flush=True)
        return 0 if passed or expected_failure else 1

    def cleanup(self):
        self.owned()
        self.existing()
        # No global prune and no orphan removal. Only this project's declared
        # services and volumes are removed; evidence files remain on the host.
        self.compose('down', '-v')


def parser():
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument('operation', choices=('setup', 'seed', 'start', 'reference', 'measure', 'cleanup'))
    result.add_argument('--output', required=True, help='dedicated evidence/config directory')
    result.add_argument('--project', default='drilldown-memory-proof', help='dedicated Docker Compose project')
    result.add_argument('--context', default='desktop-linux', help='explicit Docker context (never switches the active context)')
    result.add_argument('--loki-port', type=int, default=63101)
    result.add_argument('--vl-port', type=int, default=63428)
    result.add_argument('--proxy-port', type=int, default=63310)
    result.add_argument('--source', choices=('baseline', 'candidate'), default='candidate')
    result.add_argument('--baseline-binary', help='Linux binary, defaults to OUTPUT/proxy-baseline')
    result.add_argument('--candidate-binary', help='Linux binary, defaults to OUTPUT/proxy-candidate')
    result.add_argument('--runtime-image', default='loki-vl-proxy:e2e-local')
    result.add_argument('--memory', default='2g', help='proxy Docker memory limit')
    result.add_argument('--go-memory-limit', default='2GiB')
    result.add_argument('--rows', type=int, default=360000, help='fixture rows, multiple of ten')
    result.add_argument('--hours', type=int, choices=(12, 48), default=48)
    result.add_argument('--sample-interval', type=float, default=.25, help='metrics sampling seconds')
    result.add_argument('--ready-timeout', type=float, default=90)
    result.add_argument('--reference-wait', type=float, default=180, help='completeness retry budget per field')
    result.add_argument('--allow-baseline-oom', action='store_true', help='baseline OOM is recorded as expected; candidate failures always fail')
    result.add_argument('--existing-stack', action='store_true', help='allow reference/measure on an existing unowned output; no lifecycle operations')
    return result


def main():
    arguments = parser()
    args = arguments.parse_args()
    if not re.fullmatch(r'[a-z0-9][a-z0-9_-]{2,62}', args.project):
        arguments.error('project must be 3–63 lowercase letters, digits, hyphens or underscores')
    if args.rows < 10 or args.rows % 10:
        arguments.error('rows must be a positive multiple of ten')
    if min(args.sample_interval, args.ready_timeout, args.reference_wait) <= 0:
        arguments.error('sampling and timeout values must be positive')
    if any(not 1 <= port <= 65535 for port in (args.loki_port, args.vl_port, args.proxy_port)):
        arguments.error('ports must be in 1–65535')
    if len({args.loki_port, args.vl_port, args.proxy_port}) != 3:
        arguments.error('service ports must be distinct')
    if args.existing_stack and args.operation not in ('reference', 'measure'):
        arguments.error('--existing-stack only permits reference and measure')
    try:
        return getattr(Harness(args), args.operation)() or 0
    except (OSError, ValueError, RuntimeError, KeyError) as error:
        print(json.dumps({'operation': args.operation, 'error': str(error)}), file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
