"""compare.py on synthetic captures: identical, different, empty, and Loki differences the base has too."""
import json
import os
import subprocess
import sys
import tempfile
import unittest
from copy import deepcopy
from pathlib import Path
from unittest.mock import patch
from contextlib import redirect_stdout
from io import StringIO

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))
import compare  # noqa: E402


def capture(values, ui=None, settled=True, uid="ds1", fields=None, status=200, error=None):
    frame = {"schema": {"name": "", "fields": fields or [{"type": "time"}, {"type": "number", "labels": {"level": "info"}}]},
             "data": {"values": [list(range(len(values))), values]}}
    result = {"error": error} if error else {"frames": [frame]}
    rec = {"url": "/api/ds/query?ds_type=loki", "method": "POST", "status": status,
           "request": {"from": "1", "to": "2", "requestId": uid + "-1", "queries": [
               {"refId": "A", "expr": "sum(x)", "queryType": "range", "datasource": {"type": "loki", "uid": uid}, "maxLines": 1000}]},
           "response": {"results": {"A": result}}}
    return {"settled": settled, "settle_ms": 1000, "records": [rec], "ui": ui or {"noData": 0, "banners": [], "panelErrors": 0}}


class CompareTest(unittest.TestCase):
    def client_error_fixture(self):
        proof = json.loads(Path(HERE, '../../ab/results/oneoff/2026-11-21-mixed-parser-metrics/grafana-uppercase-volume-proof.json').resolve().read_text())
        page = proof['existing_contracts']['visual_scenes'][0]
        observed = next(c for c in proof['ci']['captures'] if c['scene_id'] == page)
        primary = next(q for q in observed['queries'] if q['ref_id'] == 'A')
        supporting = next(q for q in observed['queries'] if q['ref_id'] == 'log-volume-A')
        cap = capture([3, 4])
        cap['records'][0]['request'] = deepcopy(primary['request'])
        start = int(primary['request']['from'])
        cap['records'][0]['response']['results']['A']['frames'][0]['data']['values'][0] = [start, start + 5000]
        cap['records'].append(dict(url='/api/ds/query', method='POST', status=400,
                                   request=deepcopy(supporting['request']), response=deepcopy(supporting['response'])))
        cap['errors'] = [*deepcopy(observed['console_errors']), 'pageerror: Object']
        cap['runtime'] = dict(browser_thread_seconds=1, browser_task_seconds=1, metric_samples=2,
                              js_heap_max_sampled_bytes=1000, api_requests=2, api_encoded_bytes=100,
                              page_encoded_bytes=200, first_api_data_ms=300)
        cap['client_exceptions'] = dict(version=1, complete=True, overflow=False, playwright_page_error_count=1,
            events=[dict(event_id='runtime:1', exception_id=1, raw_error_index=len(cap['errors'])-1, kind='uncaught-object',
                         class_name='Object', own_property_names=['message', 'refId', 'status'],
                         ref_id='log-volume-A', status=400, message=supporting['response']['results']['log-volume-A']['error'],
                         stack=[dict(url='/public/build/client.js', line=1, column=1)])])
        case = dict(id=proof['registry_case'], gap=dict(status='upstream'), compare=dict(values=dict(
            client_error_contract='loki-metric-supporting-error-v1')))
        return page, dict(id=page, kind='explore', query=primary['expression'], covers=[case['id']]), dict(main=deepcopy(cap), pr=deepcopy(cap), loki=deepcopy(cap)), [(case, proof)]

    def rejection_fixture(self):
        proof = json.loads(Path(HERE, '../../ab/results/oneoff/2026-11-24-visual-client-errors/topk-over-log-selector-rejection-proof.json').resolve().read_text())
        page = 'explore-topk-over-log-selector'
        caps = {}
        for recorded in (c for c in proof['ci']['captures'] if c['scene_id'] == page and c['range'] == '1h'):
            records = [dict(url='/api/datasources/uid/vp-x/resources/labels?start=1&end=2', method='GET', status=200, response={'status': 'success', 'data': ['app']})]
            for q in recorded['queries']:
                records.append(dict(url='/api/ds/query?ds_type=loki', method='POST', status=q['status'], request=deepcopy(q['request']), response=deepcopy(q['response'])))
            errors = [*deepcopy(recorded['console_errors']), 'pageerror: Object']
            caps[recorded['side']] = dict(
                settled=True, settle_ms=1000, records=records, errors=errors, ui=deepcopy(recorded['ui']),
                runtime=dict(browser_thread_seconds=1, browser_task_seconds=1, metric_samples=2, js_heap_max_sampled_bytes=1000,
                             api_requests=2, api_encoded_bytes=100, page_encoded_bytes=200, first_api_data_ms=300),
                client_exceptions=dict(version=1, complete=True, overflow=False, playwright_page_error_count=1, events=[dict(
                    event_id='runtime:1', exception_id=1, raw_error_index=len(errors) - 1, kind='uncaught-object', class_name='Object',
                    own_property_names=['message', 'refId', 'status'], ref_id=recorded['thrown']['ref_id'], status=recorded['thrown']['status'],
                    message=recorded['thrown']['message'], stack=[dict(url='/public/build/client.js', line=1, column=1)])]))
        case = dict(id=proof['registry_case'], gap=dict(status='upstream'), compare=dict(values=dict(client_error_contract='loki-query-rejection-v1')))
        entry = dict(id=page, kind='explore', query=proof['ci']['captures'][0]['queries'][0]['expression'], covers=[case['id']])
        return page, entry, caps, [(case, proof)]

    def test_an_answer_the_other_side_also_gave_one_more_time_is_no_difference(self):
        # explore-unwrap-conversion-error at 6h: Grafana asked the rejected query twice on the base and once on the PR.
        a, b = {("error", ""): ("error", "pipeline error")}, {("series", "x"): ("metric", {"a": 1})}
        key = ("query", "A", "q", "range", "1", "2", "c")
        for left, right in (({key: [a, a]}, {key: [a]}), ({key: [a]}, {key: [a, a]}), ({key: [a, a, a]}, {key: [a]})):
            _, ok, diffs, _, missing, _ = compare.compare(left, right)
            self.assertEqual((diffs, missing), ([], 0))
        # A different answer, or a request one side never made, is still a difference.
        for left, right in (({key: [a, b]}, {key: [a]}), ({key: [a]}, {key: [b]}), ({key: [a]}, {})):
            _, _, diffs, _, _, _ = compare.compare(left, right)
            self.assertTrue(diffs, (left, right))

    def test_a_rejection_the_page_asked_for_twice_is_the_rejection_asked_for_once(self):
        # explore-unwrap-conversion-error at 15m: Grafana sent the rejected query twice on both proxies and once on Loki.
        text = ("pipeline error: 'SampleExtractionErr' for series: '{app=\"%s\"}'.\nUse a label filter to intentionally skip "
                "this error. (e.g | __error__!=\"SampleExtractionErr\").")

        def capture(times, status=400, series="auth-service"):
            query = dict(refId="A", expr="max_over_time({a=\"b\"} | json | unwrap x [1m])", queryType="range", maxLines=1,
                         datasource=dict(uid="vp-x"), startNs="1", endNs="2")
            record = dict(url="/api/ds/query?ds_type=loki", method="POST", status=status, request=dict(queries=[query], **{"from": "1", "to": "2"}),
                          response=dict(results=dict(A=dict(status=status, error=text % series, frames=[]))))
            return dict(records=[deepcopy(record) for _ in range(times)])

        twice, once = compare.answer_evidence(capture(2)), compare.answer_evidence(capture(1))
        self.assertEqual(twice[0], once[0])
        self.assertEqual(twice[2], [])
        # The series the error names is whichever stream a shard met first; the status and the rest of the text still count.
        self.assertEqual(compare.answer_evidence(capture(2, series="frontend"))[0], once[0])
        self.assertNotEqual(compare.answer_evidence(capture(2, status=422))[0], once[0])
        changed = capture(1)
        changed["records"][0]["response"]["results"]["A"]["error"] = text % "auth-service" + " (a different closing sentence)"
        self.assertNotEqual(compare.answer_evidence(changed)[0], once[0])

    def test_query_rejection_contract_accepts_only_the_identical_recorded_rejections(self):
        page, entry, caps, contracts = self.rejection_fixture()
        evidence = compare.client_error_evidence(page, '1h', entry, caps, contracts=contracts)
        self.assertIsNotNone(evidence)
        self.assertEqual((evidence.receipt['contract'], evidence.receipt['state']), ('loki-query-rejection-v1', 'upstream_matches_loki'))
        self.assertTrue(caps['pr']['errors'])  # Evidence never deletes raw errors.
        for side in ('main', 'pr', 'loki'):
            self.assertEqual(compare.runtime_problems(caps[side], caps[side], required=True, client_error=evidence), [])
        caps['pr']['errors'].append('pageerror: TypeError: real new failure')
        self.assertTrue(compare.runtime_problems(caps['main'], caps['pr'], required=True, client_error=evidence))

    def test_query_rejection_contract_is_not_a_generic_exception_allowlist(self):
        page, entry, baseline, contracts = self.rejection_fixture()
        def supporting(c, side='pr'): return next(r for r in c[side]['records'] if r.get('request', {}).get('queries', [{}])[0].get('refId') == 'log-volume-A')
        def primary(c, side='pr'): return next(r for r in c[side]['records'] if r.get('request', {}).get('queries', [{}])[0].get('refId') == 'A')
        event = lambda c, side='pr': c[side]['client_exceptions']['events'][0]
        def all_sides(change):
            def run(c):
                for side in ('main', 'pr', 'loki'):
                    change(c, side)
            return run
        mutations = {
            'missing Loki': lambda c: c.pop('loki'),
            'old Object only': lambda c: c['pr'].pop('client_exceptions'),
            'incomplete capture': lambda c: c['pr']['client_exceptions'].update(complete=False),
            'extra page error': lambda c: c['pr']['client_exceptions'].update(playwright_page_error_count=2),
            'thrown for the primary': lambda c: event(c).update(ref_id='A'),
            'other status thrown': lambda c: event(c).update(status=500),
            'thrown message differs from the supporting DTO': lambda c: event(c).update(message='parse error: something else'),
            'primary answered 200': lambda c: primary(c).update(status=200),
            'supporting answered 500': lambda c: supporting(c).update(status=500),
            'one side rejects differently': lambda c: primary(c)['response']['results']['A'].update(error='parse error: another failure'),
            'frames returned': lambda c: primary(c)['response']['results']['A'].update(frames=[{'data': {'values': [[1], [1]]}}]),
            'extra request': lambda c: c['pr']['records'].append(deepcopy(primary(c))),
            'unknown query': lambda c: primary(c)['request']['queries'][0].update(refId='B'),
            'resource failure': lambda c: c['pr']['records'][0].update(status=500),
            'new console error': lambda c: c['pr']['errors'].insert(0, 'Error: something new'),
            'cancelled request': lambda c: primary(c).update(aborted=True),
            'primary expression changed': all_sides(lambda c, s: primary(c, s)['request']['queries'][0].update(expr='vector(1)')),
            'supporting expression changed': all_sides(lambda c, s: supporting(c, s)['request']['queries'][0].update(expr='vector(1)')),
            'query type changed': all_sides(lambda c, s: primary(c, s)['request']['queries'][0].update(queryType='instant')),
            'DTO changed on every side': all_sides(lambda c, s: primary(c, s)['response']['results']['A'].update(error='parse error: a new shared failure')),
        }
        for name, mutate in mutations.items():
            with self.subTest(name):
                caps = deepcopy(baseline)
                mutate(caps)
                self.assertIsNone(compare.client_error_evidence(page, '1h', entry, caps, contracts=contracts))
        # the contract is bound to its registry case, its page and its recorded range
        case, proof = contracts[0]
        for name, args in {
            'not an upstream gap': (page, '1h', entry, [(dict(case, gap=dict(status='fixed')), proof)]),
            'case not covered by the page': (page, '1h', dict(entry, covers=[]), contracts),
            'other page': ('another-page', '1h', dict(entry, id='another-page'), contracts),
            'other range': (page, '6h', entry, contracts),
            'proof of another case': (page, '1h', entry, [(case, dict(proof, registry_case='profiles/other'))]),
            'other contract kind': (page, '1h', entry, [(dict(case, compare=dict(values=dict(client_error_contract='loki-metric-supporting-error-v1'))), proof)]),
        }.items():
            with self.subTest(name):
                self.assertIsNone(compare.client_error_evidence(args[0], args[1], args[2], deepcopy(baseline), contracts=args[3]))

    def test_query_rejection_receipt_is_reported_without_claiming_primary_points(self):
        page, entry, caps, contracts = self.rejection_fixture()
        with tempfile.TemporaryDirectory() as out:
            directory = Path(out, 'data', page, '1h')
            directory.mkdir(parents=True)
            for side, cap in caps.items():
                Path(directory, side + '.json').write_text(json.dumps(cap))
            Path(out, 'plan.json').write_text(json.dumps({'runtime_proof': {'required': True}}))
            import plan
            with patch.object(sys, 'argv', ['compare.py', out]), patch.object(compare, 'client_error_contracts', return_value=contracts), \
                    patch.object(plan, 'load_spec', return_value={'pages': [entry], 'ranges': {'1h': 3600}}), redirect_stdout(StringIO()), self.assertRaises(SystemExit) as result:
                compare.main()
            self.assertEqual(result.exception.code, 0)
            rows = json.loads(Path(out, 'compare.json').read_text())
            self.assertEqual(rows[0]['expected_client_errors'][0]['contract'], 'loki-query-rejection-v1')
            text = Path(out, 'compare.md').read_text()
            self.assertIn('rejected with the identical 400 DTOs on all three builds', text)
            self.assertNotIn('primary points match', text)

    def test_registry_client_error_requires_complete_loki_triplet(self):
        page, entry, caps, contracts = self.client_error_fixture()
        evidence = compare.client_error_evidence(page, '1h', entry, caps, contracts=contracts)
        self.assertIsNotNone(evidence)
        self.assertEqual(evidence.receipt['registry_case'], contracts[0][0]['id'])
        self.assertEqual(evidence.receipt['state'], 'upstream_matches_loki')
        self.assertTrue(caps['pr']['errors'])  # Evidence never deletes raw errors.
        self.assertEqual(compare.runtime_problems(caps['main'], caps['pr'], required=True, client_error=evidence), [])
        caps['pr']['errors'].append('pageerror: TypeError: real new failure')
        self.assertTrue(compare.runtime_problems(caps['main'], caps['pr'], required=True, client_error=evidence))

    def test_registry_client_error_retains_full_fresh_captures(self):
        page, entry, caps, contracts = self.client_error_fixture()
        with tempfile.TemporaryDirectory() as out:
            directory = Path(out, 'data', page, '1h')
            directory.mkdir(parents=True)
            for side, cap in caps.items():
                Path(directory, side + '.json').write_text(json.dumps(cap))
            Path(out, 'plan.json').write_text(json.dumps({'runtime_proof': {'required': True}}))
            import plan
            with patch.object(sys, 'argv', ['compare.py', out]), patch.object(compare, 'client_error_contracts', return_value=contracts), \
                    patch.object(plan, 'load_spec', return_value={'pages': [entry], 'ranges': {'1h': 3600}}), redirect_stdout(StringIO()), self.assertRaises(SystemExit) as result:
                compare.main()
            self.assertEqual(result.exception.code, 0)
            rows = json.loads(Path(out, 'compare.json').read_text())
            self.assertEqual(rows[0]['expected_client_errors'][0]['state'], 'upstream_matches_loki')
            raw = json.loads(Path(out, 'expected-client-error-captures.json').read_text())
            self.assertEqual(raw[0]['captures'], caps)
            self.assertIn('upstream gap remains open', Path(out, 'compare.md').read_text())

    def test_client_error_retention_budget_cannot_accept_without_proof(self):
        page, entry, caps, contracts = self.client_error_fixture()
        with patch.object(compare, 'CLIENT_ERROR_PROOF_BYTES', 1):
            self.assertIsNone(compare.client_error_evidence(page, '1h', entry, caps, contracts=contracts))

    def test_client_error_requires_recorded_scope_even_if_all_three_change_together(self):
        page, entry, baseline, contracts = self.client_error_fixture()
        for changed in ('error', 'primary-expression', 'support-expression', 'support-type', 'interval', 'window'):
            with self.subTest(changed=changed):
                caps = deepcopy(baseline)
                for cap in caps.values():
                    if changed == 'error':
                        cap['records'][1]['response']['results']['log-volume-A']['error'] = 'parse error: another shared failure'
                        cap['client_exceptions']['events'][0]['message'] = 'parse error: another shared failure'
                    elif changed.endswith('expression'):
                        index = 0 if changed.startswith('primary') else 1
                        cap['records'][index]['request']['queries'][0]['expr'] = 'vector(1)'
                    elif changed == 'support-type':
                        cap['records'][1]['request']['queries'][0]['queryType'] = 'instant'
                    elif changed == 'interval':
                        cap['records'][1]['request']['queries'][0]['intervalMs'] = 1
                    else:
                        for record in cap['records']:
                            record['request']['to'] = str(int(record['request']['to']) + 3600000)
                self.assertIsNone(compare.client_error_evidence(page, '1h', entry, caps, contracts=contracts))

    def test_client_error_allows_a_fresh_shifted_window_but_not_a_new_shape(self):
        page, entry, caps, contracts = self.client_error_fixture()
        for cap in caps.values():
            for record in cap['records']:
                for key in ('from', 'to'):
                    record['request'][key] = str(int(record['request'][key]) + 3600000)
            times = cap['records'][0]['response']['results']['A']['frames'][0]['data']['values'][0]
            times[:] = [t + 3600000 for t in times]
        self.assertIsNotNone(compare.client_error_evidence(page, '1h', entry, caps, contracts=contracts))

    def test_registry_client_error_is_not_a_generic_exception_allowlist(self):
        page, entry, baseline, contracts = self.client_error_fixture()
        event = lambda c: c['pr']['client_exceptions']['events'][0]
        def primary(c): return c['pr']['records'][0]['response']['results']['A']
        mutations = {
            'missing Loki': lambda c: c.pop('loki'),
            'old Object only': lambda c: c['pr'].pop('client_exceptions'),
            'incomplete': lambda c: c['pr']['client_exceptions'].update(complete=False),
            'overflow': lambda c: c['pr']['client_exceptions'].update(overflow=True),
            'missing audit event': lambda c: c['pr']['client_exceptions'].update(playwright_page_error_count=0),
            'different event identity': lambda c: event(c).update(event_id='runtime:2'),
            'wrong raw index': lambda c: event(c).update(raw_error_index=0),
            'TypeError': lambda c: event(c).update(class_name='TypeError'),
            'unknown property': lambda c: event(c)['own_property_names'].append('newError'),
            'wrong ref': lambda c: event(c).update(ref_id='another-query'),
            'wrong exception message': lambda c: event(c).update(message='another error'),
            'wrong exception status': lambda c: event(c).update(status=500),
            'extra exception': lambda c: c['pr']['client_exceptions']['events'].append(deepcopy(event(c))),
            'extra raw error': lambda c: c['pr']['errors'].append('pageerror: Object'),
            'unknown console error': lambda c: c['pr']['errors'].append('new failure in client code'),
            'raw error missing': lambda c: c['pr'].update(errors=[]),
            'missing stack provenance': lambda c: event(c).update(stack=[]),
            'changed value': lambda c: primary(c)['frames'][0]['data']['values'][1].__setitem__(0, 99),
            'missing point': lambda c: primary(c)['frames'][0]['data']['values'][1].pop(),
            'changed label': lambda c: primary(c)['frames'][0]['schema']['fields'][1].update(labels={'new': 'label'}),
            'empty primary': lambda c: primary(c).update(frames=[]),
            'primary failed': lambda c: c['pr']['records'][0].update(status=400),
            'support failed differently': lambda c: c['pr']['records'][1].update(status=500),
            'support error different': lambda c: c['pr']['records'][1]['response']['results']['log-volume-A'].update(error='parse error: another'),
            'support extra field': lambda c: c['pr']['records'][1]['response']['results']['log-volume-A'].update(new=True),
            'support wrong expression': lambda c: c['pr']['records'][1]['request']['queries'][0].update(expr='sum(vector(1))'),
            'support wrong query type': lambda c: c['pr']['records'][1]['request']['queries'][0].update(queryType='instant'),
            'support missing source type': lambda c: c['pr']['records'][1]['request']['queries'][0].pop('supportingQueryType'),
            'support wrong interval': lambda c: c['pr']['records'][1]['request']['queries'][0].update(intervalMs=1),
            'primary wrong expression': lambda c: c['pr']['records'][0]['request']['queries'][0].update(expr='vector(1)'),
            'unreadable body': lambda c: c['pr']['records'][0].update(response=None),
            'unsettled': lambda c: c['pr'].update(settled=False),
        }
        for name, mutate in mutations.items():
            with self.subTest(name=name):
                caps = deepcopy(baseline)
                mutate(caps)
                self.assertIsNone(compare.client_error_evidence(page, '1h', entry, caps, contracts=contracts))
                self.assertTrue(compare.runtime_problems(caps['main'], caps['pr'], required=True))
        self.assertIsNone(compare.client_error_evidence('other-scene', '1h', entry, baseline, contracts=contracts))
        self.assertIsNone(compare.client_error_evidence(page, '24h', entry, baseline, contracts=contracts))
        self.assertIsNone(compare.client_error_evidence(page, '1h', dict(entry, query='vector(1)'), baseline, contracts=contracts))
        self.assertIsNone(compare.client_error_evidence(page, '1h', dict(entry, covers=[]), baseline, contracts=contracts))
        self.assertIsNone(compare.client_error_evidence(page, '1h', entry, baseline, contracts=[]))
        changed = deepcopy(contracts)
        changed[0][0]['gap']['status'] = 'fixed'
        self.assertIsNone(compare.client_error_evidence(page, '1h', entry, baseline, contracts=changed))

    def test_runtime_proof_keeps_real_javascript_page_errors(self):
        base = capture([1])
        base["runtime"] = dict(browser_thread_seconds=1, browser_task_seconds=1, metric_samples=2,
                               js_heap_max_sampled_bytes=1000, api_requests=1, api_encoded_bytes=100,
                               page_encoded_bytes=200, first_api_data_ms=300)
        base["errors"] = ["pageerror: TypeError: Cannot read properties of undefined"]
        self.assertIn("Grafana JavaScript page error", " ".join(compare.runtime_problems(base, base, required=True)))
        base["errors"] = ["Error: pipeline error: 'JSONParserErr'"]
        self.assertEqual(compare.runtime_problems(base, base, required=True), [])

    def test_failed_comparison_cannot_leave_a_stale_success_report(self):
        with tempfile.TemporaryDirectory() as out:
            d = os.path.join(out, "data", "page-a", "1h")
            os.makedirs(d)
            with open(os.path.join(d, "main.json"), "w") as f:
                f.write("[]")  # a malformed top-level capture crashes before a fresh report is written
            with open(os.path.join(d, "pr.json"), "w") as f:
                json.dump(capture([1]), f)
            stale = os.path.join(out, "compare.json")
            with open(stale, "w") as f:
                json.dump([{"page": "page-a", "range": "1h", "main_pr_diffs": []}], f)
            result = subprocess.run([sys.executable, os.path.join(HERE, "..", "compare.py"), out], capture_output=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertFalse(os.path.exists(stale))

    def run_compare(self, main, pr, loki=None, rng="1h", runtime_required=False, fix_cases=None, first_pass_loki=None):
        with tempfile.TemporaryDirectory() as out:
            d = os.path.join(out, "data", "page-a", rng)
            os.makedirs(d)
            if runtime_required:
                with open(os.path.join(out, "plan.json"), "w", encoding="utf-8") as f:
                    json.dump({"runtime_proof": {"required": True}, "fix_cases": fix_cases or {},
                               "entries": {"page-a": {"ranges": [rng], "runtime_required": True}}}, f)
            for name, cap in (("main", main), ("pr", pr), ("loki", loki)):
                if cap is not None:
                    with open(os.path.join(d, f"{name}.json"), "w", encoding="utf-8") as f:
                        json.dump(cap, f)
            if first_pass_loki is not None:  # Loki's capture of the first pass of the run, kept next to a recapture
                first = os.path.join(out, "first-pass", "data", "page-a", rng)
                os.makedirs(first)
                with open(os.path.join(first, "loki.json"), "w", encoding="utf-8") as f:
                    json.dump(first_pass_loki, f)
            proc = subprocess.run([sys.executable, os.path.join(HERE, "..", "compare.py"), out], capture_output=True, text=True)
            with open(os.path.join(out, "compare.md"), encoding="utf-8") as f:
                self.last_md = f.read()
            with open(os.path.join(out, "compare.json"), encoding="utf-8") as f:
                return proc.returncode, json.load(f)[0]

    def test_required_catalogued_rejections_and_successful_correction(self):
        runtime = dict(browser_thread_seconds=1, browser_task_seconds=1, metric_samples=2,
                       js_heap_max_sampled_bytes=1000, api_requests=1, api_encoded_bytes=100,
                       page_encoded_bytes=200, first_api_data_ms=25)
        detail = ("maximum number of series (500) reached for a single query; consider reducing query cardinality by "
                  "adding more specific stream selectors, reducing the time range, or aggregating results with "
                  "functions like sum(), count() or topk()")
        error = dict(capture([], status=400, error=detail), runtime=runtime)
        self.assertEqual(self.run_compare(error, error, error, runtime_required=True)[0], 0)
        for values in ([1], [], [0, 0]):
            answer = dict(capture(values), runtime=dict(runtime, browser_thread_seconds=5))
            _, row = self.run_compare(error, answer, answer, runtime_required=True)
            self.assertEqual(row["runtime_problems"], [])
            self.assertIn("unavailable", row["base_comparison_note"])
            self.assertEqual(row["corrected_base_rejections"][0]["errors"], [[400, 400, detail]])
            import visual_comment
            self.assertEqual(visual_comment.assess(row, None)[::2], ([], "improved"))
            case = "semantics/corrected-query"
            plan = {"entries": {"page-a": {"ranges": ["1h"], "core": False, "why": [case]}},
                    "fix_cases": {case: {"entries": ["page-a"]}}}
            items, verdict = visual_comment.evaluate([row], {}, plan)
            self.assertEqual((verdict["exit"], verdict["failures"]), (0, []))
            self.assertEqual(visual_comment.fix_proofs(items, plan)[0][3], "fixed")
            if not values:
                without_proof = dict(row, corrected_base_rejections=[])
                self.assertNotEqual(visual_comment.evaluate([without_proof], {}, plan)[1]["exit"], 0)
        answer["runtime"] = dict(runtime, metric_samples=0)
        self.assertTrue(self.run_compare(error, answer, answer, runtime_required=True)[1]["runtime_problems"])

    def test_required_resource_keys_and_error_envelopes(self):
        def resource(path, body):
            cap = capture([])
            cap["records"][0].update(url=path, response=body)
            cap["runtime"] = dict(browser_thread_seconds=1, browser_task_seconds=1, metric_samples=2,
                                  js_heap_max_sampled_bytes=1000, api_requests=1,
                                  api_encoded_bytes=100, page_encoded_bytes=200)
            return cap
        for route in ("/api/datasources/proxy/uid/{}/loki/api/v1/labels",
                      "/api/datasources/proxy/{}/loki/api/v1/labels", "/api/datasources/uid/{}/resources/labels"):
            caps = [resource(route.format(uid), {"data": []}) for uid in ("1", "2", "3")]
            code, row = self.run_compare(*caps, runtime_required=True)
            self.assertEqual((code, row["runtime_problems"]), (0, []))
        malformed = resource("/api/datasources/uid/x/resources/labels", {"status": "error", "data": []})
        self.assertTrue(self.run_compare(malformed, malformed, malformed, runtime_required=True)[1]["runtime_problems"])

    def test_required_runtime_proof_rejects_screenshots_without_measurements(self):
        problems = compare.runtime_problems(capture([1]), capture([1]), required=True)
        self.assertTrue(problems)
        self.assertIn("runtime", " ".join(problems))

    def test_required_runtime_is_enforced_by_existing_comparison_cli(self):
        code, row = self.run_compare(capture([1]), capture([1]), capture([1]), runtime_required=True)
        self.assertEqual(code, 1)
        self.assertTrue(row["runtime_problems"])
        self.assertEqual(row["main_pr_diffs"], [])

    def test_runtime_gate_distinguishes_noise_and_material_regressions(self):
        from copy import deepcopy
        base = capture([1])
        base["runtime"] = {"browser_thread_seconds": 1.0, "browser_task_seconds": 1.2,
                           "js_heap_max_sampled_bytes": 100 << 20, "metric_samples": 3, "first_api_data_ms": 300,
                           "api_requests": 2, "api_encoded_bytes": 1 << 20, "page_encoded_bytes": 2 << 20}
        pr = deepcopy(base)
        self.assertEqual(compare.runtime_problems(base, pr, required=True), [])
        pr["runtime"]["browser_thread_seconds"] = 1.1
        self.assertEqual(compare.runtime_problems(base, pr, required=True), [])
        pr["runtime"]["js_heap_max_sampled_bytes"] = 180 << 20
        self.assertIn("heap", " ".join(compare.runtime_problems(base, pr, required=True)))
        del pr["settle_ms"]
        self.assertIn("settle_ms", " ".join(compare.runtime_problems(base, pr, required=True)))

    def test_missing_runtime_is_not_measured_as_zero(self):
        base = capture([1])
        pr = capture([1])
        self.assertEqual(compare.runtime_problems(base, pr, required=False), [])
        base["runtime"] = {"browser_thread_seconds": float("nan")}
        self.assertTrue(compare.runtime_problems(base, pr, required=True))

    def test_required_loki_reference_must_be_readable_and_settled(self):
        from copy import deepcopy
        base = capture([1])
        base["runtime"] = {"browser_thread_seconds": 1.0, "browser_task_seconds": 1.2,
                           "js_heap_max_sampled_bytes": 100 << 20, "metric_samples": 3, "first_api_data_ms": 300,
                           "api_requests": 2, "api_encoded_bytes": 1 << 20, "page_encoded_bytes": 2 << 20}
        loki = deepcopy(base)
        code, _ = self.run_compare(base, base, loki, runtime_required=True)
        self.assertEqual(code, 0)
        loki["settled"] = False
        code, row = self.run_compare(base, base, loki, runtime_required=True)
        self.assertEqual(code, 1)
        self.assertIn("Loki reference is incomplete", " ".join(row["runtime_problems"]))
        loki["settled"] = True
        loki["records"][0]["response"] = None
        self.assertEqual(self.run_compare(base, base, loki, runtime_required=True)[0], 1)

    def test_a_seeded_log_line_that_names_a_script_error_is_not_a_ui_crash(self):
        # The log generator seeds "TypeError: Cannot read property"; a page that lists it shows it on Loki's
        # side as much as on the proxy's. Chrome's message for a crashed panel is "Cannot read properties of".
        runtime = dict(browser_thread_seconds=1.0, browser_task_seconds=1.2, js_heap_max_sampled_bytes=100 << 20,
                       metric_samples=3, first_api_data_ms=300, api_requests=2, api_encoded_bytes=1 << 20,
                       page_encoded_bytes=2 << 20)

        def page(banner):
            return dict(capture([1], ui={"noData": 0, "banners": [banner], "panelErrors": 0}), runtime=runtime)

        log_line = 'Cannot read property", "level": "error"} '
        self.assertEqual(compare.runtime_problems(page(log_line), page(log_line), required=True), [])
        crash = "Cannot read properties of undefined (reading 'defaults')"
        problems = compare.runtime_problems(page(crash), page(crash), required=True)
        self.assertEqual(sorted(problems), ["runtime proof: PR Grafana UI infrastructure error",
                                            "runtime proof: main Grafana UI infrastructure error"])

    def test_required_loki_reference_failure_is_not_a_successful_comparison(self):
        runtime = {"browser_thread_seconds": 1.0, "browser_task_seconds": 1.2,
                   "js_heap_max_sampled_bytes": 100 << 20, "metric_samples": 3, "first_api_data_ms": 300,
                   "api_requests": 2, "api_encoded_bytes": 1 << 20, "page_encoded_bytes": 2 << 20}
        base, loki = capture([1]), capture([], error="Service unavailable")
        base["runtime"] = loki["runtime"] = runtime
        loki["records"][0]["status"] = 503
        code, row = self.run_compare(base, base, loki, runtime_required=True)
        self.assertEqual(code, 1)
        self.assertIn("infrastructure error", " ".join(row["runtime_problems"]))
        for body in ("upstream unavailable", ["not a query result"]):
            loki["records"][0]["response"] = body
            self.assertEqual(self.run_compare(base, base, loki, runtime_required=True)[0], 1)

    def test_required_answers_distinguish_empty_errors_and_missing_data(self):
        from copy import deepcopy
        base = capture([])
        base["runtime"] = dict(browser_thread_seconds=1, browser_task_seconds=1, metric_samples=2,
                               js_heap_max_sampled_bytes=1000, api_requests=1, api_encoded_bytes=100, page_encoded_bytes=200)
        def check(a, b, c, code):
            result, row = self.run_compare(a, b, c, runtime_required=True)
            self.assertEqual(result, code, row["runtime_problems"])
        check(base, base, base, 0)
        check(base, base, dict(base, records=[]), 1)
        failed_ui = dict(base, ui={"banners": ["Plugin failed to load"]})
        check(failed_ui, failed_ui, failed_ui, 1)
        check(dict(base, settled=False), dict(base, settled=False), base, 1)
        err = deepcopy(base)
        err["records"][0].update(status=500, response={"messageId": "plugin.unavailable"})
        check(err, err, err, 1)
        err["records"][0].update(status=400, response={"results": {"A": {"error": "parse error: unexpected token"}}})
        check(err, err, err, 0)
        different = deepcopy(err)
        different["records"][0]["response"]["results"]["A"]["error"] = "parse error: different token"
        check(err, err, different, 1)
        different = deepcopy(err)
        different["records"][0]["request"]["queries"][0]["expr"] = "different()"
        check(err, err, different, 1)
        for response in ({"data": ["a"]}, {"fields": [{"label": "service"}]}, {"data": {"result": [1]}}):
            resource = deepcopy(base)
            resource["records"][0].update(url="/api/datasources/uid/x/resources/labels", response=response)
            check(resource, resource, resource, 1)
            resource["runtime"]["first_api_data_ms"] = 100
            check(resource, resource, resource, 0)

    def test_show_context_queries_pair_up_despite_grafanas_random_ref_ids(self):
        def ctx(ref):
            cap = capture([1, 2])
            rec = cap["records"][0]
            rec["request"]["queries"][0].update(refId=ref, direction="forward")
            rec["response"]["results"] = {ref: rec["response"]["results"]["A"]}
            return cap
        code, row = self.run_compare(ctx("log-row-context-query-_0.43741"), ctx("log-row-context-query-_0.95931"),
                                     ctx("log-row-context-query-_0.11"))
        self.assertEqual((code, row["main_pr_diffs"], row["loki_diffs"]), (0, [], []))
        self.assertEqual(compare.stable_ref("log-row-context-query-_0.4374129728512264"), "log-row-context-query")
        self.assertEqual(compare.stable_ref("A"), "A")
    def test_an_error_loki_answers_too_is_lokis_answer_beyond_its_history(self):
        err = "pipeline error: 'SampleExtractionErr' for series: '{v=\"x\"}'."
        # 6h is beyond the history Loki holds on the CI stack: the data is not compared, the error still is.
        _, row = self.run_compare(capture([1, 2]), capture([1], error=err), capture([1], error=err), rng="6h")
        self.assertFalse(row["loki_compared"])
        self.assertEqual(row["errors_pr"], [])
        self.assertEqual(len(row["errors_loki"]), 1)
        # An error Loki does not answer stays an error of the PR.
        _, row = self.run_compare(capture([1, 2]), capture([1], error=err), capture([1, 2]), rng="6h")
        self.assertEqual(len(row["errors_pr"]), 1)

    def test_identical_counts_points_and_exits_zero(self):
        code, row = self.run_compare(capture([1, 2, 0]), capture([1, 2, 0]), capture([1, 2, 0]))
        self.assertEqual(code, 0)
        self.assertEqual((row["points_main"], row["points_pr"]), (2, 2))  # zero points are not data
        self.assertEqual(row["main_pr_diffs"], [])
        self.assertEqual(row["loki_diffs"], [])

    def test_unreadable_success_body_is_capture_gap_not_empty_result(self):
        base = capture([1, 2])
        base["records"][0]["response"] = None  # response headers arrived, but Playwright could not read the body
        code, row = self.run_compare(base, capture([1, 2]), capture([1, 2]))
        self.assertEqual(code, 0)
        self.assertEqual(row["main_pr_diffs"], [])
        self.assertTrue(any("capture body unreadable" in d for d in row["main_pr_nondet"]))
        self.assertFalse(row["settled"])
        self.assertTrue(row["settled_pr"])

        pr = capture([1, 2])
        pr["records"][0]["response"] = None
        _, row = self.run_compare(capture([1, 2]), pr, capture([1, 2]))
        self.assertFalse(row["settled_pr"])  # the verdict must still reject missing PR evidence

    def two_requests(self, readable_first=True, readable_second=True, settled=True, reason=None):
        """A page with two big requests (as dd-fields 6h: sum by (span_id) and sum by (trace_id)) and one small one."""
        cap = capture([1, 2], settled=settled)
        for i, ok in enumerate((readable_first, readable_second)):
            rec = json.loads(json.dumps(cap["records"][0]))
            rec["request"]["queries"][0].update(refId=f"B{i}", expr=f"sum by (id{i}) (count_over_time(x[1m]))")
            if not ok:
                rec["response"] = None
                if reason:
                    rec["body_unavailable"] = reason
                    rec["content_length"] = 52_000_000
            cap["records"].append(rec)
        return cap

    def test_a_body_unreadable_on_both_sides_is_not_blamed_on_the_pr(self):
        # The row of dd-fields 6h in runs 41658646164 and 41696033829: two 6h high-cardinality requests answer 200 with
        # no body the browser can read, on the base and on the PR alike (equal points, equal settle times).
        both = lambda: self.two_requests(False, False, reason="No resource with given identifier found")
        code, row = self.run_compare(both(), both(), both(), rng="6h")
        self.assertEqual(code, 0)
        self.assertFalse(row["settled"])  # the capture is incomplete, and the table says so
        self.assertTrue(row["settled_pr"])  # but the PR is not blamed for it
        self.assertEqual((row["unreadable_main"], row["unreadable_pr"], row["unreadable_both"]), (2, 2, 2))
        self.assertEqual(row["unreadable_why"], ["No resource with given identifier found"])
        self.assertEqual(row["points_main"], row["points_pr"])
        self.assertEqual(row["main_pr_diffs"], [])

    def test_a_body_only_the_pr_lost_still_fails_the_pr(self):
        code, row = self.run_compare(self.two_requests(True, False), self.two_requests(False, False), self.two_requests(), rng="6h")
        self.assertEqual((row["unreadable_main"], row["unreadable_pr"], row["unreadable_both"]), (1, 2, 1))
        self.assertFalse(row["settled_pr"])  # one request the base read and the PR did not
        _, row = self.run_compare(self.two_requests(), self.two_requests(True, False), self.two_requests(), rng="6h")
        self.assertFalse(row["settled_pr"])

    def test_a_pr_that_never_settled_is_not_excused_by_unreadable_bodies(self):
        both = lambda settled: self.two_requests(False, False, settled=settled)
        _, row = self.run_compare(both(True), both(False), both(True), rng="6h")
        self.assertFalse(row["settled_pr"])

    @staticmethod
    def cancelled(cap):
        """The request as capture.spec.ts records one the page cancelled (net::ERR_ABORTED): no status, no body."""
        rec = cap["records"][0]
        rec.update(status=0, response=None, aborted="net::ERR_ABORTED")
        return cap

    def test_a_request_the_page_cancelled_on_one_side_is_reported_not_gated(self):
        # The volume panel's query of the Drilldown level-pill pages (runs 41724504757, 41721299647): the page cancels
        # it on any datasource, Loki direct included; whether the answer or the cancel comes first is a race of the page.
        for main, pr in ((capture([1, 2]), self.cancelled(capture([1, 2]))), (self.cancelled(capture([1, 2])), capture([1, 2]))):
            code, row = self.run_compare(main, pr, capture([1, 2]))
            self.assertEqual((code, row["main_pr_diffs"]), (0, []))
            self.assertTrue(any("request cancelled by the page" in d for d in row["main_pr_nondet"]), row["main_pr_nondet"])
            self.assertTrue(row["settled"] and row["settled_pr"])  # a cancel is not an unfinished page nor an unreadable body
            self.assertEqual((row["unreadable_main"], row["unreadable_pr"]), (0, 0))

    def test_a_request_the_page_cancelled_on_both_sides_is_equal(self):
        code, row = self.run_compare(self.cancelled(capture([1, 2])), self.cancelled(capture([1, 2])), self.cancelled(capture([1, 2])))
        self.assertEqual((code, row["main_pr_diffs"], row["main_pr_nondet"], row["loki_diffs"], row["loki_nondet"]), (0, [], [], [], []))
        self.assertEqual(row["main_vs_pr"], "1/1")

    def test_a_cancelled_request_the_base_never_made_is_still_a_difference(self):
        base = capture([1, 2])
        base["records"] = []
        code, row = self.run_compare(base, self.cancelled(capture([1, 2])), capture([1, 2]))
        self.assertEqual(code, 1)
        self.assertTrue(any("request only on one side" in d for d in row["main_pr_diffs"]))

    def test_a_request_cancelled_against_loki_is_history_dependent_not_a_difference(self):
        _, row = self.run_compare(capture([1, 2]), capture([1, 2]), self.cancelled(capture([1, 2])))
        self.assertEqual(row["loki_diffs"], [])
        self.assertTrue(any("request cancelled by the page" in d for d in row["loki_nondet"]), row["loki_nondet"])

    def test_a_value_difference_is_not_hidden_by_a_cancelled_request(self):
        def page(first, second, cancel_first=False):
            cap = capture(first)
            other = capture(second)
            other["records"][0]["request"]["queries"][0]["expr"] = "sum(y)"
            cap["records"].append(other["records"][0])
            return self.cancelled(cap) if cancel_first else cap
        code, row = self.run_compare(page([1, 2], [5, 6]), page([1, 2], [5, 7], cancel_first=True), page([1, 2], [5, 6]))
        self.assertEqual(code, 1)
        self.assertTrue(any("sum(y)" in d for d in row["main_pr_diffs"]), row["main_pr_diffs"])
        self.assertTrue(any("request cancelled by the page" in d for d in row["main_pr_nondet"]))

    def test_value_difference_is_reported_and_exits_one(self):
        code, row = self.run_compare(capture([1, 2]), capture([1, 3]), capture([1, 2]))
        self.assertEqual(code, 1)
        self.assertTrue(row["main_pr_diffs"])
        self.assertEqual(len(row["loki_new"]), 1)  # PR differs from Loki where the base does not

    def test_differences_carry_the_query_they_are_about(self):
        # the window and ids vary by range; the expression and query type do not
        _, row = self.run_compare(capture([1, 2]), capture([1, 3]), capture([1, 2]))
        self.assertEqual(row["main_pr_qids"], ["sum(x)|range"])
        self.assertEqual(row["loki_qids"], ["sum(x)|range"])
        _, row = self.run_compare(capture([1, 2]), capture([1, 2]), capture([1, 2]))
        self.assertEqual((row["main_pr_qids"], row["loki_qids"]), ([], []))

    def test_a_resource_difference_has_no_query(self):
        _, row = self.run_compare(resources(("index/stats?q=1", {"streams": 1})), resources(("index/stats?q=1", {"streams": 2})))
        self.assertEqual((len(row["main_pr_diffs"]), row["main_pr_qids"]), (1, [None]))

    def test_query_statistics_of_a_resource_answer_are_not_its_data(self):
        def volume(n, ms):
            return ("index/volume?q=1", {"status": "success", "data": {"resultType": "vector", "result": [{"metric": {"app": "a"}, "value": [1, str(n)]}],
                                                                        "stats": {"summary": {"execTime": ms}}}})
        _, row = self.run_compare(resources(volume(5, 0.012)), resources(volume(5, 0.034)))
        self.assertEqual(row["main_pr_diffs"], [])
        _, row = self.run_compare(resources(volume(5, 0.012)), resources(volume(6, 0.012)))
        self.assertEqual(len(row["main_pr_diffs"]), 1)

    def test_loki_difference_the_base_has_is_not_new(self):
        _, row = self.run_compare(capture([1, 2]), capture([1, 2]), capture([5, 5]))
        self.assertEqual(len(row["loki_diffs"]), 1)
        self.assertEqual(row["loki_main_n"], 1)
        self.assertEqual(row["loki_new"], [])

    def test_ui_state_is_carried_per_side(self):
        bad = {"noData": 1, "banners": ["Plugin unavailable"], "panelErrors": 1}
        _, row = self.run_compare(capture([1]), capture([1], ui=bad))
        self.assertEqual(row["ui_pr"], bad)
        self.assertEqual(row["ui_main"]["noData"], 0)

    def test_datasource_uid_and_request_id_do_not_split_requests(self):
        _, row = self.run_compare(capture([1, 2], uid="vp-main"), capture([1, 2], uid="vp-pr"))
        self.assertEqual(row["main_pr_diffs"], [])
        self.assertEqual(row["main_vs_pr"], "1/1")

    def test_other_request_fields_make_a_different_request(self):
        a, b = capture([1, 2]), capture([1, 2])
        b["records"][0]["request"]["queries"][0]["maxLines"] = 5000
        _, row = self.run_compare(a, b)
        self.assertEqual(len(row["main_pr_diffs"]), 2)  # one request on each side only

    def test_frame_schema_difference_is_a_difference(self):
        other = [{"type": "time"}, {"type": "number", "labels": {"level": "info"}, "config": {"interval": 60000}}]
        _, row = self.run_compare(capture([1, 2]), capture([1, 2], fields=other))
        self.assertIn("schema", row["main_pr_diffs"][0])

    def test_duplicate_answers_pair_by_content_not_arrival_order(self):
        a, b = capture([1, 2]), capture([3, 4])
        a["records"] = a["records"] + b["records"]
        b2, a2 = capture([3, 4]), capture([1, 2])
        b2["records"] = b2["records"] + a2["records"]  # same two answers, other order
        _, row = self.run_compare(a, b2)
        self.assertEqual(row["main_pr_diffs"], [])

    def test_errors_are_listed_per_side_minus_the_allow_list(self):
        bad = capture([1], error="plugin unavailable")
        _, row = self.run_compare(bad, bad)
        self.assertEqual(row["errors_pr"], ["plugin unavailable"])
        self.assertEqual(row["main_pr_diffs"], [])  # equal on both sides, and still reported
        self.assertEqual(compare.errors({"k": [{("A", "error"): ("error", "plugin unavailable")}]}, ["unavailable"]), [])
        _, row = self.run_compare(capture([1]), capture([1], status=500))
        self.assertTrue(row["errors_pr"])

    def test_an_error_the_pr_answers_as_loki_does_is_lokis_answer(self):
        # Loki's pipeline error names the first failing line its shards meet: the series is set aside.
        def err(series):
            return ("pipeline error: 'SampleExtractionErr' for series: '{__error__=\"SampleExtractionErr\", v=\"" + series +
                    "\"}'.\nUse a label filter to intentionally skip this error.")
        code, row = self.run_compare(capture([1, 2]), capture([1], error=err("x7")), capture([1], error=err("x3")))
        self.assertEqual(code, 1)  # base and PR differ; the gate decides against Loki
        self.assertEqual(row["errors_pr"], [])
        self.assertEqual(len(row["errors_loki"]), 1)
        self.assertEqual(row["loki_diffs"], [])
        self.assertEqual(row["loki_main_n"], 1)
        # An error Loki does not answer stays an error of the PR.
        _, row = self.run_compare(capture([1, 2]), capture([1], error=err("x7")), capture([1, 2]))
        self.assertEqual(len(row["errors_pr"]), 1)
        self.assertEqual(compare.error_text(err("a")), compare.error_text(err("b")))
        self.assertNotEqual(compare.error_text(err("a")), compare.error_text("pipeline error: 'JSONParserErr' for series: '{}'."))

    def test_missing_loki_capture_within_its_window_is_flagged(self):
        _, row = self.run_compare(capture([1]), capture([1]))
        self.assertTrue(row["loki_missing"])
        _, row = self.run_compare(capture([1]), capture([1]), capture([1]))
        self.assertFalse(row["loki_missing"])

    def test_points(self):
        self.assertEqual(compare.points({"k": [{"a": ("metric", {1: 0, 2: 5}, "sig"), "b": ("other", "h", 7), "c": ("error", "x")}]}), 8)


def resources(*answers, uid="vp-pr"):
    """A capture of datasource resource answers: (path, body) pairs."""
    recs = [{"url": f"/api/datasources/uid/{uid}/resources/{path}", "method": "GET", "status": 200, "request": None, "response": body}
            for path, body in answers]
    return {"settled": True, "settle_ms": 1000, "records": recs, "ui": {"noData": 0, "banners": [], "panelErrors": 0}}


def log_capture(labels, types, uid="ds1"):
    names = ["labels", "Time", "Line", "tsNs", "labelTypes", "id"]
    frame = {"schema": {"name": "", "fields": [{"name": n, "type": "other" if n in ("labels", "labelTypes") else "string"} for n in names]},
             "data": {"values": [[labels], [1], ["line"], ["1"], [types], [f"1_{hash(json.dumps(labels, sort_keys=True)) & 0xffff:x}"]]}}
    rec = {"url": "/api/ds/query?ds_type=loki", "method": "POST", "status": 200,
           "request": {"from": "1", "to": "2", "queries": [{"refId": "A", "expr": "{app=\"x\"} | json", "queryType": "range",
                                                           "datasource": {"type": "loki", "uid": uid}}]},
           "response": {"results": {"A": {"frames": [frame]}}}}
    return {"settled": True, "settle_ms": 1000, "records": [rec], "ui": {"noData": 0, "banners": [], "panelErrors": 0}}


class ExplainedVsLokiTest(unittest.TestCase):
    """Differences from Loki by design are listed as explained, everything else stays a difference."""
    run_compare = CompareTest.run_compare

    def check(self, pr, loki, first_pass_loki=None):
        _, row = self.run_compare(pr, pr, loki, first_pass_loki=first_pass_loki)
        return row

    def test_detected_fields_envelope_and_extracted_suffix_are_explained(self):
        field = {"label": "pipeline", "type": "string", "cardinality": 3, "parsers": ["json"], "jsonPath": ["pipeline"]}
        meta = {"label": "trace_id", "type": "string", "cardinality": 9, "parsers": None}
        pr = resources(("detected_fields?q=1", {"status": "success", "data": [field, meta], "fields": [field, meta], "limit": 1000}))
        loki = resources(("detected_fields?q=1", {"fields": [meta, field, {"label": "level_extracted", "type": "string", "cardinality": 2,
                                                                       "parsers": ["json"], "jsonPath": ["level"]}], "limit": 1000}), uid="vp-loki")
        row = self.check(pr, loki)
        self.assertEqual(row["loki_diffs"], [])
        self.assertEqual(len(row["loki_explained"]), 1)
        self.assertIn("_extracted", row["loki_explained"][0])
        # structured metadata listed as a JSON key is a real difference
        bad = dict(meta, parsers=["json"], jsonPath=["trace_id"])
        row = self.check(resources(("detected_fields?q=1", {"fields": [field, bad], "limit": 1000})),
                         resources(("detected_fields?q=1", {"fields": [field, meta], "limit": 1000}), uid="vp-loki"))
        self.assertEqual(len(row["loki_diffs"]), 1)
        self.assertEqual(row["loki_explained"], [])

    def test_base_that_needs_an_explanation_the_pr_does_not_is_not_a_loki_match(self):
        # The base lacks Loki's level_extracted (explained by design), the PR returns it: improved, not unsettled.
        import visual_comment
        stream, types = {"app": "x", "level": "info"}, {"app": "I", "level": "I"}
        loki = log_capture(dict(stream, level_extracted="debug"), dict(types, level_extracted="P"), uid="vp-loki")
        pr = log_capture(dict(stream, level_extracted="debug"), dict(types, level_extracted="P"))
        _, row = self.run_compare(log_capture(stream, types), pr, loki)
        self.assertEqual(row["loki_diffs"], [])
        self.assertEqual(row["loki_main_n"], 1)
        self.assertEqual(visual_comment.classify(row), "improved")
        # a parse error both lack plus the suffix only the base lacks: the base needs one more reason
        loki_err = log_capture(dict(stream, level_extracted="debug", __error__="JSONParserErr"),
                               dict(types, level_extracted="P", __error__="P"), uid="vp-loki")
        _, row = self.run_compare(log_capture(stream, types), pr, loki_err)
        self.assertEqual(row["loki_diffs"], [])
        self.assertEqual(visual_comment.classify(row), "improved")
        # both builds needing the same explanation still both match Loki
        _, row = self.run_compare(log_capture(stream, types), log_capture(stream, types), loki)
        self.assertEqual(row["loki_main_n"], 0)

    def test_extracted_label_the_pr_returns_is_compared_not_set_aside(self):
        # Loki adds level_extracted and a parse error the proxy does not report; a PR that returns level_extracted
        # with Loki's value is explained only by the parse-error rule, a different value stays a difference.
        stream = {"app": "x", "level": "info"}
        loki = log_capture(dict(stream, level_extracted="debug", __error__="JSONParserErr"),
                           {"app": "I", "level": "I", "level_extracted": "P", "__error__": "P"}, uid="vp-loki")
        row = self.check(log_capture(dict(stream, level_extracted="debug"), {"app": "I", "level": "I", "level_extracted": "P"}), loki)
        self.assertEqual(row["loki_diffs"], [])
        self.assertNotIn("_extracted suffix", row["loki_explained"][0])
        row = self.check(log_capture(dict(stream, level_extracted="warn"), {"app": "I", "level": "I", "level_extracted": "P"}), loki)
        self.assertEqual(len(row["loki_diffs"]), 1)
        # a base without the suffix is still explained by it
        row = self.check(log_capture(stream, {"app": "I", "level": "I"}), loki)
        self.assertEqual(row["loki_diffs"], [])
        self.assertIn("_extracted suffix", row["loki_explained"][0])

    def test_detected_labels_cardinality_explained_but_not_an_empty_loki(self):
        pr = resources(("detected_labels?q=1", {"detectedLabels": [{"label": "pod", "cardinality": 45}, {"label": "level", "cardinality": 2}]}))
        loki = resources(("detected_labels?q=1", {"detectedLabels": [{"label": "level", "cardinality": 2}, {"label": "pod", "cardinality": 67}]}), uid="vp-loki")
        row = self.check(pr, loki)
        self.assertEqual(row["loki_diffs"], [])
        self.assertIn("sampled cardinality", row["loki_explained"][0])
        row = self.check(pr, resources(("detected_labels?q=1", {}), uid="vp-loki"))
        self.assertEqual(len(row["loki_diffs"]), 1)

    def test_index_stats_bytes_explained_entries_not(self):
        loki = resources(("index/stats?q=1", {"streams": 13513, "chunks": 13513, "bytes": 62091264, "entries": 289279}), uid="vp-loki")
        row = self.check(resources(("index/stats?q=1", {"streams": 13513, "chunks": 13513, "bytes": 28927900, "entries": 289279})), loki)
        self.assertEqual((row["loki_diffs"], len(row["loki_explained"])), ([], 1))
        row = self.check(resources(("index/stats?q=1", {"streams": 1, "chunks": 1, "bytes": 39508700, "entries": 395087})), loki)
        self.assertEqual(len(row["loki_diffs"]), 1)

    def test_index_stats_of_a_recapture_follows_the_same_rules_as_the_first_pass(self):
        # ci_run recaptures a page and runs this same compare again. Loki's index/stats is an estimate from chunk
        # metadata: while the live generator writes into the stream, bytes and chunks move (explained) and so can
        # entries (a difference), which is why the recapture is taken before the Live tail pass.
        pr = resources(("index/stats?q=1", {"streams": 13513, "chunks": 13513, "bytes": 28927900, "entries": 289279}))
        first = resources(("index/stats?q=1", {"streams": 13513, "chunks": 13513, "bytes": 62091264, "entries": 289279}), uid="vp-loki")
        recapture = resources(("index/stats?q=1", {"streams": 13513, "chunks": 13590, "bytes": 62917300, "entries": 289279}), uid="vp-loki")
        moved = resources(("index/stats?q=1", {"streams": 13513, "chunks": 13590, "bytes": 62917300, "entries": 289954}), uid="vp-loki")
        for loki, n_diffs in ((first, 0), (recapture, 0), (moved, 1)):
            row = self.check(pr, loki)
            self.assertEqual((len(row["loki_diffs"]), len(row["loki_explained"])), (n_diffs, 1 - n_diffs))

    def test_index_stats_bodies_name_both_sides_numbers(self):
        # Loki's results cache answered a 15m window with the 1h window's stream count (index_stats_cache.go Extract):
        # the difference stays counted, and the artifact keeps both answers so the run shows which numbers moved.
        pr = resources(("index/stats?q=1", {"streams": 3365, "chunks": 3365, "bytes": 7230800, "entries": 72308}))
        loki = resources(("index/stats?q=1", {"streams": 13501, "chunks": 13501, "bytes": 15496192, "entries": 72315}), uid="vp-loki")
        row = self.check(pr, loki)
        self.assertEqual((len(row["loki_diffs"]), row["loki_explained"]), (1, []))
        self.assertEqual(len(row["loki_bodies"]), 1)
        kept = row["loki_bodies"][0]
        self.assertIn("/resources/index/stats?q=1", kept["request"])
        self.assertEqual(json.loads(kept["left"])["streams"], 3365)
        self.assertEqual(json.loads(kept["right"])["streams"], 13501)
        numbers = ("streams 3365, chunks 3365, entries 72308, bytes 7230800 vs "
                   "streams 13501, chunks 13501, entries 72315, bytes 15496192")
        self.assertEqual(kept["numbers"], numbers)
        self.assertIn(f"- PR vs Loki index/stats: {numbers}", self.last_md)
        # an explained difference (bytes and chunks only) keeps its answers too
        row = self.check(pr, resources(("index/stats?q=1", {"streams": 3365, "chunks": 3365, "bytes": 15496192, "entries": 72308}), uid="vp-loki"))
        self.assertEqual((row["loki_diffs"], len(row["loki_explained"]), len(row["loki_bodies"])), ([], 1, 1))
        # main vs PR keeps the bodies of a differing resource; identical answers keep none
        _, row = self.run_compare(resources(("index/stats?q=1", {"streams": 2, "chunks": 2, "bytes": 200, "entries": 2})), pr)
        self.assertEqual(row["main_pr_bodies"][0]["numbers"].split(" vs ")[0], "streams 2, chunks 2, entries 2, bytes 200")
        _, row = self.run_compare(pr, pr, pr)
        self.assertEqual((row["main_pr_bodies"], row["loki_bodies"]), ([], []))

    def test_index_stats_explains_whole_chunk_copies_only_when_loki_itself_moved(self):
        # registry quality/index-stats-loki-chunk-accounting: every copy of a chunk Loki reads is added. Equal streams,
        # and entries and chunks both an exact whole multiple (k >= 2) of the proxy's, is that signature, but it also fits a
        # proxy that undercounts by 1/k. It is explained only when Loki's own earlier answer in the run (the first pass,
        # kept next to the recapture) equalled the proxy's current streams, entries and chunks: Loki moved, the proxy did not.
        pr = resources(("index/stats?q=1", {"streams": 490, "chunks": 490, "bytes": 1221200, "entries": 12212}))

        def loki(**numbers):
            body = {"streams": 490, "chunks": 980, "bytes": 8386560, "entries": 24424}
            body.update(numbers)
            return resources(("index/stats?q=1", body), uid="vp-loki")
        first = resources(("index/stats?q=1", {"streams": 490, "chunks": 490, "bytes": 4193280, "entries": 12212}), uid="vp-loki")
        row = self.check(pr, loki(), first_pass_loki=first)
        self.assertEqual((row["loki_diffs"], len(row["loki_explained"])), ([], 1))
        self.assertIn("every chunk 2 times", row["loki_explained"][0])
        self.assertIn("earlier in this run Loki answered this request with the proxy's", row["loki_explained"][0])
        row = self.check(pr, loki(chunks=1470, entries=36636), first_pass_loki=first)
        self.assertEqual((row["loki_diffs"], len(row["loki_explained"])), ([], 1))
        self.assertIn("every chunk 3 times", row["loki_explained"][0])
        # no evidence that only Loki moved: no first pass, or a first pass that did not equal the proxy's numbers
        self.assertEqual(len(self.check(pr, loki())["loki_diffs"]), 1)
        undercount = resources(("index/stats?q=1", {"streams": 490, "chunks": 980, "bytes": 8386560, "entries": 24424}), uid="vp-loki")
        self.assertEqual(len(self.check(pr, loki(), first_pass_loki=undercount)["loki_diffs"]), 1)
        self.assertEqual(len(self.check(pr, loki(), first_pass_loki=resources(("index/stats?q=2", {"streams": 490, "chunks": 490, "entries": 12212}),
                                                                           uid="vp-loki"))["loki_diffs"]), 1)
        # the signature itself is strict, even with a first pass that equals the proxy's numbers
        for name, numbers in (
                ("entries doubled, chunks not", dict(chunks=490)),
                ("entries not a multiple", dict(entries=24425)),
                ("entries and chunks differ in multiple", dict(chunks=1470)),
                ("streams differ", dict(streams=491)),
                ("fewer entries than the proxy", dict(entries=6106, chunks=245)),
                ("no entries on Loki", dict(entries=0, chunks=0))):
            row = self.check(pr, loki(**numbers), first_pass_loki=first)
            self.assertEqual((len(row["loki_diffs"]), row["loki_explained"]), (1, []), name)
        # entries equal the proxy's: the older rule applies (only bytes and chunks differ), not the copies rule
        row = self.check(pr, loki(entries=12212))
        self.assertEqual((row["loki_diffs"], len(row["loki_explained"])), ([], 1))
        self.assertNotIn("times", row["loki_explained"][0])
        # a proxy that counts nothing is never a divisor of Loki's count
        zero = resources(("index/stats?q=1", {"streams": 490, "chunks": 490, "bytes": 0, "entries": 0}))
        zero_first = resources(("index/stats?q=1", {"streams": 490, "chunks": 490, "bytes": 0, "entries": 0}), uid="vp-loki")
        row = self.check(zero, loki(), first_pass_loki=zero_first)
        self.assertEqual((len(row["loki_diffs"]), row["loki_explained"]), (1, []))

    def test_a_base_that_cannot_answer_a_fixed_page_does_not_fail_the_runtime_proof(self):
        runtime = dict(browser_thread_seconds=1, browser_task_seconds=1, metric_samples=2,
                       js_heap_max_sampled_bytes=1000, api_requests=1, api_encoded_bytes=100,
                       page_encoded_bytes=200, first_api_data_ms=25)
        fixes = {"semantics/case-a": {"entries": ["page-a"], "status": "fixed"}}
        broken = dict(capture([], status=500, error="Internal Server Error"), runtime=runtime)
        answer = dict(capture([1]), runtime=runtime)
        # without a fix claim for the page the base's unusable answer is a failure, as before
        _, row = self.run_compare(broken, answer, answer, runtime_required=True)
        self.assertIn("main infrastructure error", " ".join(row["runtime_problems"]))
        self.assertNotIn("base_unusable", row)
        # a page the PR fixes: the base's own problems are set aside and recorded, the PR side stays fully proven
        _, row = self.run_compare(broken, answer, answer, runtime_required=True, fix_cases=fixes)
        self.assertEqual(row["runtime_problems"], [])
        self.assertIn("main infrastructure error", " ".join(row["base_unusable"]))
        self.assertIn("unusable as a baseline", row["base_comparison_note"])
        import visual_comment
        fails, warns, _ = visual_comment.assess(row, None)
        self.assertIn("base could not serve as the runtime baseline", " ".join(warns))
        # status other than fixed (an open case with partial proof) gets no allowance
        _, row = self.run_compare(broken, answer, answer, runtime_required=True,
                                  fix_cases={"semantics/case-a": {"entries": ["page-a"], "status": "open"}})
        self.assertTrue(row["runtime_problems"])
        # a PR that errors, never settled, differs from Loki, or regresses gets none either
        pr_error = dict(capture([], status=500, error="Internal Server Error"), runtime=runtime)
        _, row = self.run_compare(broken, pr_error, pr_error, runtime_required=True, fix_cases=fixes)
        self.assertTrue(row["runtime_problems"])
        unsettled = dict(answer, settled=False)
        _, row = self.run_compare(broken, unsettled, answer, runtime_required=True, fix_cases=fixes)
        self.assertTrue(row["runtime_problems"])
        other = dict(capture([2]), runtime=runtime)
        _, row = self.run_compare(broken, answer, other, runtime_required=True, fix_cases=fixes)
        self.assertTrue(row["runtime_problems"])
        slow = dict(answer, runtime=dict(runtime, browser_thread_seconds=9, js_heap_max_sampled_bytes=900 << 20))
        _, row = self.run_compare(broken, slow, slow, runtime_required=True, fix_cases=fixes)
        self.assertTrue(row["runtime_problems"], "a PR-side problem is never set aside")

    def test_an_unreadable_base_body_is_set_aside_only_when_the_pr_body_is_readable(self):
        runtime = dict(browser_thread_seconds=1, browser_task_seconds=1, metric_samples=2,
                       js_heap_max_sampled_bytes=1000, api_requests=1, api_encoded_bytes=100,
                       page_encoded_bytes=200, first_api_data_ms=25)
        fixes = {"semantics/case-a": {"entries": ["page-a"], "status": "fixed"}}
        answer = dict(capture([1]), runtime=runtime)
        unreadable = dict(capture([1]), runtime=runtime)
        unreadable["records"][0]["response"] = None  # a body the browser could not hand over (a very large answer)
        _, row = self.run_compare(unreadable, answer, answer, runtime_required=True)
        self.assertIn("unreadable API bodies", " ".join(row["runtime_problems"]))
        _, row = self.run_compare(unreadable, answer, answer, runtime_required=True, fix_cases=fixes)
        self.assertEqual(row["runtime_problems"], [])
        self.assertTrue(row["base_unusable"])
        # unreadable on the PR side as well: nothing is proven about the page, nothing is set aside
        both = dict(capture([1]), runtime=runtime)
        both["records"][0]["response"] = None
        _, row = self.run_compare(unreadable, both, answer, runtime_required=True, fix_cases=fixes)
        self.assertIn("unreadable API bodies", " ".join(row["runtime_problems"]))

    def test_kept_bodies_are_bounded(self):
        big = {"fields": [{"label": f"field_{i}", "type": "string", "cardinality": i} for i in range(500)]}
        pr = resources(*[(f"detected_fields?q={i}", big) for i in range(20)])
        loki = resources(*[(f"detected_fields?q={i}", {"fields": []}) for i in range(20)], uid="vp-loki")
        row = self.check(pr, loki)
        self.assertEqual(len(row["loki_bodies"]), compare.BODIES_PER_ROW)
        for kept in row["loki_bodies"]:
            self.assertLessEqual(len(kept["left"]), compare.BODY_CHARS + 40)
            self.assertTrue(kept["left"].endswith(" characters)"))
            self.assertEqual(kept["right"], '{"fields": []}')
            self.assertNotIn("numbers", kept)
        self.assertEqual(compare.bounded(None), "null")
        self.assertEqual(compare.stats_numbers({"streams": 1}), "streams 1, chunks None, entries None, bytes None")

    def test_patterns_set_is_compared_counts_are_not(self):
        pr = resources(("patterns?q=1", {"status": "success", "data": [{"pattern": "a <_>", "samples": [[1, 3]]}]}))
        row = self.check(pr, resources(("patterns?q=1", {"status": "success", "data": []}), uid="vp-loki"))
        self.assertEqual(len(row["loki_diffs"]), 1)
        self.assertEqual(row.get("loki_nondet") or [], [])
        same = self.check(pr, resources(("patterns?q=1", {"status": "success", "data": [{"pattern": "a <_>", "samples": [[1, 5]]}]}), uid="vp-loki"))
        self.assertEqual(same["loki_diffs"], [])

    def test_log_labels_extracted_suffix_explained_parsed_extra_not(self):
        loki = log_capture({"level": "info", "level_extracted": "info", "service_name": "svc", "service_name_extracted": "svc"},
                           {"level": "I", "level_extracted": "P", "service_name": "I", "service_name_extracted": "S"}, uid="vp-loki")
        pr = log_capture({"level": "info", "service_name": "svc"}, {"level": "I", "service_name": "S"}, uid="vp-pr")
        row = self.check(pr, loki)
        self.assertEqual(row["loki_diffs"], [])
        self.assertIn("_extracted", row["loki_explained"][0])
        pr = log_capture({"level": "info", "service_name": "svc", "method": "GET"}, {"level": "I", "service_name": "I", "method": "P"}, uid="vp-pr")
        row = self.check(pr, log_capture({"level": "info", "service_name": "svc"}, {"level": "I", "service_name": "I"}, uid="vp-loki"))
        self.assertEqual(len(row["loki_diffs"]), 1)


    def test_log_labels_parse_error_and_parsed_collision_explained(self):
        # A line | json rejects: Loki adds __error__ / __error_details__; a JSON key named like a stream label is
        # Loki's <key>_extracted while the proxy types the stream label as parsed.
        loki = log_capture({"app": "a", "__error__": "JSONParserErr", "__error_details__": "x", "service_name": "svc",
                            "service_name_extracted": "svc"},
                           {"app": "I", "__error__": "P", "__error_details__": "P", "service_name": "I",
                            "service_name_extracted": "P"}, uid="vp-loki")
        pr = log_capture({"app": "a", "service_name": "svc"}, {"app": "I", "service_name": "P"}, uid="vp-pr")
        row = self.check(pr, loki)
        self.assertEqual(row["loki_diffs"], [])
        self.assertIn("__error__", row["loki_explained"][0])
        self.assertIn("_extracted", row["loki_explained"][0])
        # a different value under the collision stays a difference
        pr = log_capture({"app": "a", "service_name": "other"}, {"app": "I", "service_name": "P"}, uid="vp-pr")
        self.assertEqual(len(self.check(pr, loki)["loki_diffs"]), 1)


def ranked_capture(series, expr='topk by (service_name) (2, sum by (service_name, level) (count_over_time({env="production"}[5s])))', uid="ds1"):
    """A capture of one topk query: {(service_name, level): {step: value}}, one frame per series."""
    frames = []
    for (svc, level), by_t in series.items():
        times = sorted(by_t)
        frames.append({"schema": {"name": "", "fields": [{"type": "time"}, {"type": "number", "labels": {"service_name": svc, "level": level}}]},
                       "data": {"values": [times, [by_t[t] for t in times]]}})
    rec = {"url": "/api/ds/query?ds_type=loki", "method": "POST", "status": 200,
           "request": {"from": "1", "to": "2", "requestId": uid + "-1", "queries": [
               {"refId": "A", "expr": expr, "queryType": "range", "datasource": {"type": "loki", "uid": uid}}]},
           "response": {"results": {"A": {"frames": frames}}}}
    return {"settled": True, "settle_ms": 1000, "records": [rec], "ui": {"noData": 0, "banners": [], "panelErrors": 0}}


class RankedAnswerTest(unittest.TestCase):
    """Loki keeps the tied series its map yields at the k-th place, so a topk answer is compared by its ranked values per step."""
    run_compare = CompareTest.run_compare

    # Step 10: pricing-svc ties error 5 = warn 5 behind info 20 (k=2). Loki may keep either; the proxy keeps error.
    PROXY = {("pricing-svc", "info"): {10: 20, 20: 20}, ("pricing-svc", "error"): {10: 5, 20: 4}}
    LOKI = {("pricing-svc", "info"): {10: 20, 20: 20}, ("pricing-svc", "warn"): {10: 5}, ("pricing-svc", "error"): {20: 4}}

    def test_a_different_tied_series_with_the_same_ranked_values_is_the_same_answer(self):
        _, row = self.run_compare(ranked_capture(self.PROXY), ranked_capture(self.PROXY), ranked_capture(self.LOKI))
        self.assertEqual(row["loki_diffs"], [])
        self.assertEqual(row["loki_new"], [])
        self.assertEqual(row["pr_vs_loki"], "1/1 identical")

    def test_uppercase_topk_uses_the_same_ranked_comparison(self):
        expr = 'TOPK(2, SUM BY (service_name, level) (COUNT_OVER_TIME({env="production"}[5s])))'
        _, row = self.run_compare(ranked_capture(self.PROXY, expr), ranked_capture(self.PROXY, expr), ranked_capture(self.LOKI, expr))
        self.assertEqual(row["loki_diffs"], [])

    def test_the_union_of_series_differing_by_one_is_still_a_difference_for_other_queries(self):
        expr = 'sum by (service_name, level) (count_over_time({env="production"}[5s]))'
        _, row = self.run_compare(ranked_capture(self.PROXY, expr), ranked_capture(self.PROXY, expr), ranked_capture(self.LOKI, expr))
        self.assertEqual(len(row["loki_diffs"]), 1)
        self.assertIn("series sets differ", row["loki_diffs"][0])

    def test_bottomk_is_ranked_too(self):
        expr = 'bottomk(2, sum by (service_name, level) (count_over_time({env="production"}[5s])))'
        _, row = self.run_compare(ranked_capture(self.PROXY, expr), ranked_capture(self.PROXY, expr), ranked_capture(self.LOKI, expr))
        self.assertEqual(row["loki_diffs"], [])

    def test_a_wrong_ranked_value_differs(self):
        loki = {**self.LOKI, ("pricing-svc", "warn"): {10: 6}}
        _, row = self.run_compare(ranked_capture(self.PROXY), ranked_capture(self.PROXY), ranked_capture(loki))
        self.assertEqual(len(row["loki_diffs"]), 1)
        self.assertIn("ranked values at 10", row["loki_diffs"][0])

    def test_a_missing_step_differs(self):
        loki = {("pricing-svc", "info"): {10: 20}, ("pricing-svc", "warn"): {10: 5}}  # nothing at step 20
        _, row = self.run_compare(ranked_capture(self.PROXY), ranked_capture(self.PROXY), ranked_capture(loki))
        self.assertEqual(len(row["loki_diffs"]), 1)
        self.assertIn("timestamps differ", row["loki_diffs"][0])

    def test_an_extra_ranked_series_at_a_step_differs(self):
        loki = {**self.LOKI, ("pricing-svc", "error"): {10: 5, 20: 4}}  # three series at step 10 against two
        _, row = self.run_compare(ranked_capture(self.PROXY), ranked_capture(self.PROXY), ranked_capture(loki))
        self.assertEqual(len(row["loki_diffs"]), 1)
        self.assertIn("ranked series at 10", row["loki_diffs"][0])

    def test_the_base_and_the_pr_run_the_same_code_so_their_series_must_match(self):
        swapped = {**self.PROXY, ("pricing-svc", "warn"): self.PROXY[("pricing-svc", "error")]}
        del swapped[("pricing-svc", "error")]
        code, row = self.run_compare(ranked_capture(self.PROXY), ranked_capture(swapped))
        self.assertEqual(code, 1)
        self.assertIn("series sets differ", row["main_pr_diffs"][0])

    def test_a_dropped_label_differs(self):
        # without (level): the proxy must keep the labels the series carry, whichever tied series it picks
        no_level = ranked_capture(self.LOKI)
        for frame in no_level["records"][0]["response"]["results"]["A"]["frames"]:
            frame["schema"]["fields"][1]["labels"].pop("level")
        _, row = self.run_compare(ranked_capture(self.PROXY), ranked_capture(self.PROXY), no_level)
        self.assertEqual(len(row["loki_diffs"]), 1)
        self.assertIn("label names differ", row["loki_diffs"][0])

    def test_a_frame_meta_difference_of_a_series_both_sides_have_differs(self):
        loki = ranked_capture(self.LOKI)
        for frame in loki["records"][0]["response"]["results"]["A"]["frames"]:
            frame["schema"]["fields"][1]["config"] = {"interval": 60000}
        _, row = self.run_compare(ranked_capture(self.PROXY), ranked_capture(self.PROXY), loki)
        self.assertEqual(len(row["loki_diffs"]), 1)
        self.assertIn("schema or meta", row["loki_diffs"][0])

    def test_an_error_answer_is_not_a_ranked_answer(self):
        err = ranked_capture(self.PROXY)
        err["records"][0]["response"] = {"results": {"A": {"error": "boom"}}}
        _, row = self.run_compare(ranked_capture(self.PROXY), ranked_capture(self.PROXY), err)
        self.assertEqual(len(row["loki_diffs"]), 1)


class OneSidedTest(unittest.TestCase):
    def test_only_an_extracted_field_breakdown_is_explained(self):
        key = ("query", "A", 'sum by (service_name_extracted) (count_over_time({service_name="x"} | service_name_extracted!="" [$__auto]))')
        self.assertIn("_extracted", compare.explain_one_sided(key))
        self.assertIsNone(compare.explain_one_sided(("query", "A", 'sum by (k8s_pod_name) (count_over_time({service_name="x"} [$__auto]))')))
        self.assertIsNone(compare.explain_one_sided(("resource", "/api/datasources/uid/*/resources/labels")))


class NondeterministicTest(unittest.TestCase):
    def test_only_unreadable_captures_are_history_dependent(self):
        import compare
        self.assertTrue(compare.nondeterministic("resource /api/datasources/uid/*/resources/labels: capture body unreadable"))
        self.assertFalse(compare.nondeterministic("resource /api/datasources/uid/*/resources/patterns?x: body differs"))
        self.assertFalse(compare.nondeterministic("query ('A', 'sum(...)'): value differs"))
        self.assertFalse(compare.nondeterministic("resource /api/datasources/uid/*/resources/labels: body differs"))

class ConfigurationTest(unittest.TestCase):
    def test_only_drilldown_limits_is_configuration(self):
        import compare
        self.assertTrue(compare.configuration("resource /api/datasources/uid/*/resources/drilldown-limits: body differs"))
        self.assertFalse(compare.configuration("resource /api/datasources/uid/*/resources/detected_fields?x: body differs"))


if __name__ == "__main__":
    unittest.main()
