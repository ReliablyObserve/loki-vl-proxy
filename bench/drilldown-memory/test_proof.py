import json
from pathlib import Path
import tempfile
import unittest

import proof


def body(series, warnings=None):
    obj = {'status': 'success', 'data': {'resultType': 'matrix', 'result': series}}
    if warnings is not None:
        obj['warnings'] = warnings
    return json.dumps(obj).encode()


class ProofTests(unittest.TestCase):
    def setUp(self):
        self.fixture = {'start': 1000000, 'end': 1172800, 'rows': 360000, 'recent_heavy': True}

    def test_full_fixture_oracle(self):
        for field in ('trace_id', 'span_id'):
            self.assertEqual(proof.expected_counts(self.fixture, field, 48), (360000, 360000))
        for field in ('request_id', 'total'):
            self.assertEqual(proof.expected_counts(self.fixture, field, 48), (180000, 180000))
        self.assertEqual(proof.expected_counts(self.fixture, 'user_id', 48), (5, 180000))
        self.assertEqual(proof.expected_counts(self.fixture, 'version', 48), (2, 360000))

    def test_short_range_includes_first_evaluation_lookback(self):
        field = 'trace_id'
        lower = (self.fixture['end'] - 12 * 3600 - 60) * 10**9
        included = sum(proof.timestamp(self.fixture, i) > lower for i in range(360000))
        self.assertEqual(proof.expected_counts(self.fixture, field, 12), (included, included))
        self.assertGreater(included, 288000)

    def test_query_uses_exact_drilldown_step_and_scoped_selector(self):
        query = proof.query(self.fixture, 'total', 48)
        self.assertEqual(query['step'], '60000ms')
        self.assertIn(proof.SELECTOR, query['query'])
        self.assertIn('json total="[\\"total\\"]"', query['query'])

    def test_canonical_normalizes_numbers_and_order(self):
        first = body([{'metric': {'version': 'v1'}, 'values': [[10, '1.0'], [20, '2']]}])
        second = body([{'metric': {'version': 'v1'}, 'values': [[20.0, '2.0'], [10.0, '1']]}])
        self.assertEqual(proof.canonical(first), proof.canonical(second))

    def test_canonical_rejects_warning_duplicate_and_nonfinite(self):
        series = {'metric': {'version': 'v1'}, 'values': [[10, '1']]}
        for value in (body([series], ['partial result']), body([series, series]),
                      body([{'metric': {}, 'values': [[10, 'NaN']]}]),
                      body([{'metric': {}, 'values': [[10, '1'], [10, '1']]}])):
            with self.assertRaises(ValueError):
                proof.canonical(value)

    def test_nonempty_partial_reference_is_rejected(self):
        partial = body([{'metric': {'trace_id': '0'}, 'values': [[10, '1']]}])
        with self.assertRaisesRegex(ValueError, 'incomplete Loki'):
            proof.validate_reference(partial, self.fixture, 'trace_id', 48)

    def test_ownership_binds_project_and_context(self):
        with tempfile.TemporaryDirectory() as output:
            args = proof.parser().parse_args(['cleanup', '--output', output])
            harness = proof.Harness(args)
            proof.write_json(Path(output) / 'owner.json', {'project': args.project, 'context': 'other'})
            with self.assertRaises(RuntimeError):
                harness.owned()

    def test_seed_attempt_cannot_be_repeated(self):
        with tempfile.TemporaryDirectory() as output:
            args = proof.parser().parse_args(['seed', '--output', output, '--rows', '10'])
            harness = proof.Harness(args)
            proof.write_json(Path(output) / 'owner.json', {'project': args.project, 'context': args.context})
            (Path(output) / 'seed-started.json').write_text('{}')
            with self.assertRaises(FileExistsError):
                harness.seed()


if __name__ == '__main__':
    unittest.main()
