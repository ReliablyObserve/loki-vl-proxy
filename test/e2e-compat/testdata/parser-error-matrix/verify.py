#!/usr/bin/env python3
"""Verify retained source identities, records and observed parity (no services)."""

import gzip
import hashlib
import json
import math
from pathlib import Path


def same_answer(left, right):
    if left["status"] != right["status"]:
        return False
    if left["status"] == 400:
        # Different shards can report different malformed rows first; Loki
        # uses plain text while the proxy keeps its JSON error envelope.
        return any(
            category in left["response"] and category in right["response"]
            for category in ("JSONParserErr", "parse error")
        )
    if left["status"] != 200:
        return False
    first, second = left["response"], right["response"]
    if first["status"] != "success" or second["status"] != "success":
        return False
    kind = first["data"]["resultType"]
    if kind != second["data"]["resultType"]:
        return False

    def series(response):
        result = {}
        for item in response["data"]["result"]:
            labels = tuple(sorted(item["metric"].items()))
            assert labels not in result, "duplicate output series"
            result[labels] = [item["value"]] if kind == "vector" else item["values"]
        return result

    left_series, right_series = series(first), series(second)
    if left_series.keys() != right_series.keys():
        return False
    for labels, points in left_series.items():
        other = right_series[labels]
        if len(points) != len(other):
            return False
        for (stamp, value), (other_stamp, other_value) in zip(points, other):
            if stamp != other_stamp or not math.isclose(float(value), float(other_value), rel_tol=0, abs_tol=1e-12):
                return False
    return True


def verify(directory):
    manifest = json.loads((directory / "manifest.json").read_text())
    assert manifest["schema_version"] == 1
    for source in ("main", "pr"):
        name = source + ".json.gz"
        packed = (directory / name).read_bytes()
        assert hashlib.sha256(packed).hexdigest() == manifest["sha256"][name]
        records = json.loads(gzip.decompress(packed))
        pairs = {}
        for record in records:
            case, backend = record["case"].rsplit("/", 1)
            assert backend in ("loki", "proxy")
            pair = pairs.setdefault(case, {})
            assert backend not in pair, "duplicate backend response"
            pair[backend] = record
        assert len(pairs) == manifest["case_count_per_source"]
        mismatches = []
        for case, pair in pairs.items():
            assert pair.keys() == {"loki", "proxy"}
            assert pair["loki"]["params"] == pair["proxy"]["params"]
            if not same_answer(pair["loki"], pair["proxy"]):
                mismatches.append(case + "/proxy")
        summary = manifest["summary"][source]
        assert len(records) == summary["records"]
        assert sorted(mismatches) == sorted(summary["failed_cases"])
        assert len(pairs) - len(mismatches) == summary["proxy_passed"]
        assert len(mismatches) == summary["proxy_failed"]
        print(f"{source}: {len(pairs) - len(mismatches)}/{len(pairs)} matching cases")
    assert manifest["summary"]["pr"]["proxy_failed"] == 0


if __name__ == "__main__":
    for directory in sorted(Path(__file__).parent.iterdir()):
        if (directory / "manifest.json").is_file():
            print(directory.name)
            verify(directory)
