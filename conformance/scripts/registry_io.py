"""Small file helpers shared by the conformance scripts.

Every read and write goes through a context manager so no handle is left open,
and the scripts stay readable.
"""
import json


def read_text(path, errors="strict"):
    with open(path, encoding="utf-8", errors=errors) as handle:
        return handle.read()


def load_json(path):
    with open(path, encoding="utf-8") as handle:
        return json.load(handle)


def write_text(path, text):
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(text)


def dump_json(path, payload):
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
