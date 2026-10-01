"""File helpers shared by the visual-proof scripts: every file is closed again."""
import json


def load_json(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def dump_json(path, obj, indent=1):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=indent)


def write_text(path, text):
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)
