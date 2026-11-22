"""Text and artifact I/O helpers shared by visual proof and its trusted renderer."""
import json
import re


def load_json(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def dump_json(path, obj, indent=1):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=indent)


def write_text(path, text):
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)


def error_text(message):
    """Loki's pipeline reports whichever failing stream a shard met first."""
    return re.sub(r"for series: '\{.*?\}'\.", "for series: '{...}'.", str(message).strip(), flags=re.S)
