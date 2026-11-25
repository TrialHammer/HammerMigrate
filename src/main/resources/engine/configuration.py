"""Read storage configuration without materializing credentials into manifests."""

import json
import re
import tomllib
from yaml_storage import parse as parse_yaml


def read(path):
    text = path.read_text()
    suffix = path.suffix.lower()
    if suffix == ".json":
        return json.loads(text)
    if suffix == ".toml":
        return tomllib.loads(text)
    if suffix == ".properties":
        result = {}
        for line in text.splitlines():
            line = line.strip()
            if not line or line.startswith(("#", "!")):
                continue
            match = re.match(r"([^:=\s]+)\s*[:=]\s*(.*)$", line)
            if not match:
                raise ValueError("Unsupported properties configuration syntax")
            keys = match[1].split(".")
            section = result
            for key in keys[:-1]:
                section = section.setdefault(key, {})
            section[keys[-1]] = match[2]
        return result
    return parse_yaml(text)
