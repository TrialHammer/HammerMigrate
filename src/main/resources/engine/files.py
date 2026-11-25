"""Format-specific physical storage migration; snapshots move by owner index."""

import csv, io, json, os, re
import tomllib
from pathlib import Path
import yaml
from yaml_storage import parse as parse_yaml
from common import patch, safe_path, sha, ignored_path, parse_json
import nbt
from discovery import HISTORY

PLAYER_FILES = {"usercache.json", "ops.json", "whitelist.json", "banned-players.json"}


def sparse_patch(before, after):
    # Region layout keeps existing offsets. Record only touched sectors and appended bytes.
    if len(after) < len(before):
        return patch(before, after)
    pieces = []
    for pos in range(0, len(before), 4096):
        a = before[pos : pos + 4096]
        b = after[pos : pos + len(a)]
        if a != b:
            pieces.append({"start": pos, "old": a, "new": b})
    if len(after) > len(before):
        pieces.append({"start": len(before), "old": b"", "new": after[len(before) :]})
    return {"before": sha(before), "after": sha(after), "pieces": pieces}


def transform(p, kind, raw, ident):
    loc = str(p)
    table = "players/" + loc if p.name in PLAYER_FILES else loc
    if kind == "region":
        return nbt.region(raw, p, ident)
    if kind == "nbt":
        return nbt.convert_file(raw, ident, loc), []
    if kind == "external-chunk":
        data = nbt.lz4(raw) if raw.startswith(b"LZ4Block") else raw
        changed = nbt.convert_file(data, ident, loc)
        if changed == data:
            return raw, []
        return (nbt.lz4(changed, True) if raw.startswith(b"LZ4Block") else changed), []
    if kind == "csv":
        text = raw.decode("utf8")
        # Shopkeepers trade logs may start with a one-character metadata row
        # before the actual CSV header. Keep that row byte-for-byte while
        # giving the CSV parser the real header and records.
        prefix = ""
        while True:
            line_end = text.find("\n")
            if line_end < 0:
                break
            first = text[: line_end + 1]
            if first.strip() not in ("", ","):
                break
            prefix += first
            text = text[line_end + 1 :]
        dialect = csv.Sniffer().sniff(text[:8192])
        reader = csv.DictReader(io.StringIO(text), dialect=dialect)
        if not reader.fieldnames or len(set(reader.fieldnames)) != len(
            reader.fieldnames
        ):
            raise ValueError("Invalid CSV headers")
        rows = list(reader)
        changed = []
        for row in rows:
            if None in row or any(v is None for v in row.values()):
                raise ValueError("CSV column count mismatch")
            changed.append(
                {
                    key: ident.cell(value, table, key, loc + "." + key)
                    for key, value in row.items()
                }
            )
        if rows == changed:
            return raw, []
        out = io.StringIO()
        writer = csv.DictWriter(out, reader.fieldnames, dialect=dialect)
        writer.writeheader()
        writer.writerows(changed)
        return (prefix + out.getvalue()).encode(), []
    if kind == "toml":
        import tomlkit

        text = raw.decode("utf8")
        parsed = tomllib.loads(text)
        changed = ident.structured(parsed, table, loc)
        if parsed == changed:
            return raw, []
        document = tomlkit.parse(text)

        def update(node, before, after):
            for key, value in before.items():
                new_key = (
                    ident.new_uuid
                    if key == ident.old_uuid
                    else ident.new if key == ident.old else key
                )
                if new_key not in after:
                    new_key = ident.new.lower() if key == ident.old.lower() else key
                if new_key != key:
                    node[new_key] = node.pop(key)
                if isinstance(value, dict) and isinstance(after[new_key], dict):
                    update(node[new_key], value, after[new_key])
                elif value != after[new_key]:
                    node[new_key] = after[new_key]

        update(document, parsed, changed)
        out = tomlkit.dumps(document)
        if tomllib.loads(out) != changed:
            raise ValueError("TOML roundtrip mismatch")
        return out.encode(), []
    if kind == "properties":
        text = raw.decode("utf8")
        out = []
        for line in text.splitlines(keepends=True):
            match = re.match(
                r"^(\s*)([^#!\s:=][^:=]*?)(\s*[:=]\s*)(.*?)(\r?\n)?$", line
            )
            if not match:
                if ident.old_uuid in line or ident.old in line:
                    ident.note("unknown", loc, "Escaped/multiline properties entry")
                out.append(line)
                continue
            prefix, key, separator, value, ending = match.groups()
            new_value = ident.cell(value, table, key, loc + "." + key)
            key_role = (
                "uuid"
                if ident.raw_uuid(key)
                else (
                    "name"
                    if key.lower() in (ident.old.lower(), ident.new.lower())
                    else "preserve"
                )
            )
            new_key = ident.scalar(key, key_role, loc + "#key")
            out.append(prefix + new_key + separator + new_value + (ending or ""))
        return "".join(out).encode(), []
    if kind in ("yaml", "json"):
        text = raw.decode("utf8")
        obj = parse_yaml(text) if kind == "yaml" else parse_json(text)
        changed = ident.structured(obj, table, loc)
        if changed == obj:
            return raw, []
        if kind == "yaml":
            edits = []
            seen = set()

            def visit(node, before, after):
                if before == after:
                    return
                if id(node) in seen:
                    raise ValueError("Changed YAML alias needs an explicit adapter")
                seen.add(id(node))
                if isinstance(node, yaml.MappingNode):
                    if len(node.value) != len(before) or len(before) != len(after):
                        raise ValueError(
                            "Duplicate YAML keys or unsupported mapping change"
                        )
                    for (
                        (key_node, value_node),
                        (old_key, old_value),
                        (new_key, new_value),
                    ) in zip(node.value, before.items(), after.items()):
                        visit(key_node, old_key, new_key)
                        visit(value_node, old_value, new_value)
                elif isinstance(node, yaml.SequenceNode):
                    for child, old_value, new_value in zip(node.value, before, after):
                        visit(child, old_value, new_value)
                elif isinstance(before, str) and isinstance(after, str):
                    edits.append(
                        (
                            node.start_mark.index,
                            node.end_mark.index,
                            json.dumps(after, ensure_ascii=False),
                        )
                    )
                else:
                    raise ValueError("Unsupported YAML scalar change")

            visit(yaml.compose(text), obj, changed)
            out = text
            for start, end, value in sorted(edits, reverse=True):
                out = out[:start] + value + out[end:]
        else:
            out = json.dumps(changed, ensure_ascii=False, indent=2) + "\n"
        # Parser roundtrip must preserve the planned structure exactly.
        if (parse_yaml(out) if kind == "yaml" else parse_json(out)) != changed:
            raise ValueError("Structured file roundtrip failed " + loc)
        return out.encode(), []
    if p.name == "premium_names.cache":
        lines = raw.decode("utf8").splitlines()
        out = []
        for line in lines:
            if line.lower() == ident.new.lower():
                ident.note("collisions", loc, line)
            if line.lower() == ident.old.lower():
                ident.note("changes", loc, line)
                if ident.premium:
                    out.append(ident.new.lower())
            else:
                out.append(line)
        if ident.premium and ident.new.lower() not in [v.lower() for v in out]:
            out.append(ident.new.lower())
            ident.note("changes", loc, "Enable verified premium name cache")
        return ("\n".join(out) + "\n").encode() if out != lines else raw, []
    if kind in ("properties", "text-cache", "csv", "text", "toml", "config"):
        text = raw.decode("utf8")
        # Cache files containing a single exact identity per line are discoverable.
        if kind == "text-cache":
            lines = text.splitlines(keepends=True)
            out = []
            for line in lines:
                value = line.rstrip("\r\n")
                new = ident.scalar(
                    value, "uuid" if ident.raw_uuid(value) else "name", loc
                )
                out.append(new + line[len(value) :])
            return "".join(out).encode(), []
        if (
            ident.old_uuid in text.lower()
            or ident.old_uuid.replace("-", "") in text.lower()
            or re.search(
                r"(?<![A-Za-z0-9_])" + re.escape(ident.old) + r"(?![A-Za-z0-9_])",
                text,
                re.I,
            )
        ):
            ident.note(
                "unknown",
                loc,
                "Text format requires structured field adapter / explicit rule",
            )
        if (
            ident.new_uuid in text.lower()
            or ident.new_uuid.replace("-", "") in text.lower()
            or (
                ident.old.lower() != ident.new.lower()
                and re.search(
                    r"(?<![A-Za-z0-9_])" + re.escape(ident.new) + r"(?![A-Za-z0-9_])",
                    text,
                    re.I,
                )
            )
        ):
            ident.note("collisions", loc, "Text format contains target identity")
        return raw, []
    # Unsupported binary formats cannot silently hide an account reference.
    old_name = re.search(
        rb"(?<![A-Za-z0-9_])" + ident.old.encode() + rb"(?![A-Za-z0-9_])", raw, re.I
    )
    lowered = raw.lower()
    if (
        ident.raw_old in raw
        or ident.old_uuid.encode() in lowered
        or ident.old_uuid.replace("-", "").encode() in lowered
        or old_name
    ):
        ident.note("unknown", loc, "Opaque format contains source identity")
    if (
        ident.raw_new in raw
        or ident.new_uuid.encode() in lowered
        or ident.new_uuid.replace("-", "").encode() in lowered
        or (
            ident.old.lower() != ident.new.lower()
            and re.search(
                rb"(?<![A-Za-z0-9_])" + ident.new.encode() + rb"(?![A-Za-z0-9_])",
                raw,
                re.I,
            )
        )
    ):
        ident.note("collisions", loc, "Opaque format contains target identity")
    return raw, []


def moves(cfg, ident):
    root = Path(cfg["networkRoot"])
    result = []
    skip = set(cfg.get("exclude", [])) | {
        "HammerMigrate",
        "libraries",
        "bundler",
        ".git",
        ".paper-remapped",
        "logs",
        "node_modules",
    }
    for server in cfg["servers"]:
        for directory, dirs, files in os.walk(root / server, followlinks=False):
            parent = Path(directory)
            for name in list(dirs):
                if (
                    (parent / name).is_symlink()
                    or name in skip
                    or name == "cache"
                    and parent == root / server
                    or HISTORY.search(name)
                    or ignored_path(cfg, str((parent / name).relative_to(root)))
                ):
                    if (
                        name in ("backups", "backup")
                        and "InventoryRollbackPlus" in parent.parts
                    ):
                        continue
                    dirs.remove(name)
            for name in dirs + files:
                p = parent / name
                if p.is_symlink() or ignored_path(cfg, str(p.relative_to(root))):
                    continue
                changed = name
                token = name.split(".", 1)[0]
                raw_uuid = ident.raw_uuid(token)
                source_name = token.lower() == ident.old.lower()
                target_name = token.lower() == ident.new.lower() and not source_name
                role = "unknown"
                player_directories = {
                    "players",
                    "playerdata",
                    "userdata",
                    "users",
                    "user",
                    "stats",
                    "advancements",
                    "profiles",
                    "preferences",
                    "backups",
                    "backup",
                    "history",
                    "cache",
                }
                if any(
                    part.lower() in player_directories
                    for part in p.parent.relative_to(root / server).parts
                ):
                    role = "name"
                if p.is_dir() and (p / "level.dat").exists():
                    role = "preserve"
                if (
                    p.parent == root / server
                    or p.name.lower()
                    in {
                        "config.yml",
                        "config.yaml",
                        "config.json",
                        "settings.yml",
                        "data.yml",
                        "storage.yml",
                        "messages.yml",
                        "database.yml",
                    }
                    and role == "unknown"
                ):
                    role = "preserve"
                for rule in ident.rules:
                    if re.fullmatch(rule["location"], str(p) + "#path"):
                        role = rule["role"]
                        break
                if source_name and role == "name":
                    changed = ident.new + name[len(token) :]
                elif source_name and role == "unknown":
                    ident.note(
                        "unknown",
                        str(p) + "#path",
                        "Source-named path needs a player-storage context or field rule",
                    )
                if target_name and role == "name":
                    ident.note("collisions", str(p), "Existing target username path")
                if raw_uuid == ident.raw_old:
                    changed = (
                        ident.new_uuid
                        if "-" in token
                        else ident.new_uuid.replace("-", "")
                    ) + name[len(token) :]
                if raw_uuid == ident.raw_new:
                    ident.note("collisions", str(p), "Existing target UUID path")
                if changed != name:
                    if any(
                        n != name and n.lower() == changed.lower()
                        for n in os.listdir(parent)
                    ):
                        ident.note("collisions", str(p), "Destination already exists")
                    result.append(
                        {
                            "kind": "move",
                            "source": str(p.relative_to(root)),
                            "target": str(p.with_name(changed).relative_to(root)),
                            "directory": p.is_dir(),
                        }
                    )
    return sorted(result, key=lambda x: (-len(Path(x["source"]).parts), x["source"]))
