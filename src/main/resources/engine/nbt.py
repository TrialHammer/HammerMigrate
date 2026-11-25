"""Lossless NBT field patches and Anvil sector editing, including external chunks."""

import gzip, hashlib, io, json, re, struct, subprocess, zlib
from pathlib import Path

JAR = None


def lz4(raw, encode=False):
    if not JAR:
        raise ValueError("LZ4 chunk requires the packaged Java codec")
    p = subprocess.run(
        [
            "java",
            "-cp",
            str(JAR),
            "ink.ziip.hammer.migrate.Codec",
            "encode" if encode else "decode",
        ],
        input=raw,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if p.returncode:
        raise ValueError(
            "LZ4 codec failed: " + p.stderr.decode(errors="replace")[-200:]
        )
    return p.stdout


def unpack_payload(raw, mode):
    if mode == 1:
        return gzip.decompress(raw)
    if mode == 2:
        return zlib.decompress(raw)
    if mode == 3:
        return raw
    if mode == 4:
        return lz4(raw)
    raise ValueError("Unknown Anvil compression " + str(mode))


def pack_payload(raw, mode):
    if mode == 1:
        return gzip.compress(raw, mtime=0)
    if mode == 2:
        return zlib.compress(raw)
    if mode == 3:
        return raw
    if mode == 4:
        return lz4(raw, True)
    raise ValueError("Unknown Anvil compression")


class Node:
    def __init__(self, t, start, end, value):
        self.t = t
        self.start = start
        self.end = end
        self.value = value


class Reader:
    def __init__(self, raw):
        self.raw = raw
        self.i = 0
        self.count = 0

    def take(self, n):
        if n < 0 or self.i + n > len(self.raw):
            raise ValueError("Invalid NBT length")
        out = self.raw[self.i : self.i + n]
        self.i += n
        return out

    def num(self, fmt):
        return struct.unpack(fmt, self.take(struct.calcsize(fmt)))[0]

    def string(self):
        return (
            self.take(self.num(">H"))
            .replace(b"\xc0\x80", b"\x00")
            .decode("utf8", "surrogatepass")
        )

    def tag(self, t, depth=0):
        self.count += 1
        if depth > 128 or self.count > 5000000:
            raise ValueError("NBT exceeds parsing limits")
        start = self.i
        if t in range(1, 7):
            v = self.num({1: ">b", 2: ">h", 3: ">i", 4: ">q", 5: ">f", 6: ">d"}[t])
        elif t == 8:
            v = self.string()
        elif t in (7, 11, 12):
            v = self.take(self.num(">i") * {7: 1, 11: 4, 12: 8}[t])
        elif t == 9:
            subtype = self.num(">B")
            n = self.num(">i")
            if n < 0 or n > len(self.raw) or n and subtype == 0:
                raise ValueError("Invalid NBT list")
            v = [self.tag(subtype, depth + 1) for _ in range(n)]
        elif t == 10:
            v = {}
            while True:
                typ = self.num(">B")
                if not typ:
                    break
                key_start = self.i
                key = self.string()
                key_end = self.i
                if key in v:
                    raise ValueError("Duplicate compound key")
                v[key] = self.tag(typ, depth + 1)
                v[key].key_span = (key_start, key_end)
        else:
            raise ValueError("Unknown NBT tag " + str(t))
        return Node(t, start, self.i, v)

    def root(self):
        typ = self.num(">B")
        self.string()
        n = self.tag(typ)
        if self.i != len(self.raw):
            raise ValueError("Trailing NBT data")
        return n


def string_bytes(s):
    raw = s.encode("utf8", "surrogatepass").replace(b"\x00", b"\xc0\x80")
    if len(raw) > 65535:
        raise ValueError("NBT string too long")
    return struct.pack(">H", len(raw)) + raw


def role(path, key, location, identity):
    for rule in identity.rules:
        if re.fullmatch(rule["location"], location):
            return rule["role"]
    f = re.sub(r"\[\d+\]", "[]", path)
    if any(
        x in f.lower()
        for x in (
            "minecraft:profile",
            "skullowner",
            "gameprofile",
            "textures",
            "minecraft:lore",
            ".lore",
            ".lastoutput",
            ".messages",
            ".customname",
        )
    ):
        return "preserve"
    if f in ("$.UUID", "$.Data.Player.UUID"):
        return (
            "uuid"
            if "playerdata/" in location or f == "$.Data.Player.UUID"
            else "preserve"
        )
    if key.lower() in ("lastknownname", "mapcopyname"):
        return "name"
    known = (
        "Owner",
        "Target",
        "OwnerUUID",
        "owner_uuid",
        "Thrower",
        "ThrowerUUID",
        "LoveCause",
        "Trusted[]",
        "Gossips[].Target",
        "minecraft:liked_player.value",
        "interaction.player",
        "attack.player",
        "Paper.ExpData.trigger",
        "MapCopyOwner",
        "lootedPlayers[].UUID",
        "rewarded_players[]",
        "registered_players[]",
        "minecraft:angry_at.value",
        "anger.suspects[].uuid",
        "last_hurt_by_player",
        "last_hurt_by_mob",
        "Paper.SpawningEntity",
        "SpawningEntity",
        "Paper.ExpData.source",
        "angry_at",
    )
    if any(f.endswith("." + s) for s in known):
        return "uuid"
    if key in (
        "lands:o",
        "lands:placer",
        "blocklocker:u",
        "hammercore:owneruuid",
        "codframe:owner",
    ):
        return "uuid"
    if key.lower() in ("uuid", "worlduuid", "worlduuidmost", "worlduuidleast"):
        return "preserve"
    return "unknown"


def convert(raw, identity, location):
    lowered = raw.lower()
    needles = (
        identity.raw_old,
        identity.raw_new,
        identity.old_uuid.encode(),
        identity.new_uuid.encode(),
        identity.old_uuid.replace("-", "").encode(),
        identity.new_uuid.replace("-", "").encode(),
        identity.old.lower().encode(),
        identity.new.lower().encode(),
    )
    if (
        not any(
            (
                needle in lowered
                if needle not in (identity.raw_old, identity.raw_new)
                else needle in raw
            )
            for needle in needles
        )
        and b"most" not in lowered
    ):
        return raw
    root = Reader(raw).root()
    patches = []

    def add(node, body):
        if raw[node.start : node.end] != body:
            patches.append((node.start, node.end, body))

    def walk(node, path, key=""):
        r = role(path, key, location + ":" + path, identity)
        if r == "preserve":
            return
        if node.t == 10:
            # Older NBT stores player references as signed Most/Least pairs.
            lower_keys = {k.lower(): k for k in node.value}
            for k, v in node.value.items():
                key_role = (
                    "uuid"
                    if identity.raw_uuid(k) in (identity.raw_old, identity.raw_new)
                    else (
                        "name"
                        if k.lower() in (identity.old.lower(), identity.new.lower())
                        else "preserve"
                    )
                )
                new_key = identity.scalar(k, key_role, location + ":" + path + "#key")
                if new_key != k:
                    if new_key in node.value:
                        raise ValueError("NBT compound key collision")
                    start, end = v.key_span
                    patches.append((start, end, string_bytes(new_key)))
                least_key = lower_keys.get((k[:-4] + "least").lower())
                if k.lower().endswith("most") and least_key and v.t == 4:
                    lo = node.value[least_key]
                    if lo.t != 4:
                        raise ValueError("Mismatched NBT Most/Least types")
                    prefix = k[:-4]
                    b = struct.pack(
                        ">QQ", v.value & ((1 << 64) - 1), lo.value & ((1 << 64) - 1)
                    )
                    rr = role(
                        path + "." + prefix,
                        prefix,
                        location + ":" + path + "." + prefix,
                        identity,
                    )
                    out = identity.scalar(b, rr, location + ":" + path + "." + prefix)
                    if out != b:
                        add(v, out[:8])
                        add(lo, out[8:])
                walk(v, path + "." + k, k)
        elif node.t == 9:
            if len(node.value) in (2, 4) and all(
                v.t == (4 if len(node.value) == 2 else 3) for v in node.value
            ):
                width = 8 if len(node.value) == 2 else 4
                packed = b"".join(
                    (v.value & ((1 << (width * 8)) - 1)).to_bytes(width, "big")
                    for v in node.value
                )
                out = identity.scalar(packed, r, location + ":" + path)
                if out != packed:
                    for i, v in enumerate(node.value):
                        add(v, out[i * width : (i + 1) * width])
                    return
            for i, v in enumerate(node.value):
                walk(v, path + "[" + str(i) + "]", key)
        elif node.t == 8:
            if key == "quickshop-hikari:hopper-persistent-data":
                try:
                    obj = json.loads(node.value)
                except ValueError:
                    raise ValueError("Malformed QuickShop hopper JSON")
                out = identity.structured(
                    obj, "quickshop_players", location + ":" + path
                )
                if out != obj:
                    add(
                        node,
                        string_bytes(
                            json.dumps(out, separators=(",", ":"), ensure_ascii=False)
                        ),
                    )
            else:
                out = identity.scalar(node.value, r, location + ":" + path)
                if out == node.value and node.value.lstrip().startswith(("{", "[")):
                    try:
                        obj = json.loads(node.value)
                    except ValueError:
                        obj = None
                    if isinstance(obj, (dict, list)):
                        changed = identity.structured(
                            obj, "nbt_players", location + ":" + path
                        )
                        if changed != obj:
                            out = json.dumps(
                                changed, separators=(",", ":"), ensure_ascii=False
                            )
                if (
                    out == node.value
                    and (
                        identity.old_uuid in node.value.lower()
                        or re.search(
                            r"(?<![A-Za-z0-9_])"
                            + re.escape(identity.old)
                            + r"(?![A-Za-z0-9_])",
                            node.value,
                            re.I,
                        )
                    )
                    and node.value.lower() != identity.old.lower()
                ):
                    identity.note(
                        "unknown",
                        location + ":" + path,
                        "Embedded NBT string identity requires an adapter",
                    )
                if out != node.value:
                    add(node, string_bytes(out))
        elif node.t in (7, 11, 12):
            out = identity.scalar(node.value, r, location + ":" + path)
            if out != node.value:
                add(node, raw[node.start : node.start + 4] + out)

    walk(root, "$")
    patches.sort()
    if any(a[1] > b[0] for a, b in zip(patches, patches[1:])):
        raise ValueError("Overlapping NBT patches")
    out = raw
    for start, end, body in reversed(patches):
        out = out[:start] + body + out[end:]
    Reader(out).root()
    return out


def convert_file(raw, identity, location):
    if raw.startswith(b"\x1f\x8b"):
        data = gzip.decompress(raw)
        out = convert(data, identity, location)
        return gzip.compress(out, mtime=0) if out != data else raw
    if raw.startswith((b"x\x01", b"x\x9c", b"x\xda")):
        data = zlib.decompress(raw)
        out = convert(data, identity, location)
        return zlib.compress(out) if out != data else raw
    return convert(raw, identity, location)


def region(raw, path, identity, external_values=None):
    if len(raw) < 8192:
        raise ValueError("Truncated region header")
    used = set()
    out = bytearray(raw)
    external = []
    for slot in range(1024):
        if identity.scan_progress:
            identity.scan_progress()
        entry = int.from_bytes(raw[slot * 4 : slot * 4 + 4], "big")
        if not entry:
            continue
        offset = entry >> 8
        size = entry & 255
        sectors = set(range(offset, offset + size))
        if (
            offset < 2
            or not size
            or (offset + size) * 4096 > len(raw)
            or used & sectors
        ):
            raise ValueError("Invalid / overlapping region allocation")
        used |= sectors
        start = offset * 4096
        n = int.from_bytes(raw[start : start + 4], "big")
        mode = raw[start + 4]
        if n < 1 or n + 4 > size * 4096:
            raise ValueError("Invalid chunk record size")
        if mode & 128:
            rx, rz = map(int, path.stem.split(".")[1:])
            p = path.parent / f"c.{rx*32+slot%32}.{rz*32+slot//32}.mcc"
            if p.is_symlink():
                raise ValueError("External chunk is a symlink")
            identity.external_chunks.add(str(p))
            original = (
                external_values[str(p)]
                if external_values is not None and str(p) in external_values
                else p.read_bytes()
            )
            identity.external_fingerprints[str(p)] = hashlib.sha256(
                original
            ).hexdigest()
            data = unpack_payload(original, mode & 127)
            changed = convert(data, identity, str(p) + ":slot=" + str(slot))
            if changed != data:
                external.append((p, original, pack_payload(changed, mode & 127)))
            continue
        data = unpack_payload(raw[start + 5 : start + 4 + n], mode)
        changed = convert(data, identity, str(path) + ":slot=" + str(slot))
        if changed == data:
            continue
        payload = pack_payload(changed, mode)
        record = struct.pack(">I", len(payload) + 1) + bytes([mode]) + payload
        need = (len(record) + 4095) // 4096
        if need > 255:
            raise ValueError(
                "Changed chunk exceeds internal Anvil allocation; externalization required"
            )
        if need <= size:
            out[start : start + size * 4096] = record + bytes(size * 4096 - len(record))
        else:
            out.extend(bytes((-len(out)) % 4096))
            new_offset = len(out) // 4096
            if new_offset > 0xFFFFFF:
                raise ValueError("Region offset exceeds format limit")
            out.extend(record + bytes(need * 4096 - len(record)))
            out[slot * 4 : slot * 4 + 4] = ((new_offset << 8) | need).to_bytes(4, "big")
    return bytes(out), external
