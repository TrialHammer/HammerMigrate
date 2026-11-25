"""Semantic account references shared by physical and relational adapters."""

import base64
import gzip
import json
import re
import uuid
import zlib
from common import offline, parse_json

PRESERVE = re.compile(
    r"(password|salt|email|signature|texture|premium.?uuid|world.?uuid|server.?uuid|session.?id|profile.?id|profile.?uuid|skin.?id|skin.?uuid|nickname|displayname|totp)",
    re.I,
)
UUID_KEYS = re.compile(
    r"^(uuid|uniqueid|unique_id|player|player_?id|player_?uuid|user_?uuid|user_?id|owner|owner_?uuid|owner_?id|creator|creator_?uuid|victim_?uuid|killer_?uuid|sender_?uuid|receiver_?uuid|target_?uuid|author_?uuid|source_?uuid|visitor_?uuid|buyer_?uuid|seller_?uuid|account|account_?id|account_?uuid|member|members|trusted|thrower|lovecause|target|angry_at|trigger|source|mapcopyowner|lootedplayers|rewarded_players|registered_players)$",
    re.I,
)
NAME_KEYS = re.compile(
    r"^(username|realname|player_?name|user_?name|owner_?name|cachedname|last_?known_?name|mapcopyname|name|user)$",
    re.I,
)
PLAYER_TABLE = re.compile(
    r"(player|user|account|authme|cache|econom|balance|points|sign|bans|victim|names|co_user|whitelist|husk|cmi|faction|member|resident|town|lands|pkw|chess|liberty|quickshop)",
    re.I,
)
PROFILE_BRANCHES = {
    "textures",
    "signature",
    "minecraft:profile",
    "skullowner",
    "gameprofile",
    "profile",
    "skin",
    "minecraft:lore",
    "lore",
    "lastoutput",
    "messages",
    "customname",
}


class Identity:
    def __init__(self, old, new, premium=False, rules=(), merge=False):
        for name in (old, new):
            if not re.fullmatch(r"[A-Za-z0-9_]{3,16}", name):
                raise ValueError(
                    "Java player names must be 3..16 ASCII letters, numbers or underscores"
                )
        if old == new:
            raise ValueError("Source and target must differ")
        self.old = old
        self.new = new
        self.old_uuid = offline(old)
        self.new_uuid = offline(new)
        self.raw_old = uuid.UUID(self.old_uuid).bytes
        self.raw_new = uuid.UUID(self.new_uuid).bytes
        self.premium = premium
        self.merge = merge
        self.premium_uuid = None
        self.rules = rules
        self.changes = []
        self.collisions = []
        self.unknown = []
        self.preserved = []
        self.auth_accounts = []
        self.external_chunks = set()
        self.external_fingerprints = {}
        self.scan_progress = None

    def note(self, which, where, value):
        getattr(self, which).append({"location": where, "value": str(value)[:100]})

    def role(self, table, key, where):
        for rule in self.rules:
            if re.fullmatch(rule["location"], where):
                return rule["role"]
        k = str(key).lower()
        t = table.lower()
        # Plugin object UUIDs and saved group names are not player identities.
        if t == "playerparticles_group" and k in ("uuid", "name"):
            return "preserve"
        if t == "playerparticles_particle" and k in ("uuid", "group_uuid"):
            return "preserve"
        if t == "playerparticles_fixed" and k == "particle_uuid":
            return "preserve"
        if k == "premiumuuid":
            return "premium"
        if PRESERVE.search(k):
            return "preserve"
        # SkinsRestorer source caches identify Mojang profiles, independently of game owners.
        if t.rsplit(":", 1)[-1] in (
            "sr_cache",
            "sr_player_skins",
            "sr_custom_skins",
            "sr_url_skins",
        ):
            return "preserve"
        if "skin" in t and k in (
            "value",
            "skin_identifier",
            "skinid",
            "profile_id",
            "profile_uuid",
        ):
            return "preserve"
        if k in (
            "world",
            "world_id",
            "worlduuid",
            "server_id",
            "server_uuid",
            "session_id",
        ):
            return "preserve"
        if k == "id":
            if t in ("chesscraft_matches", "chesscraft_complete_matches"):
                return "preserve"
            return "uuid" if PLAYER_TABLE.search(t) else "preserve"
        if k in (
            "user",
            "player",
            "owner",
            "creator",
            "buyer",
            "seller",
            "sender",
            "receiver",
            "member",
        ):
            return "identity"
        if k in (
            "operator",
            "driveruuid",
            "activedriveruuid",
            "fastestlapuuid",
            "white",
            "black",
            "white_uuid",
            "black_uuid",
            "white_player_id",
            "black_player_id",
            "player1",
            "player2",
            "inviter_uuid",
            "invited_uuid",
        ):
            return "uuid"
        if UUID_KEYS.fullmatch(k):
            return "uuid"
        if (
            k
            in ("whitelist", "manual-whitelist", "verified-players", "revoked-players")
            and "cargowhitelist" in t
        ):
            return "name"
        if NAME_KEYS.fullmatch(k):
            return (
                "name"
                if k not in ("name", "user") or PLAYER_TABLE.search(t)
                else "unknown"
            )
        return "unknown"

    def raw_uuid(self, value):
        if isinstance(value, bytes):
            return value if len(value) == 16 else None
        if isinstance(value, str) and re.fullmatch(
            r"[0-9a-fA-F]{32}|[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}",
            value,
        ):
            return uuid.UUID(value).bytes
        return None

    def scalar(self, value, role, where):
        if not isinstance(value, (str, bytes)):
            return value
        raw = self.raw_uuid(value)
        if role == "identity":
            role = "uuid" if raw else "name"
        is_uuid = raw == self.raw_old
        is_name = isinstance(value, str) and value.lower() == self.old.lower()
        target = (
            raw == self.raw_new
            or isinstance(value, str)
            and value.lower() == self.new.lower()
        )
        if target and not is_name and role in ("uuid", "name", "unknown"):
            self.note("collisions", where, value)
        if not is_uuid and not is_name:
            return value
        if role in ("preserve", "premium"):
            self.note("preserved", where, value)
            return value
        if is_uuid and role == "uuid":
            out = (
                self.raw_new
                if isinstance(value, bytes)
                else self.new_uuid if "-" in value else self.new_uuid.replace("-", "")
            )
        elif is_name and role == "name":
            out = self.new
        else:
            self.note("unknown", where, value)
            return value
        self.note("changes", where, value)
        return out

    def structured(self, obj, table, where, inherited="unknown"):
        if isinstance(obj, dict):
            out = {}
            merged_keys = set()
            if self.merge:
                pairs = (
                    (self.old, self.new),
                    (self.old_uuid, self.new_uuid),
                    (self.old_uuid.replace("-", ""), self.new_uuid.replace("-", "")),
                )
                for source, target in pairs:
                    if source not in obj or target not in obj or source == target:
                        continue
                    if not isinstance(obj[source], dict) or not isinstance(
                        obj[target], dict
                    ):
                        continue
                    source_value = self.structured(
                        obj[source], table, where + "." + source
                    )
                    target_value = self.structured(
                        obj[target], table, where + "." + target
                    )
                    out[target] = self.merge_maps(source_value, target_value)
                    self.note("changes", where + "#key", source)
                    self.note(
                        "preserved",
                        where + "#merge",
                        "target values retained in reversible before-image",
                    )
                    merged_keys.update((source, target))
            for key, value in obj.items():
                if key in merged_keys:
                    continue
                key_role = (
                    "uuid"
                    if self.raw_uuid(key) in (self.raw_old, self.raw_new)
                    else (
                        "name"
                        if isinstance(key, str)
                        and key.lower() in (self.old.lower(), self.new.lower())
                        else "preserve"
                    )
                )
                new_key = self.scalar(key, key_role, where + "#key")
                if new_key in out:
                    raise ValueError("Structured key collision " + where)
                location = where + "." + str(key)
                role = self.role(table, str(key), location)
                if str(key).lower() in PROFILE_BRANCHES or role in (
                    "preserve",
                    "premium",
                ):
                    out[new_key] = value
                elif isinstance(value, (dict, list)):
                    out[new_key] = self.structured(value, table, location, role)
                else:
                    out[new_key] = self.cell(value, table, str(key), location)
            return out
        if isinstance(obj, list):
            return [
                (
                    self.structured(v, table, where + "[]", inherited)
                    if isinstance(v, (dict, list))
                    else self.scalar(v, inherited, where + "[]")
                )
                for v in obj
            ]
        return self.scalar(obj, inherited, where)

    @staticmethod
    def merge_maps(source, target):
        result = dict(target)
        for key, value in source.items():
            if (
                key in result
                and isinstance(value, dict)
                and isinstance(result[key], dict)
            ):
                result[key] = Identity.merge_maps(value, result[key])
            else:
                result[key] = value
        return result

    def cell(self, value, table, column, where):
        role = self.role(table, column, where)
        if role in ("premium", "preserve"):
            return self.scalar(value, role, where)
        direct = self.scalar(value, role, where)
        if direct != value:
            if (
                role == "name"
                and column.lower() == "username"
                and table.rsplit(":", 1)[-1].lower() == "authme"
                and value == value.lower()
            ):
                return self.new.lower()
            return direct
        if isinstance(value, str) and value.lstrip().startswith(("{", "[")):
            try:
                obj = parse_json(value)
            except (ValueError, TypeError):
                pass
            else:
                out = self.structured(obj, table, where, role)
                return (
                    json.dumps(out, ensure_ascii=False, separators=(",", ":"))
                    if out != obj
                    else value
                )
        if isinstance(value, bytes):
            if value.startswith(b"\x0a\x00"):
                from nbt import convert

                return convert(value, self, where)
            if value.startswith(b"\x1f\x8b"):
                raw = gzip.decompress(value)
                out = self.cell(raw, table, column, where + "#gzip")
                return gzip.compress(out, mtime=0) if out != raw else value
            if value.startswith((b"x\x01", b"x\x9c", b"x\xda")):
                raw = zlib.decompress(value)
                out = self.cell(raw, table, column, where + "#zlib")
                return zlib.compress(out) if out != raw else value
            try:
                text = value.decode("utf8")
            except UnicodeDecodeError:
                if self.raw_old in value or self.raw_new in value:
                    self.note("unknown", where, "opaque embedded binary UUID")
                return value
            out = self.cell(text, table, column, where + "#utf8")
            return out.encode() if out != text else value
        if isinstance(value, str):
            if len(value) > 40 and re.fullmatch(r"[A-Za-z0-9+/=\r\n]+", value):
                try:
                    raw = base64.b64decode(value, validate=True)
                except ValueError:
                    raw = b""
                if raw.startswith((b"\x0a\x00", b"\x1f\x8b")):
                    from nbt import convert_file

                    out = convert_file(raw, self, where + "#base64")
                    return base64.b64encode(out).decode() if out != raw else value
            tokens = re.split(r"([,;|\s]+)", value)
            if (
                role == "uuid"
                and len(tokens) > 1
                and all(
                    not t or re.fullmatch(r"[,;|\s]+", t) or self.raw_uuid(t)
                    for t in tokens
                )
            ):
                return "".join(self.scalar(t, role, where) for t in tokens)
            if (
                self.old_uuid in value.lower()
                or self.old_uuid.replace("-", "") in value.lower()
                or re.search(
                    r"(?<![A-Za-z0-9_])" + re.escape(self.old) + r"(?![A-Za-z0-9_])",
                    value,
                    re.I,
                )
            ):
                self.note(
                    "unknown", where, "embedded identity requires a structured adapter"
                )
        return value
