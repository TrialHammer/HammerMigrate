"""Disposable fixtures for account migration, constraints, and crash recovery."""

import base64
import gzip
import json
import os
from pathlib import Path
import shlex
import shutil
import socket
import sqlite3
import struct
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch as mock_patch
import uuid
import zlib

ENGINE = Path(__file__).resolve().parents[1] / "src/main/resources/engine"
sys.path[:0] = [str(ENGINE), str(ENGINE / "vendor")]
import common
import database
import discovery
import files
import identity
import nbt
import worker

OLD = "TianyaOVO"
NEW = "TianyaQAQ"
OU = common.offline(OLD)
NU = common.offline(NEW)
ORAW = uuid.UUID(OU).bytes
NRAW = uuid.UUID(NU).bytes
JAR = os.environ.get("HAMMERMIGRATE_JAR")


def tag_payload(typ, value):
    if typ == 10:
        return (
            b"".join(
                bytes([t]) + nbt.string_bytes(k) + tag_payload(t, v)
                for k, (t, v) in value.items()
            )
            + b"\0"
        )
    if typ == 8:
        return nbt.string_bytes(value)
    if typ in (7, 11, 12):
        return struct.pack(">i", len(value) // {7: 1, 11: 4, 12: 8}[typ]) + value
    if typ == 9:
        subtype, items = value
        return (
            bytes([subtype])
            + struct.pack(">i", len(items))
            + b"".join(tag_payload(subtype, v) for v in items)
        )
    return struct.pack({1: ">b", 3: ">i", 4: ">q"}[typ], value)


def compound(values):
    return b"\x0a\0\0" + tag_payload(10, values)


def region_data(raw, mode=2):
    payload = nbt.pack_payload(raw, mode)
    body = struct.pack(">I", len(payload) + 1) + bytes([mode]) + payload
    size = (len(body) + 4095) // 4096
    return (
        ((2 << 8) | size).to_bytes(4, "big")
        + bytes(8188)
        + body
        + bytes(size * 4096 - len(body))
    )


class Sandbox(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="hammermigrate-test-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.server = self.root / "s"
        self.server.mkdir()
        self.cfg = {
            "networkRoot": str(self.root),
            "servers": ["s"],
            "databaseEndpoints": [],
            "exclude": ["logs", "backups"],
            "startup": "external",
        }
        self.config = self.root / "config.json"
        common.save(self.config, self.cfg)
        self.ident = identity.Identity(OLD, NEW)
        self.endpoint = {"kind": "sqlite", "path": "s/players.db"}

    def db_fixture(self, cascade=False):
        connection = sqlite3.connect(self.server / "players.db")
        connection.executescript("""
            PRAGMA foreign_keys=ON;
            CREATE TABLE authme(id INTEGER PRIMARY KEY,username TEXT UNIQUE COLLATE NOCASE,realname TEXT,uuid TEXT UNIQUE,premiumUUID TEXT,password TEXT,isLogged INT,hasSession INT);
            CREATE TABLE plan_users(id INTEGER PRIMARY KEY,uuid TEXT UNIQUE,name TEXT);
            CREATE TABLE plan_sessions(id INTEGER PRIMARY KEY,user_id INTEGER REFERENCES plan_users(id),server_uuid TEXT);
            CREATE TABLE players(uuid TEXT PRIMARY KEY,username TEXT UNIQUE,score INT);
        """)
        connection.execute(
            "CREATE TABLE homes(id INTEGER PRIMARY KEY,owner_uuid TEXT REFERENCES players(uuid)"
            + (" ON UPDATE CASCADE" if cascade else "")
            + ",name TEXT)"
        )
        connection.execute(
            "INSERT INTO authme VALUES (1,?,?,?,?,?,?,?)",
            (OLD.lower(), OLD, OU, None, "unchanged-hash", 1, 1),
        )
        connection.execute("INSERT INTO plan_users VALUES (42,?,?)", (OU, OLD))
        connection.execute("INSERT INTO plan_sessions VALUES (7,42,?)", (OU,))
        connection.execute("INSERT INTO players VALUES (?,?,9)", (OU, OLD))
        connection.execute("INSERT INTO homes VALUES (1,?,?)", (OU, "home"))
        connection.commit()
        connection.close()

    def open_db(self, readonly=False):
        db = database.Database(self.cfg, self.endpoint, readonly)
        self.addCleanup(db.close)
        return db

    def test_explicit_merge_structured_identity_maps_and_reverse_patch(self):
        before = {
            OLD: {"score": 10, "homes": {"shared": "old", "old": 1}},
            NEW: {"score": 20, "homes": {"shared": "new", "new": 2}},
        }
        ident = identity.Identity(OLD, NEW, merge=True)
        after = ident.structured(before, "players", "fixture")
        self.assertEqual(
            after, {NEW: {"score": 10, "homes": {"shared": "old", "old": 1, "new": 2}}}
        )
        self.assertFalse(ident.collisions)
        raw_before = json.dumps(before).encode()
        raw_after = json.dumps(after).encode()
        delta = common.patch(raw_before, raw_after)
        self.assertEqual(common.patch_bytes(raw_before, delta), raw_after)
        self.assertEqual(common.patch_bytes(raw_after, delta, True), raw_before)

    def test_merge_does_not_suppress_unhandled_target_collision(self):
        ident = identity.Identity(OLD, NEW, merge=True)
        ident.scalar(NEW, "name", "fixture")
        self.assertTrue(ident.collisions)
        self.assertFalse(
            worker.job_identity(
                self.cfg, {"old": OLD, "new": NEW, "premium": False}
            ).merge
        )
        self.assertTrue(
            worker.job_identity(
                self.cfg, {"old": OLD, "new": NEW, "premium": False, "merge": True}
            ).merge
        )

    def fixture_files(self):
        player = self.server / "world/playerdata" / f"{OU}.dat"
        player.parent.mkdir(parents=True)
        player.write_bytes(
            gzip.compress(
                compound(
                    {"UUID": (11, ORAW), "bukkit": (10, {"lastKnownName": (8, OLD)})}
                )
            )
        )
        (self.server / "usercache.json").write_text(
            json.dumps([{"uuid": OU, "name": OLD}])
        )
        (self.server / "stats").mkdir()
        (self.server / "stats" / f"{OU}.json").write_text('{"stats":{}}')
        (self.server / "world/session.lock").write_bytes(b"\xe2\x98\x83")
        snapshot = (
            self.server / "plugins/InventoryRollbackPlus/backups" / OU / "snapshot.dat"
        )
        snapshot.parent.mkdir(parents=True)
        snapshot.write_bytes(b"opaque historical snapshot " + ORAW)
        return player, snapshot

    def job(self):
        return {
            "id": "fixture",
            "old": OLD,
            "new": NEW,
            "premium": False,
            "status": "queued",
        }

    def test_offline_uuid_and_case(self):
        self.assertEqual(
            OU,
            str(
                uuid.UUID(
                    bytes=__import__("hashlib")
                    .md5(("OfflinePlayer:" + OLD).encode())
                    .digest(),
                    version=3,
                )
            ),
        )
        self.assertNotEqual(OU, common.offline(OLD.lower()))
        with self.assertRaises(ValueError):
            identity.Identity(OLD, OLD)

    def test_case_only_rename_keeps_lowercase_login_key(self):
        ident = identity.Identity(OLD, OLD.lower())
        self.assertNotEqual(ident.old_uuid, ident.new_uuid)
        self.assertEqual(
            ident.cell(OLD.lower(), "authme", "username", "x"), OLD.lower()
        )
        self.assertEqual(ident.cell(OLD, "authme", "realname", "x"), OLD.lower())
        self.assertFalse(ident.collisions)
        ident = identity.Identity("abc", "AbcNew")
        self.assertEqual(ident.cell("abc", "players", "name", "x"), "AbcNew")
        self.assertEqual(ident.cell("abc", "authme", "username", "x"), "abcnew")
        self.assertEqual(ident.cell("abc", "players", "username", "x"), "AbcNew")

    def test_structured_semantics(self):
        value = {
            "uuid": OU,
            "username": OLD,
            "world_uuid": OU,
            "nickname": OLD,
            "members": [OU],
            "profile": {"id": OU, "name": OLD},
            "password": OLD,
            "skin_identifier": OU,
        }
        out = self.ident.structured(value, "players", "fixture")
        self.assertEqual(out["uuid"], NU)
        self.assertEqual(out["username"], NEW)
        for key in ("world_uuid", "nickname", "profile", "password", "skin_identifier"):
            self.assertEqual(out[key], value[key])
        self.assertEqual(out["members"], [NU])
        self.assertFalse(self.ident.unknown)

    def test_skin_source_and_owner(self):
        self.assertEqual(
            self.ident.cell(OU, "sr_player_skins", "uuid", "db:sr_player_skins.uuid"),
            OU,
        )
        self.assertEqual(
            self.ident.cell(OU, "sr_players", "uuid", "db:sr_players.uuid"), NU
        )
        self.assertEqual(
            self.ident.cell(
                OU, "sr_players", "skin_identifier", "db:sr_players.skin_identifier"
            ),
            OU,
        )

    def test_uuid_shapes(self):
        for value in (OU.upper(), OU.replace("-", ""), ORAW):
            self.assertNotEqual(self.ident.scalar(value, "uuid", "x"), value)
        self.assertEqual(
            self.ident.cell(OU + ";" + OU, "players", "members", "x"), NU + ";" + NU
        )

    def test_unknown_and_target_collision(self):
        self.ident.cell("text " + OLD, "plugin", "mystery", "x")
        self.ident.cell(NEW.lower(), "players", "username", "x")
        self.assertTrue(self.ident.unknown)
        self.assertTrue(self.ident.collisions)

    def test_compressed_and_base64_nbt(self):
        raw = compound({"Owner": (11, ORAW)})
        for value in (
            raw,
            gzip.compress(raw),
            zlib.compress(raw),
            base64.b64encode(gzip.compress(raw)).decode(),
        ):
            out = self.ident.cell(value, "players", "data", "x")
            self.assertNotEqual(out, value)
        self.assertFalse(self.ident.unknown)

    def test_nbt_ownership_and_profile(self):
        raw = compound(
            {
                "UUID": (11, ORAW),
                "Owner": (11, ORAW),
                "OwnerUUIDMost": (4, int.from_bytes(ORAW[:8], "big", signed=True)),
                "OwnerUUIDLeast": (4, int.from_bytes(ORAW[8:], "big", signed=True)),
                "Trusted": (9, (11, [ORAW])),
                "SkullOwner": (10, {"Id": (11, ORAW), "Name": (8, OLD)}),
                "bukkit": (10, {"lastKnownName": (8, OLD)}),
            }
        )
        out = (
            nbt.Reader(nbt.convert(raw, self.ident, "world/entities/r.0.0.mca"))
            .root()
            .value
        )
        self.assertEqual(out["UUID"].value, ORAW)
        self.assertEqual(out["Owner"].value, NRAW)
        self.assertEqual(out["Trusted"].value[0].value, NRAW)
        self.assertEqual(out["SkullOwner"].value["Id"].value, ORAW)
        self.assertEqual(out["bukkit"].value["lastKnownName"].value, NEW)
        self.assertEqual(
            out["OwnerUUIDMost"].value, int.from_bytes(NRAW[:8], "big", signed=True)
        )
        self.assertFalse(self.ident.unknown)

    def test_nbt_numeric_list_and_compound_keys(self):
        values = [
            int.from_bytes(ORAW[i : i + 4], "big", signed=True) for i in range(0, 16, 4)
        ]
        raw = compound(
            {"Owner": (9, (3, values)), OU: (10, {"username": (8, "irrelevant")})}
        )
        out = nbt.Reader(nbt.convert(raw, self.ident, "world/data.dat")).root().value
        self.assertIn(NU, out)
        self.assertEqual(
            b"".join(
                (v.value & 0xFFFFFFFF).to_bytes(4, "big") for v in out["Owner"].value
            ),
            NRAW,
        )

    def test_player_root_uuid(self):
        raw = compound({"UUID": (11, ORAW)})
        self.assertIn(
            NRAW, nbt.convert(raw, self.ident, "world/playerdata/" + OU + ".dat")
        )

    def test_anvil_and_inverse(self):
        raw = region_data(compound({"Owner": (11, ORAW)}))
        out, external = nbt.region(raw, self.server / "r.0.0.mca", self.ident)
        self.assertFalse(external)
        change = files.sparse_patch(raw, out)
        self.assertEqual(common.patch_bytes(raw, change), out)
        self.assertEqual(common.patch_bytes(out, change, True), raw)
        self.assertEqual(out[:8192], raw[:8192])

    def test_region_reallocation(self):
        # An uncompressed chunk exactly fills one sector, then a longer username grows it.
        ident = identity.Identity("Abc", "AReallyLongName")
        base = compound(
            {"bukkit": (10, {"lastKnownName": (8, "Abc")}), "padding": (7, b"")}
        )
        raw_nbt = compound(
            {
                "bukkit": (10, {"lastKnownName": (8, "Abc")}),
                "padding": (7, b"\xff" * (4091 - len(base))),
            }
        )
        raw = region_data(raw_nbt, 3)
        out, _ = nbt.region(raw, self.server / "r.0.0.mca", ident)
        self.assertGreater(len(out), len(raw))
        self.assertEqual(
            common.patch_bytes(out, files.sparse_patch(raw, out), True), raw
        )

    def test_external_chunk(self):
        p = self.server / "r.-1.2.mca"
        external = self.server / "c.-32.64.mcc"
        external.write_bytes(zlib.compress(compound({"Owner": (11, ORAW)})))
        raw = (
            ((2 << 8) | 1).to_bytes(4, "big")
            + bytes(8188)
            + b"\0\0\0\1\x82"
            + bytes(4091)
        )
        out, changes = nbt.region(raw, p, self.ident)
        self.assertEqual(out, raw)
        self.assertEqual(changes[0][0], external)
        self.assertIn(NRAW, zlib.decompress(changes[0][2]))

    def test_broken_region_blocks(self):
        with self.assertRaises(ValueError):
            nbt.region(bytes(40), self.server / "r.0.0.mca", self.ident)

    def test_yaml_comments_preserved(self):
        raw = (
            "# important\nusername: "
            + OLD
            + " # retain\nuuid: "
            + OU
            + "\nencodeKey: 123\n"
        ).encode()
        out, _ = files.transform(self.server / "players.yml", "yaml", raw, self.ident)
        self.assertIn(b"# important", out)
        self.assertIn(b"# retain", out)
        self.assertIn(b"encodeKey: 123", out)
        self.assertIn(NEW.encode(), out)

    def test_csv_properties_and_toml(self):
        for kind, text in [
            ("csv", "username,uuid\n" + OLD + "," + OU + "\n"),
            ("properties", "username=" + OLD + "\nuuid=" + OU + "\n"),
            ("toml", '# comment\nusername="' + OLD + '"\nuuid="' + OU + '"\n'),
        ]:
            out, _ = files.transform(
                self.server / ("players." + kind), kind, text.encode(), self.ident
            )
            self.assertIn(NEW.encode(), out)
            self.assertIn(NU.encode(), out)

    def test_shopkeepers_csv_metadata_preamble(self):
        raw = (
            ",\n\"player_uuid\",\"player_name\",\"note\"\n"
            + '"' + OU + '","' + OLD + '","keep, quoted"\n'
        ).encode()
        out, _ = files.transform(self.server / "trades.csv", "csv", raw, self.ident)
        self.assertTrue(out.startswith(b",\n"))
        self.assertIn(NU.encode(), out)
        self.assertIn(NEW.encode(), out)
        self.assertIn(b'"keep, quoted"', out)

    def test_sqlite_real_constraint_copy_and_rollback(self):
        self.db_fixture()
        db = self.open_db()
        ops, inventory = db.plan(self.ident)
        self.assertFalse(self.ident.unknown)
        db.validate(ops)
        db.mutate(ops)
        self.assertEqual(
            db.execute("SELECT uuid,name,id FROM plan_users")["rows"],
            [{"uuid": NU, "name": NEW, "id": 42}],
        )
        self.assertEqual(
            db.execute("SELECT server_uuid FROM plan_sessions")["rows"][0][
                "server_uuid"
            ],
            OU,
        )
        self.assertEqual(
            db.execute("SELECT password,isLogged,hasSession FROM authme")["rows"][0],
            {"password": "unchanged-hash", "isLogged": 0, "hasSession": 0},
        )
        self.assertEqual(
            db.execute("SELECT owner_uuid FROM homes")["rows"][0]["owner_uuid"], NU
        )
        db.mutate(ops)  # idempotent recovery after commit
        db.mutate(ops, True)
        db.mutate(ops, True)
        self.assertEqual(db.execute("SELECT uuid FROM players")["rows"][0]["uuid"], OU)

    def test_sqlite_cascade_child_pk(self):
        self.db_fixture(True)
        connection = sqlite3.connect(self.server / "players.db")
        connection.executescript(
            "CREATE TABLE player_members(owner_uuid TEXT PRIMARY KEY REFERENCES players(uuid) ON UPDATE CASCADE,username TEXT);"
        )
        connection.execute("INSERT INTO player_members VALUES (?,?)", (OU, OLD))
        connection.commit()
        connection.close()
        db = self.open_db()
        ops, _ = db.plan(self.ident)
        db.validate(ops)
        db.mutate(ops)
        db.mutate(ops, True)
        self.assertEqual(
            db.execute("SELECT owner_uuid FROM player_members")["rows"][0][
                "owner_uuid"
            ],
            OU,
        )

    def test_sqlite_rowid_and_trchat_user(self):
        db = self.open_db() if (self.server / "players.db").exists() else None
        connection = sqlite3.connect(self.server / "players.db")
        connection.executescript("CREATE TABLE TrChat(user TEXT,key TEXT,value TEXT);")
        connection.execute(
            "INSERT INTO TrChat VALUES (?,?,?)", (OU, "preferences", "hello")
        )
        connection.commit()
        connection.close()
        db = self.open_db()
        ops, _ = db.plan(self.ident)
        self.assertFalse(self.ident.unknown)
        self.assertEqual(ops[0]["pk"], ["__hm_rowid"])
        db.validate(ops)
        db.mutate(ops)
        db.mutate(ops, True)
        self.assertEqual(db.execute("SELECT user FROM TrChat")["rows"][0]["user"], OU)

    def test_discovery_respects_storage_switches_and_aliases(self):
        configs = {
            "inactive.yml": "database:\n  type: SQLite\n  host: localhost\n  database: unused\n  user: test\n  password: secret\n",
            "lands.yml": "database:\n  mysql-v2:\n    enabled_19: true\n    address_2: localhost\n    port_2: 3306\n    database_2: lands\n    username_2: test\n    password_2: secret\n",
            "points.yml": "mysql-settings:\n  enabled: true\n  hostname: localhost\n  database-name: points\n  user-name: test\n  user-password: secret\n",
            "korra.yml": "Storage:\n  engine: mysql\n  MySQL:\n    host: localhost\n    db: korra\n    user: test\n    pass: secret\n",
        }
        for name, text in configs.items():
            (self.server / name).write_text(text)
        result = discovery.configs(self.cfg)
        self.assertEqual(
            {e["database"] for e in result["endpoints"]}, {"lands", "points", "korra"}
        )

    def test_target_case_collision(self):
        self.db_fixture()
        db = self.open_db()
        db.execute(
            "INSERT INTO players VALUES (?,?,1)", (str(uuid.uuid4()), NEW.lower())
        )
        db.commit()
        db.plan(self.ident)
        self.assertTrue(self.ident.collisions)

    def test_authme_case_mismatch_blocks(self):
        self.db_fixture()
        db = self.open_db()
        db.execute("UPDATE authme SET realname=?", (OLD.lower(),))
        db.commit()
        with self.assertRaisesRegex(ValueError, "casing"):
            db.plan(self.ident)

    def test_premium_binding_and_cache(self):
        self.db_fixture()
        self.ident.premium = True
        self.ident.premium_uuid = str(uuid.uuid4())
        db = self.open_db()
        ops, _ = db.plan(self.ident)
        auth = next(o for o in ops if o["table"] == "authme")
        self.assertEqual(auth["after"]["uuid"], NU)
        self.assertEqual(auth["after"]["premiumUUID"], self.ident.premium_uuid)
        out, _ = files.transform(
            self.server / "premium_names.cache", "text-cache", b"other\n", self.ident
        )
        self.assertIn(NEW.lower().encode(), out)
        db.execute("UPDATE authme SET premiumUUID=?", (str(uuid.uuid4()),))
        db.commit()
        with self.assertRaisesRegex(ValueError, "another premium"):
            db.plan(self.ident)

    def test_premium_off_clears_binding(self):
        self.db_fixture()
        db = self.open_db()
        db.execute("UPDATE authme SET premiumUUID=?", (str(uuid.uuid4()),))
        db.commit()
        ops, _ = db.plan(self.ident)
        self.assertIsNone(
            next(o for o in ops if o["table"] == "authme")["after"]["premiumUUID"]
        )
        out, _ = files.transform(
            self.server / "premium_names.cache",
            "text-cache",
            (OLD.lower() + "\nother\n").encode(),
            self.ident,
        )
        self.assertEqual(out, b"other\n")

    def test_database_cas_rejects_edit(self):
        self.db_fixture()
        db = self.open_db()
        ops, _ = db.plan(self.ident)
        db.execute("UPDATE players SET score=99")
        db.commit()
        with self.assertRaisesRegex(ValueError, "Row changed"):
            db.mutate(ops)
        db.rollback()
        self.assertEqual(db.execute("SELECT uuid FROM players")["rows"][0]["uuid"], OU)

    def test_unique_collision_preflight(self):
        self.db_fixture()
        db = self.open_db()
        ops, _ = db.plan(self.ident)
        db.execute("INSERT INTO players VALUES (?,?,0)", (NU, "OtherName"))
        db.commit()
        with self.assertRaisesRegex(ValueError, "unique"):
            db.validate(ops)

    def test_discover_deduplicates_endpoints(self):
        secret = "fixture-secret"
        (self.server / "config.yml").write_text(
            "data:\n  host: localhost\n  port: 33119\n  database: fixture\n  username: test\n  password: "
            + secret
            + "\n"
        )
        (self.server / "other.yml").write_text((self.server / "config.yml").read_text())
        manifest = discovery.configs(self.cfg)
        self.assertEqual(len(manifest["endpoints"]), 1)
        self.assertNotIn(secret, common.dumps(manifest))

    def test_ignored_stale_fastlogin_endpoint_is_audited_without_connecting(self):
        folder = self.server / "plugins/FastLogin"
        folder.mkdir(parents=True)
        (folder / "config.yml").write_text(
            "mysql:\n  host: minecraft-db\n  port: 3306\n  database: fastlogin\n  username: legacy\n  password: stale-secret\n"
        )
        self.cfg["ignoredDatabaseEndpoints"] = [
            {
                "host": "minecraft-db",
                "port": 3306,
                "database": "fastlogin",
                "reason": "FastLogin database was removed; stale plugin configuration",
            }
        ]
        manifest = discovery.configs(self.cfg)
        self.assertFalse(manifest["endpoints"])
        skipped = next(item for item in manifest["skipped"] if "endpoint" in item)
        self.assertEqual(skipped["endpoint"], {"host": "minecraft-db", "port": 3306, "database": "fastlogin"})
        self.assertIn("database was removed", skipped["reason"])
        self.assertNotIn("stale-secret", common.dumps(manifest))

    def test_stale_database_ignore_does_not_match_other_endpoint(self):
        folder = self.server / "plugins/FastLogin"
        folder.mkdir(parents=True)
        (folder / "config.yml").write_text(
            "mysql:\n  host: minecraft-db\n  port: 3306\n  database: fastlogin_new\n  username: legacy\n  password: stale-secret\n"
        )
        self.cfg["ignoredDatabaseEndpoints"] = [
            {"host": "minecraft-db", "port": 3306, "database": "fastlogin", "reason": "removed"}
        ]
        manifest = discovery.configs(self.cfg)
        self.assertEqual([item["database"] for item in manifest["endpoints"]], ["fastlogin_new"])

    def test_listener_prevents_migration(self):
        sock = socket.socket()
        sock.bind(("127.0.0.1", 0))
        sock.listen()
        self.addCleanup(sock.close)
        with self.assertRaisesRegex(RuntimeError, "listening"):
            discovery.quiescent(
                self.cfg,
                {"listeners": [{"server": "s", "port": sock.getsockname()[1]}]},
            )

    def test_file_cas_and_torn_journal(self):
        change = common.patch(b"old data", b"new data")
        with self.assertRaises(ValueError):
            common.patch_bytes(b"edited", change)
        journal = common.Journal(self.root / "journal.jsonl")
        journal.append("started")
        with journal.path.open("ab") as handle:
            handle.write(b'{"event":')
        self.assertEqual(len(journal.events()), 1)
        journal.append("recovered")
        self.assertEqual(len(journal.events()), 2)

    def test_manifest_apply_and_full_logical_rollback(self):
        self.db_fixture()
        player, snapshot = self.fixture_files()
        original = {
            str(p.relative_to(self.server)): p.read_bytes()
            for p in self.server.rglob("*")
            if p.is_file() and p.suffix != ".db"
        }
        folder = self.root / "run"
        report, plan = worker.discover_plan(self.cfg, self.job(), folder)
        self.assertTrue(report["ready"], common.dumps(report))
        worker.apply_run(self.cfg, folder, plan)
        self.assertFalse(player.exists())
        self.assertTrue(player.with_name(NU + ".dat").exists())
        moved_snapshot = snapshot.parent.with_name(NU) / snapshot.name
        self.assertEqual(
            moved_snapshot.read_bytes(),
            original[str(snapshot.relative_to(self.server))],
        )
        worker.rollback_run(self.cfg, folder)
        restored = {
            str(p.relative_to(self.server)): p.read_bytes()
            for p in self.server.rglob("*")
            if p.is_file() and p.suffix != ".db"
        }
        self.assertEqual(original, restored)
        connection = sqlite3.connect(self.server / "players.db")
        self.assertEqual(
            connection.execute("SELECT uuid FROM players").fetchone()[0], OU
        )
        connection.close()

    def test_crash_after_intent_and_after_mutation(self):
        self.fixture_files()
        folder = self.root / "run"
        report, plan = worker.discover_plan(self.cfg, self.job(), folder)
        self.assertTrue(report["ready"])
        journal = common.Journal(folder / "journal.jsonl")
        journal.append("started")
        journal.append("intent", index=0)
        worker.rollback_run(self.cfg, folder)  # intent durable but action never ran
        folder = self.root / "run2"
        report, plan = worker.discover_plan(self.cfg, self.job(), folder)
        journal = common.Journal(folder / "journal.jsonl")
        journal.append("started")
        journal.append("intent", index=0)
        worker.perform(self.cfg, plan["actions"][0])
        worker.rollback_run(self.cfg, folder)  # action ran but no completion record
        self.assertIn(OLD, (self.server / "usercache.json").read_text())

    def test_failed_job_holds_startup_and_can_retry(self):
        job = self.job()
        job["status"] = "failed"
        worker.setjobs(self.cfg, [job])
        with self.assertRaisesRegex(ValueError, "Failed jobs"):
            worker.process(self.cfg)
        worker.main(["--config", str(self.config), "retry", "fixture"])
        self.assertEqual(
            [j["status"] for j in worker.jobs(self.cfg)], ["retried", "queued"]
        )

    def test_queue_and_external_startup_gate(self):
        worker.main(["--config", str(self.config), "queue", OLD, NEW])
        with self.assertRaisesRegex(ValueError, "overlaps"):
            worker.main(["--config", str(self.config), "queue", NEW, "OtherName"])
        with self.assertRaisesRegex(ValueError, "pre-start"):
            worker.main(
                ["--config", str(self.config), "startup", "--server", str(self.server)]
            )


@unittest.skipUnless(
    JAR, "Set HAMMERMIGRATE_JAR after packaging for JDBC and Java codec tests"
)
class PackagedTests(unittest.TestCase):
    fixture_files = Sandbox.fixture_files

    def setUp(self):
        Sandbox.setUp(self)
        database.JAR = nbt.JAR = Path(JAR).resolve()

    def create_jdbc(self, kind, relative="embedded", user="SA"):
        base = self.server / relative
        base.parent.mkdir(parents=True, exist_ok=True)
        url = ("jdbc:h2:file:" if kind == "h2" else "jdbc:hsqldb:file:") + str(base)
        process = subprocess.Popen(
            ["java", "-cp", str(database.JAR), "ink.ziip.hammer.migrate.JdbcBridge"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        def call(request):
            process.stdin.write(common.dumps(request) + "\n")
            process.stdin.flush()
            response = json.loads(process.stdout.readline())
            if not response.get("ok"):
                raise AssertionError(response)
            return response

        call({"url": url, "user": user, "password": ""})
        call(
            {
                "action": "sql",
                "sql": "CREATE TABLE PLAYERS(UUID VARCHAR(36) PRIMARY KEY, USERNAME VARCHAR(16) UNIQUE)",
                "params": [],
            }
        )
        call(
            {
                "action": "sql",
                "sql": "INSERT INTO PLAYERS VALUES (?,?)",
                "params": [OU, OLD],
            }
        )
        call({"action": "commit"})
        call({"action": "close"})
        process.stdin.close()
        process.wait(timeout=15)
        error = process.stderr.read()
        process.stdout.close()
        process.stderr.close()
        self.assertEqual(process.returncode, 0, error)
        return {
            "kind": kind,
            "path": "s/" + relative + (".mv.db" if kind == "h2" else ".properties"),
        }

    def test_h2_migration_copy_and_rollback(self):
        self.jdbc_case("h2")

    def chesscraft_fixture(self):
        endpoint = self.create_jdbc("h2", "plugins/ChessCraft/database", "")
        db = database.Database(self.cfg, endpoint)
        try:
            db.execute("DROP TABLE PLAYERS")
            db.execute(
                "CREATE TABLE CHESSCRAFT_PLAYERS(ID UUID PRIMARY KEY, USERNAME VARCHAR(30) NOT NULL, DISPLAYNAME VARCHAR(100), RATING INT, PEAK_RATING INT, RATED_MATCHES INT)"
            )
            db.execute(
                "CREATE TABLE CHESSCRAFT_MATCHES(ID UUID PRIMARY KEY, WHITE_PLAYER_ID UUID REFERENCES CHESSCRAFT_PLAYERS(ID), BLACK_PLAYER_ID UUID REFERENCES CHESSCRAFT_PLAYERS(ID), MOVES VARCHAR(100))"
            )
            db.execute(
                "CREATE TABLE CHESSCRAFT_COMPLETE_MATCHES(ID UUID PRIMARY KEY REFERENCES CHESSCRAFT_MATCHES(ID), RESULT_TYPE VARCHAR(16))"
            )
            db.execute(
                "INSERT INTO CHESSCRAFT_PLAYERS VALUES (?,?,?,1200,1400,6)",
                [OU, OLD, OLD],
            )
            db.execute(
                "INSERT INTO CHESSCRAFT_MATCHES VALUES (?,?,?,'e2e4')", [OU, OU, OU]
            )
            db.execute("INSERT INTO CHESSCRAFT_COMPLETE_MATCHES VALUES (?,'WIN')", [OU])
            db.commit()
        finally:
            db.close()
        return endpoint

    def test_chesscraft_restrict_foreign_keys_and_rollback(self):
        endpoint = self.chesscraft_fixture()
        db = database.Database(self.cfg, endpoint, True)
        try:
            ops, _ = db.plan(self.ident)
            self.assertFalse(self.ident.unknown)
            self.assertFalse(self.ident.collisions)
            self.assertEqual(len(ops), 2)
            db.validate(ops)
        finally:
            db.close()
        db = database.Database(self.cfg, endpoint)
        try:
            db.mutate(ops)
            self.assertEqual(
                db.execute("SELECT * FROM CHESSCRAFT_PLAYERS")["rows"],
                [
                    {
                        "ID": NU,
                        "USERNAME": NEW,
                        "DISPLAYNAME": OLD,
                        "RATING": 1200,
                        "PEAK_RATING": 1400,
                        "RATED_MATCHES": 6,
                    }
                ],
            )
            self.assertEqual(
                db.execute("SELECT * FROM CHESSCRAFT_MATCHES")["rows"],
                [
                    {
                        "ID": OU,
                        "WHITE_PLAYER_ID": NU,
                        "BLACK_PLAYER_ID": NU,
                        "MOVES": "e2e4",
                    }
                ],
            )
            self.assertEqual(
                db.execute("SELECT * FROM CHESSCRAFT_COMPLETE_MATCHES")["rows"],
                [{"ID": OU, "RESULT_TYPE": "WIN"}],
            )
            db.mutate(ops)  # Replay after commit before journal completion.
            db.mutate(ops, True)
            self.assertEqual(
                db.execute("SELECT ID,USERNAME FROM CHESSCRAFT_PLAYERS")["rows"],
                [{"ID": OU, "USERNAME": OLD}],
            )
            self.assertEqual(
                db.execute("SELECT WHITE_PLAYER_ID FROM CHESSCRAFT_MATCHES")["rows"],
                [{"WHITE_PLAYER_ID": OU}],
            )
        finally:
            db.close()

    def test_chesscraft_staged_failure_and_unplanned_cascade_compensate(self):
        endpoint = self.chesscraft_fixture()
        db = database.Database(self.cfg, endpoint)
        try:
            ops, _ = db.plan(self.ident)
            actual = db.execute

            def fail_update(sql, params=()):
                if sql.startswith('UPDATE "CHESSCRAFT_MATCHES"'):
                    raise OSError("injected child failure")
                return actual(sql, params)

            with mock_patch.object(db, "execute", side_effect=fail_update):
                with self.assertRaisesRegex(OSError, "injected"):
                    db.mutate(ops)
            self.assertEqual(
                db.execute("SELECT ID FROM CHESSCRAFT_PLAYERS")["rows"], [{"ID": OU}]
            )
            db.execute(
                "CREATE TABLE EXTRA_HISTORY(ID INT PRIMARY KEY, OWNER_UUID UUID REFERENCES CHESSCRAFT_PLAYERS(ID) ON DELETE CASCADE)"
            )
            db.execute("INSERT INTO EXTRA_HISTORY VALUES (1,?)", [OU])
            db.commit()
            with self.assertRaisesRegex(ValueError, "Unmigrated references"):
                db.mutate(ops)
            self.assertEqual(
                db.execute("SELECT OWNER_UUID FROM EXTRA_HISTORY")["rows"],
                [{"OWNER_UUID": OU}],
            )
            self.assertEqual(
                db.execute("SELECT ID FROM CHESSCRAFT_PLAYERS")["rows"], [{"ID": OU}]
            )
            self.assertEqual(
                db.execute("SELECT WHITE_PLAYER_ID FROM CHESSCRAFT_MATCHES")["rows"],
                [{"WHITE_PLAYER_ID": OU}],
            )
        finally:
            db.close()

    def test_libertybans_uuid_generated_unique_locator(self):
        endpoint = self.create_jdbc("hsqldb")
        db = database.Database(self.cfg, endpoint)
        try:
            db.execute(
                'CREATE TABLE "libertybans_names"("uuid" UUID NOT NULL,"name" VARCHAR(16) NOT NULL,"lower_name" VARCHAR(16) GENERATED ALWAYS AS (LOWER("name")), UNIQUE("uuid","name"))'
            )
            db.execute(
                'INSERT INTO "libertybans_names"("uuid","name") VALUES (?,?)', [OU, OLD]
            )
            db.commit()
            db.close()
            db = database.Database(self.cfg, endpoint, True)
            ops, _ = db.plan(self.ident)
            db.validate(ops)
            db.close()
            db = database.Database(self.cfg, endpoint)
            db.mutate(ops)
            row = db.execute('SELECT * FROM "libertybans_names"')["rows"][0]
            self.assertEqual(row, {"uuid": NU, "name": NEW, "lower_name": NEW.lower()})
            db.mutate(ops, True)
            self.assertEqual(
                db.execute('SELECT "uuid" FROM "libertybans_names"')["rows"][0]["uuid"],
                OU,
            )
        finally:
            db.close()

    def test_hsqldb_migration_copy_and_rollback(self):
        self.jdbc_case("hsqldb")

    def jdbc_case(self, kind):
        endpoint = self.create_jdbc(kind)
        db = database.Database(self.cfg, endpoint, True)
        try:
            ops, _ = db.plan(self.ident)
            self.assertEqual(len(ops), 1)
            db.validate(ops)
        finally:
            db.close()
        db = database.Database(self.cfg, endpoint)
        try:
            db.mutate(ops)
            self.assertEqual(
                db.execute("SELECT UUID FROM PLAYERS")["rows"][0]["UUID"], NU
            )
            db.mutate(ops, True)
            self.assertEqual(
                db.execute("SELECT UUID FROM PLAYERS")["rows"][0]["UUID"], OU
            )
        finally:
            db.close()

    def test_lz4_region(self):
        raw = region_data(compound({"Owner": (11, ORAW)}), 4)
        out, _ = nbt.region(raw, self.server / "r.0.0.mca", self.ident)
        self.assertNotEqual(raw, out)
        self.assertEqual(nbt.lz4(nbt.lz4(b"fixture" * 4096, True)), b"fixture" * 4096)

    def test_packaged_shared_config_selection(self):
        source = self.root / "ConfigurationProbe.java"
        source.write_text("""package ink.ziip.hammer.migrate;
import java.nio.file.Path;
public class ConfigurationProbe {
  public static void main(String[] args) throws Exception {
    System.out.println(args.length > 1 ? Main.queuePath(Path.of(args[1])) : Main.configuration(Path.of(args[0])));
  }
}""")
        classes = self.root / "probe"
        subprocess.run(["javac","-cp",str(database.JAR),"-d",str(classes),str(source)],check=True)
        shared = self.root / "shared.json"
        shared.write_text("{}")
        base = ["java","-cp",str(classes)+":"+str(database.JAR),"ink.ziip.hammer.migrate.ConfigurationProbe",str(shared)]
        result = subprocess.run(base,cwd=self.server,capture_output=True,text=True,check=True)
        self.assertEqual(Path(result.stdout.strip()),shared)
        shared.unlink()
        result = subprocess.run(base,cwd=self.server,capture_output=True,text=True,check=True)
        self.assertEqual(Path(result.stdout.strip()),self.server/"plugins/HammerMigrate/config.json")
        result = subprocess.run([base[0],"-Dhammermigrate.config="+str(self.config),*base[1:]],capture_output=True,text=True,check=True)
        self.assertEqual(Path(result.stdout.strip()),self.config)
        self.cfg["stateDirectory"] = "state"
        common.save(self.config,self.cfg)
        result = subprocess.run(base+[str(self.config)],capture_output=True,text=True,check=True)
        self.assertEqual(Path(result.stdout.strip()),self.root/"state/queue.json")
        self.cfg.pop("stateDirectory")
        common.save(self.config,self.cfg)
        result = subprocess.run(base+[str(self.config)],capture_output=True,text=True,check=True)
        self.assertEqual(Path(result.stdout.strip()),self.root/"migrate/HammerMigrate/queue.json")

    def test_packaged_cli(self):
        self.fixture_files()
        runtime = self.root / "python runtime" / "python"
        runtime.parent.mkdir()
        runtime.write_text("#!/bin/sh\nexec " + shlex.quote(sys.executable) + ' "$@"\n')
        runtime.chmod(0o755)
        self.cfg["python"] = "python runtime/python"
        common.save(self.config, self.cfg)
        binaries = self.root / "binaries"
        binaries.mkdir()
        (binaries / "java").symlink_to(Path(shutil.which("java")).resolve())
        environment = dict(os.environ, PATH=str(binaries))
        command = ["java", "-jar", str(database.JAR), "--config", str(self.config)]
        for args in (
            ["dry-run", OLD, NEW],
            ["queue", OLD, NEW],
            ["dry-run", OLD, NEW],
            ["progress"],
            ["run"],
        ):
            result = subprocess.run(
                command + args, capture_output=True, text=True, env=environment
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        job = worker.jobs(self.cfg)[0]
        for args in (["rollback", job["id"]], ["run"]):
            result = subprocess.run(
                command + args, capture_output=True, text=True, env=environment
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn(OLD, (self.server / "usercache.json").read_text())


if __name__ == "__main__":
    unittest.main()
