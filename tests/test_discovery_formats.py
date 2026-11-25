"""Connection configuration formats and explicit completeness semantics."""

import json
import sqlite3
import unittest
from unittest.mock import patch

import test_engine as fixtures
from test_engine import OLD, NEW, OU, NU
import database
import discovery
import identity
import worker


class DiscoveryFormatTests(unittest.TestCase):
    job = fixtures.Sandbox.job

    def setUp(self):
        fixtures.Sandbox.setUp(self)

    def test_json_toml_and_properties_credentials_stay_references(self):
        plugin = self.server / "plugins/Example"
        plugin.mkdir(parents=True)
        documents = {
            "config.json": json.dumps(
                {
                    "mysql": dict(
                        host="localhost",
                        database="db_json",
                        user="reader",
                        password="hidden",
                    )
                }
            ),
            "storage.toml": '[mysql]\nhost="localhost"\ndatabase="db_toml"\nuser="reader"\npassword="hidden"\n',
            "database.properties": "mysql.host=localhost\nmysql.database=db_props\nmysql.user=reader\nmysql.password=hidden\n",
        }
        for name, text in documents.items():
            (plugin / name).write_text(text)
        report = discovery.configs(self.cfg)
        self.assertEqual(
            {r["database"] for r in report["endpoints"]},
            {"db_json", "db_toml", "db_props"},
        )
        self.assertNotIn("hidden", json.dumps(report))
        for endpoint in report["endpoints"]:
            self.assertEqual(
                discovery.credentials(self.cfg, endpoint), ("reader", "hidden")
            )

    def test_ignored_region_is_accounted_but_not_complete(self):
        (self.server / "usercache.json").write_text(
            json.dumps([dict(name=OLD, uuid=OU)])
        )
        (self.server / "r.0.0.mca").write_bytes(b"invalid region")
        report, _ = worker.discover_plan(self.cfg, self.job(), self.root / "run")
        self.assertTrue(report["ready"])  # Preserve the explicit legacy ignore policy.
        self.assertEqual(report["completion"], "with-exclusions")
        self.assertFalse(report["coverage"]["complete"])
        self.assertTrue(report["coverage"]["accounted"])
        self.cfg["requireCompleteCoverage"] = True
        report, _ = worker.discover_plan(self.cfg, self.job(), self.root / "strict")
        self.assertFalse(report["ready"])
        self.assertEqual(report["completion"], "blocked")

    def test_ignored_sqlite_open_is_never_reported_as_complete(self):
        (self.server / "usercache.json").write_text(
            json.dumps([dict(name=OLD, uuid=OU)])
        )
        path = self.server / "test.db"
        with sqlite3.connect(path) as db:
            db.execute("CREATE TABLE example(value TEXT)")
        actual = worker.Database

        def fail(cfg, ep, *args, **kwargs):
            if ep["kind"] == "sqlite":
                raise OSError("cannot open fixture")
            return actual(cfg, ep, *args, **kwargs)

        with patch.object(worker, "Database", side_effect=fail):
            report, _ = worker.discover_plan(self.cfg, self.job(), self.root / "run")
        self.assertTrue(report["ready"])
        self.assertFalse(report["coverage"]["complete"])
        self.assertEqual(report["coverage"]["ignoredFiles"], ["s/test.db"])

    def test_foreign_destination_name_is_never_silently_merged(self):
        path = self.server / "players.db"
        with sqlite3.connect(path) as db:
            db.execute(
                "CREATE TABLE playerpoints_username_cache(uuid TEXT PRIMARY KEY,username TEXT)"
            )
            db.execute("INSERT INTO playerpoints_username_cache VALUES(?,?)", (OU, OLD))
            db.execute(
                "INSERT INTO playerpoints_username_cache VALUES(?,?)",
                ("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee", NEW),
            )
        db = database.Database(self.cfg, self.endpoint)
        try:
            with self.assertRaisesRegex(ValueError, "another UUID"):
                db.plan(identity.Identity(OLD, NEW, merge=True))
        finally:
            db.close()

    def test_custom_particle_prefix_preserves_object_ids_and_reverses(self):
        path = self.server / "players.db"
        with sqlite3.connect(path) as db:
            db.execute(
                "CREATE TABLE custom_group(uuid TEXT PRIMARY KEY,owner_uuid TEXT,name TEXT)"
            )
            db.execute(
                "CREATE TABLE custom_particle(uuid TEXT PRIMARY KEY,group_uuid TEXT)"
            )
            db.execute("INSERT INTO custom_group VALUES(?,?,?)", (OU, OU, "active"))
            db.execute("INSERT INTO custom_particle VALUES(?,?)", (NU, OU))
        db = database.Database(
            self.cfg,
            dict(self.endpoint, plugin="PlayerParticles", tablePrefix="custom_"),
        )
        try:
            ident = identity.Identity(OLD, NEW)
            ops, inventory = db.plan(ident)
            self.assertFalse(ident.collisions)
            self.assertFalse(ident.unknown)
            self.assertEqual(len(ops), 1)
            db.validate(ops)
            db.mutate(ops)
            self.assertEqual(
                db.execute("SELECT * FROM custom_group")["rows"],
                [dict(uuid=OU, owner_uuid=NU, name="active")],
            )
            self.assertEqual(
                db.execute("SELECT * FROM custom_particle")["rows"],
                [dict(uuid=NU, group_uuid=OU)],
            )
            db.mutate(ops, True)
        finally:
            db.close()

    def test_new_database_reference_between_scan_and_apply_is_blocked(self):
        fixtures.Sandbox.db_fixture(self)
        folder = self.root / "run"
        report, plan = worker.discover_plan(self.cfg, self.job(), folder)
        self.assertTrue(report["ready"])
        with sqlite3.connect(self.server / "players.db") as db:
            db.execute(
                "CREATE TABLE later_players(id INTEGER PRIMARY KEY,owner_uuid TEXT)"
            )
            db.execute("INSERT INTO later_players VALUES(1,?)", (OU,))
        action = next(a for a in plan["actions"] if a["kind"] == "database")
        # SQLite schema guard also protects this case; exercise the identity guard directly.
        db = database.Database(self.cfg, self.endpoint)
        try:
            with self.assertRaisesRegex(ValueError, "identity scope changed"):
                db.mutate(action["operations"], identity=identity.Identity(OLD, NEW))
            self.assertEqual(
                db.execute("SELECT uuid FROM players")["rows"], [{"uuid": OU}]
            )
        finally:
            db.close()
