"""Network coordinator invariants around startup, moves, and automatic compensation."""

import json
from pathlib import Path
import socket
import subprocess
import sys
import unittest
from unittest.mock import patch
import uuid

import test_engine as fixtures
from test_engine import OLD, NEW, OU, NU
import common
import database
import discovery
import files
import identity
import worker


class RecoveryTests(unittest.TestCase):
    job = fixtures.Sandbox.job
    db_fixture = fixtures.Sandbox.db_fixture
    fixture_files = fixtures.Sandbox.fixture_files

    def setUp(self):
        fixtures.Sandbox.setUp(self)

    def test_failed_second_action_compensates_first(self):
        self.fixture_files()
        before = (self.server / "usercache.json").read_bytes()
        folder = self.root / "run"
        report, plan = worker.discover_plan(self.cfg, self.job(), folder)
        actual = worker.perform
        calls = 0

        def fail(cfg, action, reverse=False, proof=None):
            nonlocal calls
            if not reverse:
                calls += 1
                if calls == 2:
                    raise OSError("injected failure")
            return actual(cfg, action, reverse, proof)

        with patch.object(worker, "perform", side_effect=fail):
            with self.assertRaisesRegex(OSError, "injected"):
                worker.apply_run(self.cfg, folder, plan)
        self.assertEqual(before, (self.server / "usercache.json").read_bytes())
        self.assertEqual(
            common.Journal(folder / "journal.jsonl").events()[-1]["event"],
            "rollback_complete",
        )

    def test_recovery_after_database_commit_without_done_record(self):
        self.db_fixture()
        folder = self.root / "run"
        report, plan = worker.discover_plan(self.cfg, self.job(), folder)
        self.assertTrue(report["ready"])
        index = next(
            i for i, a in enumerate(plan["actions"]) if a["kind"] == "database"
        )
        journal = common.Journal(folder / "journal.jsonl")
        journal.append("started")
        journal.append("intent", index=index)
        worker.perform(self.cfg, plan["actions"][index])
        worker.rollback_run(self.cfg, folder)
        db = database.Database(self.cfg, self.endpoint)
        try:
            self.assertEqual(
                db.execute("SELECT uuid FROM players")["rows"][0]["uuid"], OU
            )
        finally:
            db.close()

    def test_rollback_rejects_player_progress(self):
        self.fixture_files()
        folder = self.root / "run"
        report, plan = worker.discover_plan(self.cfg, self.job(), folder)
        worker.apply_run(self.cfg, folder, plan)
        path = self.server / "world/playerdata" / f"{NU}.dat"
        path.write_bytes(path.read_bytes() + b"player-progress")
        with self.assertRaisesRegex(ValueError, "changed"):
            worker.rollback_run(self.cfg, folder)
        self.assertIn(b"player-progress", path.read_bytes())

    def test_schema_change_blocks_replay(self):
        self.db_fixture()
        folder = self.root / "run"
        report, plan = worker.discover_plan(self.cfg, self.job(), folder)
        db = database.Database(self.cfg, self.endpoint)
        db.execute("ALTER TABLE players ADD COLUMN extra INT")
        db.commit()
        db.close()
        action = next(a for a in plan["actions"] if a["kind"] == "database")
        with self.assertRaisesRegex(ValueError, "schema changed"):
            worker.perform(self.cfg, action)

    def test_premium_lookup_uses_canonical_case_and_bound_identity(self):
        ident = identity.Identity(OLD, NEW, True)

        class Response:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                pass

            def read(self):
                return json.dumps({"name": NEW, "id": "a" * 32}).encode()

        with patch("urllib.request.urlopen", return_value=Response()):
            worker.premium(ident)
        self.assertEqual(ident.premium_uuid, str(uuid.UUID("a" * 32)))
        ident = identity.Identity(OLD, NEW.lower(), True)
        with patch("urllib.request.urlopen", return_value=Response()):
            with self.assertRaisesRegex(ValueError, "canonical"):
                worker.premium(ident)

    def test_ignore_directory_applies_to_scan_and_move(self):
        directory = self.server / "ignored" / OU
        directory.mkdir(parents=True)
        (directory / "data.json").write_text('{"uuid":"' + OU + '"}')
        self.cfg["ignorePaths"] = ["s/ignored"]
        self.assertFalse(discovery.configs(self.cfg)["files"])
        self.assertFalse(files.moves(self.cfg, self.ident))

    def test_world_lock_survives_tcp_recheck(self):
        self.fixture_files()
        held = discovery.quiescent(self.cfg, {"listeners": []})
        try:
            discovery.quiescent(self.cfg, {"listeners": []}, world_locks=False)
            program = 'import fcntl,sys;f=open(sys.argv[1],"r+b");fcntl.lockf(f,fcntl.LOCK_EX|fcntl.LOCK_NB)'
            result = subprocess.run(
                [
                    sys.executable,
                    "-c",
                    program,
                    str(self.server / "world/session.lock"),
                ],
                capture_output=True,
            )
            self.assertNotEqual(result.returncode, 0)
        finally:
            for handle in held:
                handle.close()

    def test_case_only_physical_paths(self):
        ident = identity.Identity("abc", "Abc")
        directory = self.server / "players"
        directory.mkdir()
        (directory / "abc.yml").write_text("username: abc")
        result = files.moves(self.cfg, ident)
        self.assertEqual(result[0]["target"], "s/players/Abc.yml")
        self.assertFalse(ident.collisions)

    def test_player_named_config_does_not_rename_plugin_config(self):
        plugin = self.server / "plugins/Example"
        plugin.mkdir(parents=True)
        (plugin / "config.yml").write_text("setting: unchanged")
        ident = identity.Identity("config", "NewName")
        self.assertFalse(files.moves(self.cfg, ident))
        self.assertFalse(ident.unknown)

    def test_uppercase_uuid_filename(self):
        path = self.server / (OU.upper() + ".json")
        path.write_text("{}")
        result = files.moves(self.cfg, self.ident)
        self.assertEqual(result[0]["target"], "s/" + NU + ".json")

    def test_orphan_external_chunk_is_scanned(self):
        import nbt, zlib

        path = self.server / "c.0.0.mcc"
        path.write_bytes(
            zlib.compress(fixtures.compound({"Owner": (11, fixtures.ORAW)}))
        )
        report, plan = worker.discover_plan(self.cfg, self.job(), self.root / "run")
        self.assertTrue(report["ready"])
        self.assertEqual(plan["actions"][0]["path"], "s/c.0.0.mcc")


if __name__ == "__main__":
    unittest.main()
