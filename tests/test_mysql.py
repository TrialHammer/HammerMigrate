"""Run against an explicitly supplied disposable UNIX socket, never a production endpoint."""

import json
import os
from pathlib import Path
import sys
import unittest
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parent))
import test_engine as fixtures
from test_engine import OLD, NEW, OU, NU
import database
import identity
import pymysql

SOCKET = os.environ.get("HAMMERMIGRATE_TEST_MYSQL_SOCKET")


@unittest.skipUnless(
    SOCKET,
    "Set HAMMERMIGRATE_TEST_MYSQL_SOCKET for a disposable local MariaDB instance",
)
class MySQLTests(unittest.TestCase):
    def setUp(self):
        fixtures.Sandbox.setUp(self)
        self.admin = pymysql.connect(unix_socket=SOCKET, user="root", autocommit=True)
        self.schema = "hm_test_" + uuid.uuid4().hex[:12]
        with self.admin.cursor() as cursor:
            cursor.execute(
                "CREATE DATABASE `"
                + self.schema
                + "` CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"
            )
        self.addCleanup(self.clean)
        self.endpoint = {
            "kind": "mysql",
            "host": "localhost",
            "unixSocket": SOCKET,
            "database": self.schema,
            "user": "root",
            "password": "",
        }
        self.db = database.Database(self.cfg, self.endpoint)
        self.addCleanup(self.db.close)
        self.db.execute(
            "CREATE TABLE players(uuid VARCHAR(36) PRIMARY KEY,username VARCHAR(16) UNIQUE,score INT) ENGINE=InnoDB"
        )
        self.db.execute(
            "CREATE TABLE homes(id INT PRIMARY KEY,owner_uuid VARCHAR(36),FOREIGN KEY (owner_uuid) REFERENCES players(uuid)) ENGINE=InnoDB"
        )
        self.db.execute(
            "CREATE TABLE plan_users(id INT PRIMARY KEY,uuid VARCHAR(36) UNIQUE,name VARCHAR(16)) ENGINE=InnoDB"
        )
        self.db.execute(
            "CREATE TABLE plan_sessions(id INT PRIMARY KEY,user_id INT,FOREIGN KEY(user_id) REFERENCES plan_users(id)) ENGINE=InnoDB"
        )
        self.db.execute("INSERT INTO players VALUES (%s,%s,7)", (OU, OLD))
        self.db.execute("INSERT INTO homes VALUES (1,%s)", (OU,))
        self.db.execute("INSERT INTO plan_users VALUES (42,%s,%s)", (OU, OLD))
        self.db.execute("INSERT INTO plan_sessions VALUES (9,42)")
        self.db.commit()

    def clean(self):
        with self.admin.cursor() as cursor:
            cursor.execute("DROP DATABASE `" + self.schema + "`")
        self.admin.close()

    def test_real_particle_settings_without_sql_primary_key(self):
        self.db.execute("CREATE TABLE playerparticles_settings(player_uuid VARCHAR(36),particles_hidden TINYINT,particles_hidden_self TINYINT) ENGINE=InnoDB")
        self.db.execute("INSERT INTO playerparticles_settings VALUES (%s,0,0)",(OU,))
        self.db.execute("INSERT INTO playerparticles_settings VALUES (%s,1,1)",(NU,))
        self.db.commit()
        ident=identity.Identity(OLD,NEW,merge=True)
        ops,_=self.db.plan(ident)
        self.assertFalse(ident.collisions)
        self.db.validate(ops)
        self.db.mutate(ops,identity=identity.Identity(OLD,NEW,merge=True))
        self.assertEqual(self.db.execute("SELECT * FROM playerparticles_settings")["rows"],[dict(player_uuid=NU,particles_hidden=0,particles_hidden_self=0)])
        self.db.mutate(ops,True)
        self.assertEqual(len(self.db.execute("SELECT * FROM playerparticles_settings")["rows"]),2)
        self.db.execute("INSERT INTO playerparticles_settings VALUES (%s,0,1)",(OU,))
        self.db.commit()
        with self.assertRaisesRegex(ValueError,"Ambiguous"):
            self.db.plan(identity.Identity(OLD,NEW,merge=True))

    def test_playerpoints_and_particle_merges_preserve_mysql_rows(self):
        self.db.execute("CREATE TABLE playerpoints_points(id INT PRIMARY KEY,uuid VARCHAR(36) UNIQUE,points INT) ENGINE=InnoDB")
        self.db.execute("CREATE TABLE playerparticles_group(uuid VARCHAR(36) PRIMARY KEY,owner_uuid VARCHAR(36),name VARCHAR(100)) ENGINE=InnoDB")
        self.db.execute("CREATE TABLE playerparticles_particle(uuid VARCHAR(36) PRIMARY KEY,group_uuid VARCHAR(36),effect VARCHAR(50)) ENGINE=InnoDB")
        for number, uid, balance in ((1,OU,384),(2,NU,10)):
            self.db.execute("INSERT INTO playerpoints_points VALUES (%s,%s,%s)",(number,uid,balance))
            self.db.execute("INSERT INTO playerparticles_group VALUES (%s,%s,'active')",(uid,uid))
            self.db.execute("INSERT INTO playerparticles_particle VALUES (%s,%s,'firefly')",(str(uuid.uuid4()),uid))
        self.db.commit()
        def snapshot():
            return {t["name"]: self.db.execute("SELECT * FROM "+self.db.q(t["name"]))["rows"] for t in self.db.tables()}
        before = snapshot()
        ident = identity.Identity(OLD,NEW,merge=True)
        ops,_ = self.db.plan(ident)
        self.assertFalse(ident.collisions)
        self.db.validate(ops)
        self.db.mutate(ops)
        self.assertEqual(self.db.execute("SELECT uuid,points FROM playerpoints_points")["rows"],[{"uuid":NU,"points":394}])
        self.assertEqual(snapshot()["playerparticles_particle"],before["playerparticles_particle"])
        self.db.mutate(ops)
        self.db.mutate(ops,True)
        self.assertEqual(snapshot(),before)
        self.db.mutate(ops,True)
        # Failure after retiring the target must restore both balances atomically.
        self.db.execute("ALTER TABLE playerpoints_points ADD CONSTRAINT balance_limit CHECK(points<=390)")
        self.db.commit()
        with self.assertRaises(pymysql.MySQLError):
            self.db.mutate(ops)
        self.assertEqual(snapshot(),before)

    def test_immediate_fk_and_stable_plan_id(self):
        ops, _ = self.db.plan(self.ident)
        self.assertFalse(self.ident.unknown)
        self.db.validate(ops)
        signature = self.db.schema_signature(ops)
        self.db.mutate(ops)
        self.assertEqual(
            self.db.execute("SELECT owner_uuid FROM homes")["rows"],
            [{"owner_uuid": NU}],
        )
        self.assertEqual(
            self.db.execute("SELECT user_id FROM plan_sessions")["rows"],
            [{"user_id": 42}],
        )
        self.assertEqual(signature, self.db.schema_signature(ops))
        self.db.mutate(ops)
        self.db.mutate(ops, True)
        self.assertEqual(
            self.db.execute("SELECT uuid FROM players")["rows"], [{"uuid": OU}]
        )
        self.assertEqual(
            self.db.execute("SELECT @@FOREIGN_KEY_CHECKS AS enabled")["rows"],
            [{"enabled": 1}],
        )

    def test_case_insensitive_occupancy_with_binary_collation(self):
        self.db.execute(
            "INSERT INTO players VALUES (%s,%s,1)", (str(uuid.uuid4()), NEW.lower())
        )
        self.db.commit()
        self.db.plan(self.ident)
        self.assertTrue(self.ident.collisions)

    def test_readonly_plan_and_constraint_failure_rollback(self):
        readonly = database.Database(self.cfg, self.endpoint, True)
        try:
            ops, _ = readonly.plan(self.ident)
            readonly.validate(ops)
            with self.assertRaises(pymysql.MySQLError):
                readonly.execute("UPDATE players SET score=99")
        finally:
            readonly.close()
        # CHECK is enforced by the actual engine, and the batch must roll back completely.
        self.db.execute("ALTER TABLE players ADD CONSTRAINT same_score CHECK (score=7)")
        self.db.commit()
        broken = [
            dict(o, after=dict(o["after"], score=0)) if o["table"] == "players" else o
            for o in ops
        ]
        with self.assertRaises(pymysql.MySQLError):
            self.db.mutate(broken)
        self.assertEqual(
            self.db.execute("SELECT uuid FROM players")["rows"], [{"uuid": OU}]
        )
        self.assertEqual(
            self.db.execute("SELECT owner_uuid FROM homes")["rows"],
            [{"owner_uuid": OU}],
        )
        self.assertEqual(
            self.db.execute("SELECT @@FOREIGN_KEY_CHECKS AS enabled")["rows"],
            [{"enabled": 1}],
        )

    def test_trigger_preflight_blocks_unjournaled_side_effects(self):
        self.db.execute("CREATE TABLE audit_log(value VARCHAR(36)) ENGINE=InnoDB")
        self.db.execute(
            "CREATE TRIGGER touched AFTER UPDATE ON players FOR EACH ROW INSERT INTO audit_log VALUES (NEW.uuid)"
        )
        self.db.commit()
        ops, _ = self.db.plan(self.ident)
        with self.assertRaisesRegex(ValueError, "triggers"):
            self.db.validate(ops)

    def test_native_uuid_columns_and_scan_inventory(self):
        self.db.execute(
            "CREATE TABLE native_players(uuid UUID PRIMARY KEY, username VARCHAR(16)) ENGINE=InnoDB"
        )
        self.db.execute("INSERT INTO native_players VALUES (%s,%s)", (OU, OLD))
        self.db.commit()
        ops, inventory = self.db.plan(self.ident)
        changed = next(op for op in ops if op["table"] == "native_players")
        self.assertEqual(changed["after"]["uuid"], NU)
        scanned = next(t for t in inventory if t["table"] == "native_players")
        self.assertEqual(scanned["totalRows"], 1)
        self.assertEqual(scanned["candidateRows"], 1)
        self.assertTrue(scanned["scanComplete"])
        self.db.validate(ops)
        self.db.mutate(ops)
        self.assertEqual(
            self.db.execute("SELECT uuid FROM native_players")["rows"], [{"uuid": NU}]
        )
        self.db.mutate(ops, True)


if __name__ == "__main__":
    unittest.main()
