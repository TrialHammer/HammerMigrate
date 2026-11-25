"""Plugin-level balances, stable particle objects and reversible account merges."""
import sqlite3
import unittest

import test_engine as fixtures
from test_engine import OLD, NEW, OU, NU
import database
import identity
import worker
from unittest.mock import patch


class PluginMergeTests(unittest.TestCase):
    def setUp(self):
        fixtures.Sandbox.setUp(self)
        db = sqlite3.connect(self.server/'players.db')
        db.executescript('''
            CREATE TABLE playerpoints_points(id INT PRIMARY KEY,uuid TEXT UNIQUE,points INT);
            CREATE TABLE playerpoints_username_cache(uuid TEXT PRIMARY KEY,username TEXT);
            CREATE TABLE playerparticles_settings(player_uuid TEXT PRIMARY KEY,particles_hidden INT);
            CREATE TABLE playerparticles_group(uuid TEXT PRIMARY KEY,owner_uuid TEXT,name TEXT);
            CREATE TABLE playerparticles_particle(uuid TEXT PRIMARY KEY,group_uuid TEXT,effect TEXT);
        ''')
        for uid, name, number, balance in ((OU, OLD, 1, 384),(NU, NEW, 2, 10)):
            db.execute('INSERT INTO playerpoints_points VALUES(?,?,?)',(number,uid,balance))
            db.execute('INSERT INTO playerpoints_username_cache VALUES(?,?)',(uid,name))
            db.execute('INSERT INTO playerparticles_settings VALUES(?,?)',(uid,number-1))
            # Group object UUIDs deliberately match player UUID strings; semantic role must win.
            db.execute('INSERT INTO playerparticles_group VALUES(?,?,?)',(uid,uid,'active'))
            db.execute('INSERT INTO playerparticles_group VALUES(?,?,?)',(uid+'-saved',uid,'default-2'))
            db.execute('INSERT INTO playerparticles_particle VALUES(?,?,?)',('effect-'+str(number),uid,'firefly'))
        db.commit()
        db.close()
        self.db = database.Database(self.cfg,self.endpoint)
        self.addCleanup(self.db.close)

    def snapshot(self):
        return {t['name']: self.db.execute('SELECT * FROM '+self.db.q(t['name']))['rows'] for t in self.db.tables()}

    def test_explicit_merge_preserves_balances_groups_and_reverse(self):
        before = self.snapshot()
        ident = identity.Identity(OLD, NEW, merge=True)
        ops, inventory = self.db.plan(ident)
        self.assertFalse(ident.collisions)
        self.assertFalse(ident.unknown)
        self.db.validate(ops)
        self.db.mutate(ops)
        points = self.db.execute('SELECT uuid,points FROM playerpoints_points')['rows']
        self.assertEqual(points, [{'uuid':NU,'points':394}])
        groups = self.db.execute('SELECT * FROM playerparticles_group')['rows']
        self.assertEqual(len(groups), 4)
        self.assertEqual({r['owner_uuid'] for r in groups},{NU})
        self.assertEqual(next(r for r in groups if r['uuid']==OU)['name'],'active')
        self.assertTrue(next(r for r in groups if r['uuid']==NU)['name'].startswith('recovery-target-'))
        self.assertEqual(self.snapshot()['playerparticles_particle'],before['playerparticles_particle'])
        self.db.mutate(ops)  # Replaying a committed batch is idempotent.
        self.db.mutate(ops, True)
        self.assertEqual(self.snapshot(), before)
        self.db.mutate(ops, True)

    def test_without_merge_target_data_is_blocked(self):
        ident = identity.Identity(OLD, NEW)
        ops,_ = self.db.plan(ident)
        self.assertTrue(ident.collisions)
        with self.assertRaisesRegex(ValueError, 'unique key'):
            self.db.validate(ops)

    def test_logical_particle_references_detect_parent_deletion(self):
        row = self.db.execute('SELECT * FROM playerparticles_group WHERE uuid=?',(OU,))['rows'][0]
        with self.assertRaisesRegex(ValueError, 'Plugin reference integrity'):
            self.db.mutate([dict(table='playerparticles_group',pk=['uuid'],before=row,after=None)])
        self.assertEqual(len(self.db.execute('SELECT * FROM playerparticles_group')['rows']),4)

    def test_points_overflow_blocks_merge(self):
        self.db.execute('UPDATE playerpoints_points SET points=2147483647 WHERE uuid=?',(OU,))
        self.db.commit()
        with self.assertRaisesRegex(ValueError, 'integer range'):
            self.db.plan(identity.Identity(OLD,NEW,merge=True))

    def test_worker_manifest_contains_full_merge_and_rollback(self):
        job = fixtures.Sandbox.job(self)
        job['merge'] = True
        report,plan = worker.discover_plan(self.cfg,job,self.root/'run')
        self.assertTrue(report['ready'],report['errors'])
        before = self.snapshot()
        worker.apply_run(self.cfg,self.root/'run',plan)
        worker.rollback_run(self.cfg,self.root/'run')
        self.assertEqual(self.snapshot(),before)

    def test_sqlite_planning_failure_is_never_ignored_as_open_error(self):
        job = fixtures.Sandbox.job(self)
        job['merge'] = True
        with patch.object(database.Database,'validate',side_effect=ValueError('plugin reference failure')):
            report, _ = worker.discover_plan(self.cfg,job,self.root/'blocked')
        self.assertFalse(report['ready'])
        self.assertFalse(report['ignoredScanErrors'])
        self.assertIn('plugin reference failure',str(report['errors']))
