"""Current plugin inventories and direct Paper premium authentication."""
import json
import unittest
import zipfile
from unittest.mock import patch

import test_engine as fixtures
from test_engine import OLD, NEW, OU, NU
import common
import discovery
import login
import worker


class TopologyTests(unittest.TestCase):
    job = fixtures.Sandbox.job

    def setUp(self):
        fixtures.Sandbox.setUp(self)

    def plugin(self, name, config=None):
        directory = self.server / 'plugins'
        directory.mkdir(exist_ok=True)
        with zipfile.ZipFile(directory / (name + '.jar'), 'w') as archive:
            archive.writestr('plugin.yml', 'name: ' + name + '\nversion: 1\n')
        if config:
            folder = directory / name
            folder.mkdir(exist_ok=True)
            (folder / 'config.yml').write_text(config)

    def test_direct_paper_premium_needs_no_proxy_cache(self):
        self.plugin('AuthMe', 'settings:\n  enablePremium: true\nHooks:\n  bungeecord: false\n')
        self.plugin('packetevents')
        (self.server / 'server.properties').write_text('online-mode=false\n')
        manifest = discovery.configs(self.cfg)
        self.assertFalse(login.premium_errors(manifest['login'], [{'location': 'authme'}]))
        self.assertEqual(manifest['login'][0]['proxyForwarding'], False)
        manifest['login'][0]['proxyHook'] = True
        self.assertIn('Direct premium', login.premium_errors(manifest['login'], [1])[0]['error'])

    def test_removed_plugin_keeps_files_but_not_stale_connections(self):
        self.plugin('AuthMe')
        self.plugin('Removed', 'mysql:\n  host: unavailable\n  database: removed\n  user: x\n  password: x\n')
        self.assertEqual(len(discovery.configs(self.cfg)['endpoints']), 1)
        (self.server / 'plugins/Removed.jar').unlink()
        manifest = discovery.configs(self.cfg)
        self.assertFalse(manifest['endpoints'])
        self.assertIn('s/plugins/Removed/config.yml', [f['path'] for f in manifest['files']])
        self.assertTrue(any('inactive plugin' in r['reason'] for r in manifest['skipped']))

    def test_every_scan_refreshes_plugin_files(self):
        (self.server / 'usercache.json').write_text(json.dumps([{'name': OLD, 'uuid': OU}]))
        old = self.server / 'old.json'
        old.write_text('{}')
        first, _ = worker.discover_plan(self.cfg, self.job(), self.root / 'first')
        old.unlink()
        (self.server / 'new.json').write_text('{}')
        second, _ = worker.discover_plan(self.cfg, self.job(), self.root / 'second')
        self.assertTrue(first['ready'] and second['ready'])
        self.assertIn('s/new.json', [f['path'] for f in common.load(self.root/'second/inventory.json')['files']])

    def test_changing_scope_converges_without_applying_stale_plan(self):
        (self.server / 'usercache.json').write_text(json.dumps([{'name': OLD, 'uuid': OU}]))
        actual = worker.moves
        calls = []
        def change(cfg, ident):
            calls.append(1)
            (self.server / 'new.json').write_text('{}') if len(calls) == 1 else None
            return actual(cfg, ident)
        with patch.object(worker, 'moves', side_effect=change):
            report, _ = worker.discover_plan(self.cfg, self.job(), self.root / 'run')
        self.assertTrue(report['ready'])
        self.assertEqual(len(calls), 2)
        self.assertEqual(report['coverage']['discoveredFiles'], 2)

    def test_unchanged_storage_with_different_walk_order_is_not_scope_drift(self):
        (self.server / 'usercache.json').write_text(json.dumps([{'name': OLD, 'uuid': OU}]))
        (self.server / 'other.json').write_text('{}')
        actual = worker.configs
        count = []
        def reordered(cfg):
            result = actual(cfg)
            count.append(1)
            if len(count) % 2 == 0:
                result['files'].reverse()
            return result
        with patch.object(worker, 'configs', side_effect=reordered):
            report, _ = worker.discover_plan(self.cfg,self.job(),self.root/'ordered')
        self.assertTrue(report['ready'])
        self.assertFalse(report['coverage']['scopeChanged'])

    def test_plugin_and_database_inventory_changes_reuse_valid_nbt_checkpoints(self):
        path = self.server / 'player.nbt'
        path.write_bytes(fixtures.compound({'Owner': (11, fixtures.ORAW)}))
        first,_ = worker.discover_plan(self.cfg,self.job(),self.root/'first')
        self.plugin('NewPlugin')
        self.cfg['scanThreads'] = 3
        self.cfg['_mysqlScope'] = []
        second,_ = worker.discover_plan(self.cfg,self.job(),self.root/'second')
        self.assertTrue(first['ready'] and second['ready'])
        self.assertEqual(second['coverage']['cacheHits'],1)
