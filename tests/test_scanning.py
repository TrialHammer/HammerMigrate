"""Persistent NBT checkpoints, stale scopes and dry-run completeness."""

import contextlib
import io
import sqlite3
import threading
import unittest
from unittest.mock import patch
import zlib

import test_engine as fixtures
from test_engine import OLD, NEW, OU, NU
import common
import scanning
import worker


class ScanningTests(unittest.TestCase):
    job = fixtures.Sandbox.job

    def setUp(self):
        fixtures.Sandbox.setUp(self)
        self.output = io.StringIO()
        self.addCleanup(self.output.close)
        self.redirect = contextlib.redirect_stdout(self.output)
        self.redirect.__enter__()
        self.addCleanup(self.redirect.__exit__, None, None, None)

    def nbt_fixture(self):
        path = self.server / "world/playerdata" / (OU + ".dat")
        path.parent.mkdir(parents=True)
        path.write_bytes(fixtures.compound({"UUID": (11, fixtures.ORAW)}))
        return path

    def scan(self, name, fresh=False):
        return worker.discover_plan(self.cfg, self.job(), self.root / name, fresh=fresh)

    def test_parallel_files_match_serial_results_and_reuse_cache(self):
        for index in range(4):
            path = self.server / ("player%d.nbt" % index)
            path.write_bytes(fixtures.compound({"Owner": (11, fixtures.ORAW)}))
        serial, serial_plan = self.scan("serial")
        self.cfg["scanThreads"] = 4
        barrier = threading.Barrier(4, timeout=10)
        original = scanning.ScanSession.analyze

        def overlap(session, item, path, raw):
            barrier.wait()
            return original(session, item, path, raw)

        with patch.object(scanning.ScanSession, "analyze", overlap):
            parallel, parallel_plan = self.scan("parallel", fresh=True)
        self.assertTrue(serial["ready"])
        self.assertTrue(parallel["ready"])
        self.assertEqual(serial_plan["actions"], parallel_plan["actions"])
        self.assertEqual(parallel["coverage"]["coveredFiles"], 4)
        cached, cached_plan = self.scan("cached")
        self.assertTrue(cached["ready"])
        self.assertEqual(cached["coverage"]["cacheHits"], 4)
        self.assertEqual(parallel_plan["actions"], cached_plan["actions"])

    def test_invalid_parallelism_fails_closed(self):
        self.nbt_fixture()
        for value in (0, 33, True, "4"):
            with self.subTest(value=value):
                self.cfg["scanThreads"] = value
                with self.assertRaisesRegex(ValueError, "scanThreads"):
                    self.scan("invalid")

    def test_nbt_checkpoint_reuse_and_content_invalidation(self):
        path = self.nbt_fixture()
        before = path.read_bytes()
        first, plan = self.scan("one")
        self.assertTrue(first["ready"])
        self.assertEqual(first["coverage"]["coveredFiles"], 1)
        with patch.object(
            scanning, "transform", side_effect=AssertionError("must reuse")
        ):
            second, again = self.scan("two")
        self.assertEqual(second["coverage"]["cacheHits"], 1)
        self.assertEqual(plan["actions"], again["actions"])
        self.assertEqual(path.read_bytes(), before)
        path.write_bytes(
            fixtures.compound({"UUID": (11, fixtures.ORAW), "score": (3, 5)})
        )
        third, _ = self.scan("three")
        self.assertTrue(third["ready"])
        self.assertEqual(third["coverage"]["cacheHits"], 0)
        fourth, _ = self.scan("four", fresh=True)
        self.assertTrue(fourth["ready"])
        self.assertEqual(fourth["coverage"]["cacheHits"], 0)

    def test_interrupt_resumes_completed_files_and_keeps_manifest_unready(self):
        self.nbt_fixture()
        path = self.server / "z.nbt"
        path.write_bytes(fixtures.compound({"Owner": (11, fixtures.ORAW)}))
        original = scanning.ScanSession.scan_file
        count = 0

        def interrupt(session, item):
            nonlocal count
            count += 1
            if count == 2:
                raise KeyboardInterrupt("interrupted test")
            return original(session, item)

        with patch.object(scanning.ScanSession, "scan_file", interrupt):
            with self.assertRaises(KeyboardInterrupt):
                self.scan("interrupted")
        self.assertFalse(common.load(self.root / "interrupted/manifest.json")["ready"])
        progress = common.load(self.root / "interrupted/progress.json")
        self.assertEqual(progress["state"], "interrupted")
        self.assertEqual(progress["completedFiles"], 1)
        report, _ = self.scan("resumed")
        self.assertTrue(report["ready"])
        self.assertEqual(report["coverage"]["cacheHits"], 1)
        self.assertEqual(report["coverage"]["coveredFiles"], 2)

    def test_external_chunk_dependency_invalidates_region_checkpoint(self):
        path = self.server / "r.0.0.mca"
        path.write_bytes(
            ((2 << 8) | 1).to_bytes(4, "big")
            + bytes(8188)
            + b"\0\0\0\1\x82"
            + bytes(4091)
        )
        external = self.server / "c.0.0.mcc"
        external.write_bytes(
            zlib.compress(fixtures.compound({"Owner": (11, fixtures.ORAW)}))
        )
        first, _ = self.scan("one")
        self.assertTrue(first["ready"])
        self.assertEqual(first["coverage"]["coveredFiles"], 2)
        second, _ = self.scan("two")
        self.assertEqual(second["coverage"]["cacheHits"], 1)
        external.write_bytes(
            zlib.compress(
                fixtures.compound({"Owner": (11, fixtures.ORAW), "score": (3, 4)})
            )
        )
        third, _ = self.scan("three")
        self.assertTrue(third["ready"])
        self.assertEqual(third["coverage"]["cacheHits"], 0)

    def test_files_added_removed_or_changed_during_scan_block_readiness(self):
        self.cfg["scanMaxPasses"] = 1
        path = self.nbt_fixture()
        original = worker.moves

        def add_file(cfg, ident):
            (self.server / "new.json").write_text("{}")
            return original(cfg, ident)

        with patch.object(worker, "moves", side_effect=add_file):
            report, _ = self.scan("added")
        self.assertFalse(report["ready"])
        self.assertIn("s/new.json", report["coverage"]["changedDuringScan"])

        def remove_file(cfg, ident):
            (self.server / "new.json").unlink()
            return original(cfg, ident)

        with patch.object(worker, "moves", side_effect=remove_file):
            report, _ = self.scan("removed")
        self.assertFalse(report["ready"])

        def edit_file(cfg, ident):
            path.write_bytes(fixtures.compound({"Owner": (11, fixtures.ORAW)}))
            return original(cfg, ident)

        with patch.object(worker, "moves", side_effect=edit_file):
            report, _ = self.scan("changed")
        self.assertFalse(report["ready"])
        self.assertIn(
            str(path.relative_to(self.root)), report["coverage"]["changedDuringScan"]
        )

    def test_unsupported_hits_and_errors_persist_in_coverage(self):
        self.nbt_fixture()
        (self.server / "opaque.unknown").write_bytes(OLD.encode())
        (self.server / "bad.nbt").write_bytes(b"truncated" + fixtures.ORAW)
        report, _ = self.scan("blocked")
        self.assertFalse(report["ready"])
        self.assertEqual(report["coverage"]["failedFiles"], 1)
        self.assertTrue(report["unresolved"])
        self.assertFalse(common.load(self.root / "blocked/coverage.json")["complete"])

    def test_rules_and_corrupt_cache_cannot_reuse_stale_results(self):
        self.nbt_fixture()
        self.scan("one")
        path = self.root / "migrate/HammerMigrate/scan-cache.sqlite"
        with sqlite3.connect(path) as connection:
            connection.execute("UPDATE files SET payload=?", [b"corrupted"])
        report, _ = self.scan("two")
        self.assertTrue(report["ready"])
        self.assertEqual(report["coverage"]["cacheHits"], 0)
        self.cfg["fieldRules"] = [{"location": ".*irrelevant", "role": "preserve"}]
        report, _ = self.scan("three")
        self.assertEqual(report["coverage"]["cacheHits"], 0)

    def test_dry_run_alias_allows_queued_identity_and_progress_bypasses_lock(self):
        path = self.nbt_fixture()
        before = path.read_bytes()
        worker.main(["--config", str(self.config), "queue", OLD, NEW])
        self.assertEqual(
            worker.main(["--config", str(self.config), "dry-run", OLD, NEW]), 0
        )
        self.assertEqual(
            worker.main(["--config", str(self.config), "dry-run", OLD, NEW, "--fresh"]),
            0,
        )
        with common.Lock(worker.state(self.cfg) / "coordinator.lock"):
            self.assertEqual(worker.main(["--config", str(self.config), "progress"]), 0)
        self.assertEqual(path.read_bytes(), before)
        self.assertEqual(worker.jobs(self.cfg)[0]["status"], "queued")

    def test_planned_output_is_reparsed_for_unmigrated_source_references(self):
        self.nbt_fixture()
        original = scanning.transform
        count = 0

        def broken_output(path, kind, raw, ident):
            nonlocal count
            count += 1
            changed, external = original(path, kind, raw, ident)
            return (raw if count == 1 else changed), external

        with patch.object(scanning, "transform", side_effect=broken_output):
            report, _ = self.scan("broken")
        self.assertFalse(report["ready"])
        self.assertIn("retains source identity", common.dumps(report["errors"]))

    def test_lowercase_nbt_uuid_halves_are_found_without_contiguous_uuid_bytes(self):
        path = self.server / "halves.nbt"
        path.write_bytes(
            fixtures.compound(
                {
                    "OwnerUUIDmost": (
                        4,
                        int.from_bytes(fixtures.ORAW[:8], "big", signed=True),
                    ),
                    "OwnerUUIDleast": (
                        4,
                        int.from_bytes(fixtures.ORAW[8:], "big", signed=True),
                    ),
                }
            )
        )
        report, plan = self.scan("halves")
        self.assertTrue(report["ready"])
        self.assertEqual(len(plan["actions"]), 1)

    def test_opaque_and_text_uuid_case_variants_and_target_names_are_detected(self):
        self.nbt_fixture()
        for suffix in ("unknown", "txt"):
            path = self.server / ("references." + suffix)
            for value in (
                OU.upper(),
                OU.replace("-", "").upper(),
                NU.upper(),
                NU.replace("-", "").upper(),
                NEW.lower(),
            ):
                with self.subTest(suffix=suffix, value=value):
                    path.write_text("prefix " + value + " suffix")
                    report, _ = self.scan("references")
                    self.assertFalse(report["ready"])
                    self.assertTrue(report["unresolved"] or report["collisions"])
            path.unlink()
