"""Separate local roots, shared-only coordination and cross-participant recovery."""

import contextlib
import io
import json
import os
from pathlib import Path
import shlex
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

from test_engine import OLD, NEW, OU, NU, ENGINE
import common
import distributed
import worker


class DistributedTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="hammermigrate-distributed-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.shared = self.root / "network"
        self.shared.mkdir()
        self.cfg = {
            "networkRoot": str(self.shared),
            "servers": ["innova", "paradise", "lavender"],
            "deployment": "distributed",
            "startup": "external",
            "participantPollSeconds": 0.02,
            "databaseEndpoints": [],
            "discoverRelatedSchemas": False,
        }
        self.config = self.shared / "HammerMigrate/network.json"
        common.save(self.config, self.cfg)
        self.directories = {}
        for server in self.cfg["servers"]:
            directory = self.root / (server + "-container") / "data"
            directory.mkdir(parents=True)
            (directory / "usercache.json").write_text(
                json.dumps([{"name": OLD, "uuid": OU}])
            )
            self.directories[server] = directory
        self.protocol = worker.state(self.cfg) / "distributed"
        self.holds = contextlib.ExitStack()
        self.addCleanup(self.holds.close)
        self.worlds = {s: [] for s in self.cfg["servers"]}
        self.addCleanup(
            lambda: [h.close() for handles in self.worlds.values() for h in handles]
        )
        self.locals = {
            s: distributed.local_config(self.cfg, s, p)
            for s, p in self.directories.items()
        }

    def queue(self, dry=False):
        with contextlib.redirect_stdout(io.StringIO()):
            worker.main(
                ["--config", str(self.config), "dry-run" if dry else "queue", OLD, NEW]
            )
        return worker.jobs(self.cfg)[0]

    def register(self):
        for server in self.cfg["servers"]:
            self.holds.enter_context(
                distributed.locked(self.protocol / "launchers" / (server + ".lock"))
            )
            common.save(
                self.protocol / "registrations" / (server + ".json"),
                {"instance": server, "configSha256": "same-config"},
            )

    def record(self, session, server, result):
        path = Path(session["roundDirectory"]) / "session.json"
        current = common.load(path)
        current["results"][server] = result
        common.save(path, current)

    def step_all(self, session, fail_server=None):
        for server in self.cfg["servers"]:
            result = {}
            try:
                if server == fail_server:
                    raise OSError("injected node failure")
                with contextlib.redirect_stdout(io.StringIO()):
                    distributed.participant_step(
                        self.cfg,
                        self.locals[server],
                        server,
                        session,
                        self.worlds[server],
                    )
            except Exception as error:
                result["error"] = str(error)
            self.record(session, server, result)

    def until(self, phase):
        for _ in range(10):
            session = distributed.coordinate(self.cfg, self.protocol)
            if session["phase"] == phase:
                return session
            self.step_all(session)
        self.fail("Did not reach " + phase)

    def assert_identity(self, name, uuid):
        for directory in self.directories.values():
            self.assertEqual(
                json.loads((directory / "usercache.json").read_text()),
                [{"name": name, "uuid": uuid}],
            )

    def test_startup_cancel_coordinates_and_recovers_writes(self):
        job = self.queue()
        self.register()
        session = self.until("applying")
        first = self.cfg["servers"][0]
        distributed.participant_step(self.cfg, self.locals[first], first, session, self.worlds[first])
        self.assertIn(NEW, (self.directories[first] / "usercache.json").read_text())
        path = Path(session["roundDirectory"]) / "session.json"
        distributed.startup_command(self.cfg, self.protocol, "cancel " + job["id"])
        current = common.load(path)
        self.assertEqual(current["phase"], "cancelling")
        self.assertTrue(current["cancelNeedsRecovery"])
        current["results"] = {s: {} for s in self.cfg["servers"]}
        common.save(path, current)
        recovery = distributed.coordinate(self.cfg, self.protocol)
        self.assertEqual(recovery["phase"], "recovering")
        self.step_all(recovery)
        self.assertEqual(distributed.coordinate(self.cfg, self.protocol)["phase"], "cancelled")
        self.assertEqual(worker.jobs(self.cfg)[0]["status"], "cancelled")
        self.assert_identity(OLD, OU)

    def test_startup_cancel_dry_run_waits_for_all_participants(self):
        job = self.queue(dry=True)
        self.register()
        session = distributed.coordinate(self.cfg, self.protocol)
        distributed.startup_command(self.cfg, self.protocol, "cancel " + job["id"])
        path = Path(session["roundDirectory"]) / "session.json"
        current = common.load(path)
        current["results"] = {s: {} for s in self.cfg["servers"][:-1]}
        common.save(path, current)
        self.assertEqual(distributed.coordinate(self.cfg, self.protocol)["phase"], "cancelling")
        current["results"][self.cfg["servers"][-1]] = {}
        common.save(path, current)
        self.assertEqual(distributed.coordinate(self.cfg, self.protocol)["phase"], "cancelled")
        self.assertEqual(worker.jobs(self.cfg)[0]["status"], "cancelled")

    def test_completed_step_is_recorded_while_another_node_is_missing(self):
        self.queue(dry=True)
        self.register()
        session = distributed.coordinate(self.cfg, self.protocol)
        waiting = {"phase": "waiting", "missing": "paradise"}
        distributed.record_step_result("innova", "innova", session, waiting, {})
        current = common.load(Path(session["roundDirectory"]) / "session.json")
        self.assertEqual(current["results"]["innova"], {})
        self.assertEqual(waiting["phase"], "waiting")

    def test_startup_console_cancel_waiting_job(self):
        job = self.queue(dry=True)
        process, marker, log = self.process("innova", interactive=True)
        process.stdin.write("status\ncancel " + job["id"] + "\n")
        process.stdin.flush()
        self.addCleanup(process.stdin.close)
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline and not marker.exists() and process.poll() is None:
            time.sleep(0.05)
        log.flush()
        log.seek(0)
        self.assertTrue(marker.exists(), log.read())
        self.assertEqual(worker.jobs(self.cfg)[0]["status"], "cancelled")

    def test_three_direct_paper_nodes_support_premium_without_proxy(self):
        import sqlite3
        premium_uuid = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
        for directory in self.directories.values():
            plugin = directory / "plugins/AuthMe"
            plugin.mkdir(parents=True)
            (plugin / "config.yml").write_text("settings:\n  enablePremium: true\nHooks:\n  bungeecord: false\n")
        path = self.directories["innova"] / "auth.db"
        with sqlite3.connect(path) as db:
            db.execute("CREATE TABLE authme(id INTEGER PRIMARY KEY,username TEXT,realname TEXT,uuid TEXT,premiumUUID TEXT,password TEXT)")
            db.execute("INSERT INTO authme VALUES (1,?,?,?,?,?)",(OLD.lower(),OLD,OU,premium_uuid,"retained-hash"))
        job = self.queue()
        job["premium"] = True
        worker.setjobs(self.cfg,[job])
        self.register()
        with patch.object(worker, "premium", side_effect=lambda ident: setattr(ident,"premium_uuid",premium_uuid)):
            self.until("done")
        self.assert_identity(NEW, NU)
        with sqlite3.connect(path) as db:
            self.assertEqual(db.execute("SELECT username,uuid,premiumUUID,password FROM authme").fetchone(),(NEW.lower(),NU,premium_uuid,"retained-hash"))

    def test_failed_preflight_does_not_block_boot_and_retry_uses_current_files(self):
        job = self.queue()
        self.register()
        broken = self.directories["innova"] / "unsupported.opaque"
        broken.write_text(OLD)
        session = self.until("failed")
        failed = worker.jobs(self.cfg)[0]
        self.assertTrue(failed["preflightBlocked"])
        self.assertFalse(worker.blocking_failures([failed]))
        self.assertIsNone(distributed.coordinate(self.cfg, self.protocol))
        worker.main(["--config", str(self.config), "startup", "--server", "innova"])
        broken.unlink()  # Plugin data removed before re-queueing the task.
        with contextlib.redirect_stdout(io.StringIO()):
            worker.main(["--config", str(self.config), "retry", job["id"]])
        self.until("done")
        self.assert_identity(NEW, NU)

    def test_distributed_database_scope_change_returns_to_discovery(self):
        self.queue()
        self.register()
        session = self.until("scanning")
        for server in self.cfg["servers"]:
            self.record(session, server, {"error": "scope changed", "rescan": True})
        refreshed = distributed.coordinate(self.cfg, self.protocol)
        self.assertEqual(refreshed["phase"], "discovering")
        self.assertFalse(refreshed["assignments"])
        self.until("done")
        self.assert_identity(NEW, NU)

    def test_startup_gate_allows_failed_dry_run_only(self):
        job = self.queue(dry=True)
        for status, dry, blocked in (("failed", True, False), ("failed", False, True),
                                     ("recovery_required", True, True)):
            job.update(status=status, dryRun=dry)
            worker.setjobs(self.cfg, [job])
            args = ["--config", str(self.config), "startup", "--server", "innova"]
            if blocked:
                with self.assertRaisesRegex(ValueError, "pre-start"):
                    worker.main(args)
            else:
                self.assertEqual(worker.main(args), 0)

    def test_force_cancel_requires_stopped_dry_run_participants(self):
        job = self.queue(dry=True)
        self.register()
        distributed.coordinate(self.cfg, self.protocol)
        args = ["--config", str(self.config), "cancel", job["id"]]
        with self.assertRaisesRegex(ValueError, "--force"):
            worker.main(args)
        with self.assertRaisesRegex(ValueError, "Stop every participant"):
            worker.main(args + ["--force"])
        self.holds.close()
        worker.main(args + ["--force"])
        self.assertEqual(worker.jobs(self.cfg)[0]["status"], "cancelled")
        self.assertNotIn("distributedRound", worker.jobs(self.cfg)[0])

    def test_force_cancel_never_discards_possible_writes(self):
        job = self.queue()
        job.update(status="running", distributedRound="test-round")
        worker.setjobs(self.cfg, [job])
        with self.assertRaisesRegex(ValueError, "dry-run"):
            worker.main(["--config", str(self.config), "cancel", job["id"], "--force"])
        self.assertEqual(worker.jobs(self.cfg)[0]["status"], "running")
        self.assertFalse(distributed.dry_run_failure({"phase": "failed", "recoveryRequired": True,
                                                       "identity": {"dryRun": True}}))

    def test_local_files_migrate_and_distributed_rollback(self):
        job = self.queue()
        self.register()
        done = self.until("done")
        self.assert_identity(NEW, NU)
        report = common.load(
            worker.state(self.cfg) / "runs" / job["id"] / "manifest.json"
        )
        self.assertTrue(report["ready"])
        self.assertEqual(set(report["participants"]), set(self.cfg["servers"]))
        for local in self.locals.values():
            self.assertEqual(local["servers"], ["data"])
        with contextlib.redirect_stdout(io.StringIO()):
            worker.main(["--config", str(self.config), "rollback", job["id"]])
        for handles in self.worlds.values():
            for handle in handles:
                handle.close()
            handles.clear()
        self.until("done")
        self.assert_identity(OLD, OU)
        self.assertEqual(worker.jobs(self.cfg)[0]["status"], "rolled_back")

    def test_apply_failure_compensates_successful_participants(self):
        self.queue()
        self.register()
        applying = self.until("applying")
        self.step_all(applying, fail_server="lavender")
        recovery = distributed.coordinate(self.cfg, self.protocol)
        self.assertEqual(recovery["phase"], "recovering")
        self.step_all(recovery)
        failed = distributed.coordinate(self.cfg, self.protocol)
        self.assertEqual(failed["phase"], "failed")
        self.assert_identity(OLD, OU)
        self.assertEqual(worker.jobs(self.cfg)[0]["status"], "failed")

    def test_restart_after_commit_recovers_every_participant(self):
        self.queue()
        self.register()
        applying = self.until("applying")
        self.step_all(applying)
        common.save(
            self.protocol / "registrations/innova.json",
            {"instance": "new-process", "configSha256": "same-config"},
        )
        recovery = distributed.coordinate(self.cfg, self.protocol)
        self.assertEqual(recovery["phase"], "recovering")
        self.step_all(recovery)
        self.assertEqual(
            distributed.coordinate(self.cfg, self.protocol)["phase"], "failed"
        )
        self.assert_identity(OLD, OU)

    def test_dry_run_and_global_target_collision(self):
        self.queue(dry=True)
        self.register()
        self.until("done")
        self.assert_identity(OLD, OU)
        self.assertEqual(worker.jobs(self.cfg)[0]["status"], "scanned")
        with contextlib.redirect_stdout(io.StringIO()):
            worker.main(["--config", str(self.config), "queue", OLD, NEW])
        target = self.directories["paradise"] / "whitelist.json"
        target.write_text(json.dumps([{"uuid": NU, "name": NEW}]))
        failed = self.until("failed")
        self.assert_identity(OLD, OU)

    def test_shared_mysql_schema_is_assigned_to_one_participant(self):
        self.queue()
        self.register()
        session = distributed.coordinate(self.cfg, self.protocol)
        endpoint = {
            "host": "database",
            "port": 3306,
            "database": "global_plan",
            "serverIdentity": ["mysql-unique", 3306, 1],
        }
        for server in self.cfg["servers"]:
            endpoint = dict(endpoint, host=server + "-db-alias")
            common.save(
                Path(session["roundDirectory"]) / server / "discovery.json",
                {"endpoints": [endpoint]},
            )
            self.record(session, server, {})
        scanning = distributed.coordinate(self.cfg, self.protocol)
        self.assertEqual(scanning["phase"], "scanning")
        self.assertEqual(sum(len(v) for v in scanning["assignments"].values()), 1)

    def test_missing_or_online_participant_prevents_migration(self):
        self.queue()
        self.assertEqual(
            distributed.coordinate(self.cfg, self.protocol)["phase"], "waiting"
        )
        self.register()
        with distributed.locked(self.protocol / "online/innova.lock"):
            session = distributed.coordinate(self.cfg, self.protocol)
            self.assertEqual(session["online"], "innova")
        self.assert_identity(OLD, OU)

    def test_local_config_retains_server_specific_storage_settings(self):
        self.cfg.update(
            ignorePaths=["innova/ignored", "paradise/other"],
            embeddedDatabases={
                "innova/plugins/ChessCraft/database.mv.db": {"user": ""}
            },
        )
        local = distributed.local_config(self.cfg, "innova", self.directories["innova"])
        self.assertEqual(local["ignorePaths"], ["data/ignored"])
        self.assertEqual(
            local["embeddedDatabases"],
            {"data/plugins/ChessCraft/database.mv.db": {"user": ""}},
        )

    def test_distributed_progress_reads_participant_reports(self):
        job = self.queue(dry=True)
        self.register()
        self.until("done")
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            worker.main(["--config", str(self.config), "progress", job["id"]])
        reports = [json.loads(line) for line in output.getvalue().splitlines()]
        self.assertEqual({r["participant"] for r in reports}, set(self.cfg["servers"]))

    def test_failed_scan_persists_global_manifest(self):
        job = self.queue(dry=True)
        self.register()
        scanning = self.until("scanning")
        self.step_all(scanning, fail_server="innova")
        self.assertEqual(
            distributed.coordinate(self.cfg, self.protocol)["phase"], "failed"
        )
        report = common.load(
            worker.state(self.cfg) / "scans" / job["id"] / "manifest.json"
        )
        self.assertFalse(report["ready"])
        self.assertFalse(report["coverage"]["complete"])
        self.assertIn("error", report["participantResults"]["innova"])

    def test_failed_dry_run_does_not_block_next_job(self):
        failed = self.queue(dry=True)
        failed["status"] = "failed"
        failed["error"] = "discovery failed"
        second = dict(failed, id="second", old="AnotherOld", new="AnotherNew", status="queued")
        worker.setjobs(self.cfg, [failed, second])
        self.register()
        self.assertTrue(
            distributed.dry_run_failure(
                {"phase": "failed", "identity": failed}
            )
        )
        self.assertFalse(
            distributed.dry_run_failure(
                {"phase": "failed", "identity": dict(failed, dryRun=False)}
            )
        )
        session = distributed.coordinate(self.cfg, self.protocol)
        self.assertEqual(session["job"], "second")

    def test_overlapping_distributed_dry_run_is_rejected(self):
        self.queue()
        with self.assertRaisesRegex(ValueError, "overlaps"):
            self.queue(dry=True)

    def process(self, server, packaged=False, interactive=False):
        directory = self.directories[server]
        marker = directory / "started"
        command = [
            sys.executable,
            "-c",
            "import pathlib,signal,sys,time; pathlib.Path(sys.argv[1]).write_text('started'); signal.signal(signal.SIGTERM,lambda *a:sys.exit(0)); time.sleep(120)",
            str(marker),
        ]
        environment = dict(
            os.environ, PYTHONPATH=str(ENGINE / "vendor"), PYTHONDONTWRITEBYTECODE="1"
        )
        log = (self.root / (server + "-process-output.log")).open("w+")
        self.addCleanup(log.close)
        invocation = [
            sys.executable,
            "-B",
            str(ENGINE / "worker.py"),
            "--config",
            str(self.config),
            "launch",
            "--server",
            server,
            "--directory",
            str(directory),
            "--",
            *command,
        ]
        if packaged:
            (directory / "plugins").mkdir(exist_ok=True)
            shutil.copyfile(
                os.environ["HAMMERMIGRATE_JAR"], directory / "plugins/HammerMigrate.jar"
            )
            template = (ENGINE.parents[3] / "bin/start-server.sh").read_text()
            script = directory / "start-hammermigrate.sh"
            script.write_text(template.replace("/network/", str(self.shared) + "/"))
            environment["HAMMERMIGRATE_SERVER"] = server
            environment["PATH"] = str(self.root / "binaries")
            invocation = ["bash", str(script), *command]
        process = subprocess.Popen(
            invocation,
            env=environment,
            stdin=subprocess.PIPE if interactive else None,
            text=interactive,
            stdout=log,
            stderr=subprocess.STDOUT,
        )

        def stop():
            if process.poll() is None:
                process.send_signal(signal.SIGTERM)
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()

        self.addCleanup(stop)
        return process, marker, log

    def test_three_process_launch_barrier_and_signal_forwarding(self):
        self.queue()
        first, marker, log = self.process("innova")
        time.sleep(0.2)
        self.assertFalse(marker.exists())
        others = [self.process(s) for s in ("paradise", "lavender")]
        all_processes = [(first, marker, log), *others]
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline and not all(
            m.exists() for _, m, _ in all_processes
        ):
            if any(p.poll() is not None for p, _, _ in all_processes):
                break
            time.sleep(0.05)
        for p, m, l in all_processes:
            l.flush()
            l.seek(0)
            self.assertTrue(m.exists(), l.read())
        self.assert_identity(NEW, NU)
        for p, _, _ in all_processes:
            p.send_signal(signal.SIGTERM)
            self.assertEqual(p.wait(timeout=5), 0)

    @unittest.skipUnless(os.environ.get("HAMMERMIGRATE_JAR"), "Requires packaged JAR")
    def test_packaged_distributed_launch_and_rollback(self):
        runtime = self.config.parent / "python runtime" / "python"
        runtime.parent.mkdir()
        runtime.write_text("#!/bin/sh\nexec " + shlex.quote(sys.executable) + ' "$@"\n')
        runtime.chmod(0o755)
        self.cfg["python"] = "python runtime/python"
        common.save(self.config, self.cfg)
        binaries = self.root / "binaries"
        binaries.mkdir()
        for name in ("java", "bash", "dirname"):
            (binaries / name).symlink_to(Path(shutil.which(name)).resolve())
        job = self.queue()
        for directory in self.directories.values():
            world = directory / "world"
            world.mkdir()
            (world / "session.lock").write_bytes(b"\xe2\x98\x83")
        # A second independent job exercises releasing world locks between rounds.
        for directory in self.directories.values():
            (directory / "whitelist.json").write_text(
                json.dumps(
                    [{"name": "AnotherOld", "uuid": common.offline("AnotherOld")}]
                )
            )
        with contextlib.redirect_stdout(io.StringIO()):
            worker.main(
                ["--config", str(self.config), "queue", "AnotherOld", "AnotherNew"]
            )
        for reverse in (False, True):
            processes = [self.process(s, packaged=True) for s in self.cfg["servers"]]
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline and not all(
                m.exists() for _, m, _ in processes
            ):
                if any(p.poll() is not None for p, _, _ in processes):
                    break
                time.sleep(0.05)
            for p, marker, log in processes:
                log.seek(0)
                self.assertTrue(marker.exists(), log.read())
            self.assert_identity(OLD if reverse else NEW, OU if reverse else NU)
            for p, marker, _ in processes:
                p.send_signal(signal.SIGTERM)
                self.assertEqual(p.wait(timeout=5), 0)
                marker.unlink()
            if not reverse:
                with contextlib.redirect_stdout(io.StringIO()):
                    worker.main(["--config", str(self.config), "rollback", job["id"]])
