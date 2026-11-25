"""Deployment preflight, atomic rollback, shared launchers and runtime reuse."""

import contextlib
import importlib.util
import io
import json
import os
from pathlib import Path
import stat
import subprocess
import shlex
import signal
import time
import sys
import tempfile
import tarfile
import unittest
from unittest.mock import patch
import zipfile

PROJECT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("hm_deploy", PROJECT / "bin/deploy.py")
deploy = importlib.util.module_from_spec(spec)
spec.loader.exec_module(deploy)


class DeploymentTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="hm-deploy-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.shared = self.root / "network"
        self.servers = {name: self.root / name for name in ["alpha", "beta", "gamma"]}
        for path in self.servers.values():
            path.mkdir()
        self.profile = self.root / "deployment.json"
        self.jar = self.root / "HammerMigrate.jar"
        with zipfile.ZipFile(self.jar, "w") as jar:
            for name in [
                "plugin.yml",
                "engine/worker.py",
                "engine/distributed.py",
                "ink/ziip/hammer/migrate/Main.class",
            ]:
                jar.writestr(name, "test-package")
        self.runtime = self.root / "runtime-source"
        for name in [
            "python",
            "linux-x86_64-gnu/bin/python3",
            "linux-x86_64-musl/bin/python3",
        ]:
            path = self.runtime / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("#!/bin/sh\nexit 0\n")
            path.chmod(0o755)
        self.write_profile()
        self.output = io.StringIO()
        self.redirect = contextlib.redirect_stdout(self.output)
        self.redirect.__enter__()
        self.addCleanup(self.redirect.__exit__, None, None, None)

    def write_profile(self):
        self.profile.write_text(
            json.dumps(
                dict(
                    sharedDirectory="network",
                    runtimeSource="runtime-source",
                    servers={n: dict(directory=n) for n in self.servers},
                )
            )
        )

    def run_deploy(self, extra=()):
        return deploy.main(
            ["deploy", "--profile", str(self.profile), "--jar", str(self.jar), *extra]
        )

    def latest(self):
        return sorted(
            (self.shared / "HammerMigrate/deployments").glob("*/deployment.json")
        )[-1]

    def test_dry_run_writes_nothing_and_does_not_build(self):
        with patch.object(
            deploy.subprocess,
            "run",
            side_effect=AssertionError("dry-run must not build"),
        ):
            self.run_deploy(["--dry-run"])
        self.assertFalse(self.shared.exists())
        self.assertTrue(
            all(not (p / "plugins").exists() for p in self.servers.values())
        )

    def test_deploy_check_and_roll_back_without_changing_queue(self):
        self.run_deploy()
        cfg = deploy.settings(self.profile)
        queue = cfg["state"] / "queue.json"
        queue.write_text("[]")
        self.assertEqual(deploy.main(["check", "--profile", str(self.profile)]), 0)
        for path in self.servers.values():
            self.assertEqual(
                (path / "plugins/HammerMigrate.jar").read_bytes(), self.jar.read_bytes()
            )
            self.assertEqual(stat.S_IMODE((path / "plugins").stat().st_mode), 0o755)
            script = (path / "start-hammermigrate.sh").read_text()
            self.assertIn("HAMMERMIGRATE_DIRECTORY", script)
            self.assertIn("/network/HammerMigrate", script)
        manifest = deploy.read_json(self.latest())
        deploy.main(
            ["rollback", "--profile", str(self.profile), "--deployment", manifest["id"]]
        )
        self.assertEqual(queue.read_text(), "[]")
        self.assertTrue(
            all(
                not (p / "plugins/HammerMigrate.jar").exists()
                for p in self.servers.values()
            )
        )

    def test_mid_deployment_failure_restores_every_applied_file(self):
        self.run_deploy()
        original = {
            p: (p / "plugins/HammerMigrate.jar").read_bytes()
            for p in self.servers.values()
        }
        with zipfile.ZipFile(self.jar, "a") as jar:
            jar.writestr("version.txt", "next")
        actual = deploy.atomic
        count = []

        def fail(path, data, *args, **kwargs):
            if path.name == "HammerMigrate.jar":
                count.append(1)
                if len(count) == 2:
                    raise OSError("injected write failure")
            return actual(path, data, *args, **kwargs)

        with patch.object(deploy, "atomic", side_effect=fail):
            with self.assertRaisesRegex(OSError, "injected"):
                self.run_deploy()
        self.assertEqual(
            {
                p: (p / "plugins/HammerMigrate.jar").read_bytes()
                for p in self.servers.values()
            },
            original,
        )
        statuses = [
            deploy.read_json(p)["status"]
            for p in (self.shared / "HammerMigrate/deployments").glob(
                "*/deployment.json"
            )
        ]
        self.assertIn("rolled-back", statuses)

    def test_live_recovery_blocks_deployment(self):
        self.run_deploy()
        cfg = deploy.settings(self.profile)
        (cfg["state"] / "queue.json").write_text(
            json.dumps([dict(status="recovery_required")])
        )
        with self.assertRaisesRegex(ValueError, "recovery"):
            self.run_deploy()

    def test_rollback_rejects_manual_file_edits(self):
        self.run_deploy()
        manifest = deploy.read_json(self.latest())
        path = next(iter(self.servers.values())) / "plugins/HammerMigrate.jar"
        path.write_bytes(b"manual update")
        with self.assertRaisesRegex(ValueError, "changed after deployment"):
            deploy.main(
                [
                    "rollback",
                    "--profile",
                    str(self.profile),
                    "--deployment",
                    manifest["id"],
                ]
            )

    def test_existing_legacy_state_location_is_preserved(self):
        base = self.shared / "HammerMigrate"
        base.mkdir(parents=True)
        (base / "network.json").write_text(
            json.dumps(
                dict(
                    networkRoot="..",
                    servers=list(self.servers),
                    python="runtime/python",
                )
            )
        )
        self.run_deploy()
        cfg = deploy.settings(self.profile)
        self.assertEqual(cfg["state"], self.shared / "migrate/HammerMigrate")

    def test_release_bundle_is_self_contained_and_reproducible(self):
        args = type("Args", (), dict(jar=self.jar, output=self.root / "release"))()
        deploy.package(args)
        package = next(args.output.glob("*.tar.gz"))
        first = package.read_bytes()
        deploy.package(args)
        self.assertEqual(package.read_bytes(), first)
        with tarfile.open(package) as archive:
            names = archive.getnames()
            self.assertTrue(any(n.endswith("/HammerMigrate.jar") for n in names))
            self.assertTrue(any(n.endswith("/bin/deploy.py") for n in names))
            self.assertTrue(any(n.endswith("/release.json") for n in names))

    def test_profile_config_and_launcher_drift_fail_check(self):
        self.run_deploy()
        path = self.shared / "HammerMigrate/bin/start-server.sh"
        path.write_text("unexpected launcher")
        self.assertEqual(deploy.main(["check", "--profile", str(self.profile)]), 1)


@unittest.skipUnless(os.environ.get("HAMMERMIGRATE_JAR"), "Requires packaged JAR")
class DeploymentPackagedTests(unittest.TestCase):
    def test_deploy_launch_migrate_and_rollback_three_nodes(self):
        import test_engine as fixtures
        import common
        import worker

        temporary = tempfile.TemporaryDirectory(prefix="hm-packaged-deploy-")
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        source = root / "runtime"
        for name in [
            "python",
            "linux-x86_64-gnu/bin/python3",
            "linux-x86_64-musl/bin/python3",
        ]:
            path = source / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(
                "#!/bin/sh\nexec " + shlex.quote(sys.executable) + ' "$@"\n'
            )
            path.chmod(0o755)
        servers = {name: root / name for name in ["alpha", "beta", "gamma"]}
        for directory in servers.values():
            directory.mkdir()
            (directory / "usercache.json").write_text(
                json.dumps([dict(name=fixtures.OLD, uuid=fixtures.OU)])
            )
        shared = root / "network"
        profile = root / "deployment.json"
        profile.write_text(
            json.dumps(
                dict(
                    sharedDirectory=str(shared),
                    containerSharedDirectory=str(shared),
                    runtimeSource=str(source),
                    servers={n: dict(directory=str(p)) for n, p in servers.items()},
                    network=dict(
                        discoverRelatedSchemas=False,
                        participantPollSeconds=0.02,
                        requireCompleteCoverage=True,
                    ),
                )
            )
        )
        with contextlib.redirect_stdout(io.StringIO()):
            deploy.main(
                [
                    "deploy",
                    "--profile",
                    str(profile),
                    "--jar",
                    os.environ["HAMMERMIGRATE_JAR"],
                ]
            )
        config = shared / "HammerMigrate/network.json"
        for reverse in [False, True]:
            with contextlib.redirect_stdout(io.StringIO()):
                if not reverse:
                    worker.main(
                        ["--config", str(config), "queue", fixtures.OLD, fixtures.NEW]
                    )
                else:
                    worker.main(["--config", str(config), "rollback", job["id"]])
            processes = []
            try:
                for name, directory in servers.items():
                    log = (root / (name + ".log")).open("w+")
                    self.addCleanup(log.close)
                    command = [
                        str(directory / "start-hammermigrate.sh"),
                        sys.executable,
                        "-c",
                        "import pathlib,signal,sys,time;pathlib.Path('started').write_text('ok');signal.signal(signal.SIGTERM,lambda *a:sys.exit(0));time.sleep(60)",
                    ]
                    processes.append(
                        (
                            subprocess.Popen(
                                command,
                                stdout=log,
                                stderr=subprocess.STDOUT,
                                umask=0o077,
                            ),
                            directory,
                            log,
                        )
                    )
                deadline = time.monotonic() + 20
                while time.monotonic() < deadline and not all(
                    (d / "started").exists() for _, d, _ in processes
                ):
                    if any(p.poll() is not None for p, _, _ in processes):
                        break
                    time.sleep(0.05)
                for process, directory, log in processes:
                    log.seek(0)
                    self.assertTrue((directory / "started").exists(), log.read())
                    self.assertEqual(
                        stat.S_IMODE((directory / "started").stat().st_mode), 0o644
                    )
                    self.assertEqual(
                        json.loads((directory / "usercache.json").read_text())[0][
                            "uuid"
                        ],
                        fixtures.OU if reverse else fixtures.NU,
                    )
                cfg = deploy.settings(profile)
                job = common.load(cfg["state"] / "queue.json")[0]
                self.assertEqual(job["status"], "rolled_back" if reverse else "done")
            finally:
                for process, directory, log in processes:
                    if process.poll() is None:
                        process.send_signal(signal.SIGTERM)
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait()
                    if (directory / "started").exists():
                        (directory / "started").unlink()
