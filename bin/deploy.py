#!/usr/bin/env python3
"""Transactional host deployment. Containers only need Java, Bash and /network."""
import argparse
import contextlib
import datetime
import fcntl
import hashlib
import gzip
import io
import tarfile
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import uuid
import zipfile

PROJECT = Path(__file__).resolve().parents[1]


def digest(data):
    return hashlib.sha256(data).hexdigest()


def read_json(path):
    return json.loads(path.read_text())


def ensure_directory(path, mode=0o755, owner=None):
    if path.exists():
        return
    ancestor = next(p for p in path.parents if p.exists()).stat()
    owner = owner or (ancestor.st_uid, ancestor.st_gid)
    missing = []
    current = path
    while not current.exists():
        missing.append(current)
        current = current.parent
    for current in reversed(missing):
        current.mkdir(mode=mode)
        os.chmod(current, mode)
        if os.geteuid() == 0:
            os.chown(current, *owner)


def atomic(path, data, mode=0o600, owner=None):
    ensure_directory(path.parent, owner=owner)
    fd, name = tempfile.mkstemp(prefix=".hm-deploy-", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as output:
            output.write(data)
            output.flush()
            os.fsync(output.fileno())
        os.chmod(name, mode)
        if owner and os.geteuid() == 0:
            os.chown(name, *owner)
        os.replace(name, path)
        directory = os.open(path.parent, os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if os.path.exists(name):
            os.unlink(name)


def save(path, data):
    atomic(path, (json.dumps(data, ensure_ascii=False, indent=2) + "\n").encode())


def image(path):
    if path.is_symlink():
        raise ValueError("Deployment destination must not be a symlink: " + str(path))
    if not path.exists():
        return {"exists": False}
    if not path.is_file():
        raise ValueError("Deployment destination must be a file: " + str(path))
    stat = path.stat()
    return dict(
        exists=True,
        sha256=digest(path.read_bytes()),
        mode=stat.st_mode & 0o777,
        uid=stat.st_uid,
        gid=stat.st_gid,
    )


def settings(path):
    path = path.resolve()
    profile = read_json(path)
    shared = (path.parent / profile["sharedDirectory"]).resolve()
    servers = {}
    for name, value in profile["servers"].items():
        if not re.fullmatch(r"[A-Za-z0-9_-]+", name):
            raise ValueError("Invalid participant name: " + name)
        directory = (path.parent / value["directory"]).resolve()
        if not directory.is_dir() or directory.parent == directory:
            raise ValueError("Missing server data directory: " + str(directory))
        if any(
            directory == other
            or directory in other.parents
            or other in directory.parents
            for other in [shared, *servers.values()]
        ):
            raise ValueError("Shared and server data directories must not overlap")
        servers[name] = directory
    if not servers:
        raise ValueError("At least one server is required")
    mount = profile.get("containerSharedDirectory", "/network")
    if not mount.startswith("/") or any(c.isspace() for c in mount) or "\n" in mount:
        raise ValueError(
            "Container shared mount must be an absolute path without whitespace"
        )
    base = shared / "HammerMigrate"
    config = base / "network.json"
    old = read_json(config) if config.exists() else None
    network = dict(
        old
        or {
            "networkRoot": "..",
            "stateDirectory": "state",
            "startup": "external",
            "python": "runtime/python",
            "premiumDefault": False,
            "databaseEndpoints": [],
            "fieldRules": [],
            "ignorePaths": [],
            "discoverRelatedSchemas": True,
            "exclude": ["logs", "libraries", ".paper-remapped", "backups", "backup"],
        }
    )
    network.update(profile.get("network", {}))
    network.update(deployment="distributed", startup="external", servers=list(servers))
    if (base / network["networkRoot"]).resolve() != shared:
        raise ValueError("Distributed networkRoot must resolve to the shared directory")

    def state_for(cfg):
        if cfg.get("stateDirectory"):
            return (base / cfg["stateDirectory"]).resolve()
        return (base / cfg["networkRoot"] / "migrate/HammerMigrate").resolve()

    state = state_for(network)
    if shared not in state.parents:
        raise ValueError("State directory must be inside the shared mount")
    if old and state_for(old) != state:
        raise ValueError(
            "Existing state is preserved; relocate it separately while all participants are stopped"
        )
    return dict(
        profile=profile,
        path=path,
        shared=shared,
        base=base,
        config=config,
        old=old,
        network=network,
        servers=servers,
        mount=mount,
        state=state,
    )


def inspect_jar(path):
    with zipfile.ZipFile(path) as jar:
        required = {
            "plugin.yml",
            "engine/worker.py",
            "engine/distributed.py",
            "ink/ziip/hammer/migrate/Main.class",
        }
        if not required.issubset(jar.namelist()) or jar.testzip():
            raise ValueError("Invalid HammerMigrate package")
    return path.read_bytes()


def launcher(name, mount):
    return (
        "#!/usr/bin/env bash\nset -euo pipefail\numask 022\n"
        "export HAMMERMIGRATE_SERVER=" + shlex.quote(name) + "\n"
        'export HAMMERMIGRATE_DIRECTORY=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)\n'
        "export HAMMERMIGRATE_SHARED=" + shlex.quote(mount + "/HammerMigrate") + "\n"
        'exec bash "$HAMMERMIGRATE_SHARED/bin/start-server.sh" "$@"\n'
    ).encode()


def artifacts(cfg, jar):
    result = [
        (cfg["base"] / "HammerMigrate.jar", jar, 0o644),
        (
            cfg["base"] / "bin/start-server.sh",
            (PROJECT / "bin/start-server.sh").read_bytes(),
            0o755,
        ),
        (
            cfg["config"],
            (json.dumps(cfg["network"], ensure_ascii=False, indent=2) + "\n").encode(),
            image(cfg["config"]).get("mode", 0o600),
        ),
    ]
    for name, directory in cfg["servers"].items():
        result.extend(
            [
                (directory / "plugins/HammerMigrate.jar", jar, 0o644),
                (
                    directory / "start-hammermigrate.sh",
                    launcher(name, cfg["mount"]),
                    0o755,
                ),
            ]
        )
    return result


@contextlib.contextmanager
def coordinated(cfg):
    ensure_directory(cfg["base"], mode=0o750)
    ensure_directory(cfg["state"], mode=0o700)
    with (cfg["base"] / "deployment.lock").open("a+") as deployment, (
        cfg["state"] / "coordinator.lock"
    ).open("a+") as coordinator:
        fcntl.flock(deployment, fcntl.LOCK_EX | fcntl.LOCK_NB)
        fcntl.flock(coordinator, fcntl.LOCK_EX | fcntl.LOCK_NB)
        actual = read_json(cfg["config"]) if cfg["config"].exists() else None
        if actual != cfg["old"]:
            raise ValueError("Shared configuration changed; rerun deployment")
        queue = cfg["state"] / "queue.json"
        jobs = read_json(queue) if queue.exists() else []
        if any(
            j["status"] in ("running", "recovery_required", "rollback_queued")
            or j.get("distributedRound")
            for j in jobs
        ):
            raise ValueError(
                "Finish or cancel the active migration/recovery before deployment"
            )
        active = []
        for path in (cfg["state"] / "distributed/launchers").glob("*.lock"):
            with path.open("a+") as handle:
                try:
                    fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    active.append(path.stem)
        if cfg["old"] and set(cfg["old"]["servers"]) != set(cfg["servers"]) and active:
            raise ValueError("Stop all launchers before changing participant topology")
        yield active


def runtime_ready(cfg):
    runtime = cfg["base"] / "runtime"
    return all(
        (runtime / p).is_file() and os.access(runtime / p, os.X_OK)
        for p in [
            "python",
            "linux-x86_64-gnu/bin/python3",
            "linux-x86_64-musl/bin/python3",
        ]
    )


def ensure_runtime(cfg):
    if runtime_ready(cfg):
        return
    if (cfg["base"] / "runtime").exists():
        raise ValueError(
            "Existing runtime is incomplete; repair it instead of overwriting it"
        )
    source = cfg["profile"].get("runtimeSource")
    if source:
        source = (cfg["path"].parent / source).resolve()
        staging = cfg["base"] / (".runtime-" + uuid.uuid4().hex)
        try:
            shutil.copytree(source, staging, symlinks=True)
            if not all(
                (staging / p).is_file() and os.access(staging / p, os.X_OK)
                for p in [
                    "python",
                    "linux-x86_64-gnu/bin/python3",
                    "linux-x86_64-musl/bin/python3",
                ]
            ):
                raise ValueError("Runtime source is incomplete")
            staging.rename(cfg["base"] / "runtime")
        finally:
            if staging.exists():
                shutil.rmtree(staging)
    else:
        subprocess.run(
            [
                sys.executable,
                str(PROJECT / "bin/install-python-runtime.py"),
                "--directory",
                str(cfg["base"] / "runtime"),
            ],
            check=True,
        )
    if not runtime_ready(cfg):
        raise ValueError("Installed runtime is incomplete")


def store_blob(base, data):
    name = digest(data)
    path = base / "blobs" / name
    if not path.exists():
        atomic(path, data)
    elif digest(path.read_bytes()) != name:
        raise ValueError("Deployment blob checksum mismatch")
    return name


def blob(base, name):
    data = (base / "blobs" / name).read_bytes()
    if digest(data) != name:
        raise ValueError("Deployment blob checksum mismatch")
    return data


def restore(base, entry):
    target = Path(entry["path"])
    before = entry["before"]
    if before["exists"]:
        atomic(
            target,
            blob(base, before["sha256"]),
            before["mode"],
            (before["uid"], before["gid"]),
        )
    elif target.exists():
        target.unlink()


def deploy(cfg, args):
    bundled = PROJECT / "HammerMigrate.jar"
    jar_path = (
        args.jar.resolve()
        if args.jar
        else (
            bundled if bundled.is_file() else PROJECT / "target/HammerMigrate-1.0.0.jar"
        )
    )
    if args.dry_run:
        print(
            json.dumps(
                dict(
                    mode="dry-run; no files changed",
                    shared=str(cfg["base"]),
                    servers={n: str(p) for n, p in cfg["servers"].items()},
                    jar=str(jar_path),
                    build=args.jar is None and (PROJECT / "pom.xml").is_file(),
                    installRuntime=not runtime_ready(cfg),
                    state=str(cfg["state"]),
                ),
                indent=2,
            )
        )
        return
    if not args.jar and (PROJECT / "pom.xml").is_file():
        subprocess.run(
            ["mvn", "-B", "-ntp", "-f", str(PROJECT / "pom.xml"), "clean", "verify"],
            check=True,
        )
    jar = inspect_jar(jar_path)
    with coordinated(cfg) as active:
        history = cfg["base"] / "deployments"
        for manifest in history.glob("*/deployment.json"):
            if read_json(manifest)["status"] == "applying":
                raise ValueError(
                    "Interrupted deployment requires rollback: " + manifest.parent.name
                )
        ensure_runtime(cfg)
        deploy_id = (
            datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%S")
            + "-"
            + uuid.uuid4().hex[:8]
        )
        directory = history / deploy_id
        directory.mkdir(parents=True, mode=0o700)
        files = []
        for target, data, mode in artifacts(cfg, jar):
            before = image(target)
            if before.get("sha256") == digest(data) and before.get("mode") == mode:
                continue
            if before["exists"]:
                store_blob(history, target.read_bytes())
            after = dict(exists=True, sha256=store_blob(history, data), mode=mode)
            owner = (
                target.stat()
                if target.exists()
                else next(p for p in target.parents if p.exists()).stat()
            )
            after.update(uid=owner.st_uid, gid=owner.st_gid)
            files.append(dict(path=str(target), before=before, after=after))
        if not files:
            directory.rmdir()
            print(json.dumps(dict(status="unchanged", jarSha256=digest(jar)), indent=2))
            return
        manifest = dict(
            id=deploy_id,
            status="applying",
            jarSha256=digest(jar),
            files=files,
            activeLaunchers=active,
            servers=list(cfg["servers"]),
        )
        save(directory / "deployment.json", manifest)
        applied = []
        try:
            for entry in files:
                target = Path(entry["path"])
                if image(target) != entry["before"]:
                    raise ValueError("File changed during deployment: " + str(target))
                after = entry["after"]
                atomic(
                    target,
                    blob(history, after["sha256"]),
                    after["mode"],
                    (after["uid"], after["gid"]),
                )
                applied.append(entry)
            manifest["status"] = "deployed"
            save(directory / "deployment.json", manifest)
        except BaseException:
            for entry in reversed(applied):
                restore(history, entry)
            manifest["status"] = "rolled-back"
            save(directory / "deployment.json", manifest)
            raise
        print(
            json.dumps(
                dict(
                    deployment=deploy_id,
                    changedFiles=len(files),
                    jarSha256=digest(jar),
                    activeLaunchers=active,
                    activation="Next coordinated server start; containers are not restarted",
                ),
                indent=2,
            )
        )


def rollback(cfg, args):
    if not re.fullmatch(r"[0-9]{8}T[0-9]{6}-[0-9a-f]{8}", args.deployment):
        raise ValueError("Invalid deployment ID")
    with coordinated(cfg):
        history = cfg["base"] / "deployments"
        path = history / args.deployment / "deployment.json"
        manifest = read_json(path)
        if manifest["status"] == "rolled-back":
            print("Deployment is already rolled back")
            return
        for entry in manifest["files"]:
            if image(Path(entry["path"])) not in (entry["before"], entry["after"]):
                raise ValueError("File changed after deployment: " + entry["path"])
        for entry in reversed(manifest["files"]):
            if image(Path(entry["path"])) not in (entry["before"], entry["after"]):
                raise ValueError("File changed during rollback: " + entry["path"])
            restore(history, entry)
        manifest["status"] = "rolled-back"
        save(path, manifest)
        print("Rolled back deployment " + args.deployment)


def check(cfg):
    paths = [
        cfg["base"] / "HammerMigrate.jar",
        *[p / "plugins/HammerMigrate.jar" for p in cfg["servers"].values()],
    ]
    versions = {str(p): digest(p.read_bytes()) if p.is_file() else None for p in paths}
    common = (
        bool(versions)
        and None not in versions.values()
        and len(set(versions.values())) == 1
    )
    launchers = all(
        (p / "start-hammermigrate.sh").is_file() for p in cfg["servers"].values()
    )
    result = dict(
        jars=versions,
        consistentJars=common,
        runtimeReady=runtime_ready(cfg),
        launchersPresent=launchers,
        sharedLauncher=(cfg["base"] / "bin/start-server.sh").is_file(),
        configuredServers=cfg["old"].get("servers", []) if cfg["old"] else [],
        stateDirectory=str(cfg["state"]),
    )
    result["configMatchesProfile"] = cfg["old"] == cfg["network"]
    result["sharedLauncherCurrent"] = (
        result["sharedLauncher"]
        and (cfg["base"] / "bin/start-server.sh").read_bytes()
        == (PROJECT / "bin/start-server.sh").read_bytes()
    )
    result["serverLaunchersCurrent"] = all(
        (p / "start-hammermigrate.sh").is_file()
        and (p / "start-hammermigrate.sh").read_bytes() == launcher(n, cfg["mount"])
        for n, p in cfg["servers"].items()
    )
    result["ok"] = all(
        [
            common,
            result["runtimeReady"],
            launchers,
            result["sharedLauncher"],
            result["configuredServers"] == list(cfg["servers"]),
            result["configMatchesProfile"],
            result["sharedLauncherCurrent"],
            result["serverLaunchersCurrent"],
        ]
    )
    print(json.dumps(result, indent=2))
    return 0 if result["ok"] else 1


def package(args):
    if not args.jar:
        subprocess.run(
            ["mvn", "-B", "-ntp", "-f", str(PROJECT / "pom.xml"), "clean", "verify"],
            check=True,
        )
    source = (
        args.jar.resolve() if args.jar else PROJECT / "target/HammerMigrate-1.0.0.jar"
    )
    data = inspect_jar(source)
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    prefix = "HammerMigrate-" + digest(data)[:12]
    files = {"HammerMigrate.jar": (data, 0o644)}
    for name in [
        "hammermigrate",
        "README.md",
        "THIRD_PARTY.md",
        "bin/deploy.py",
        "bin/start-server.sh",
        "bin/start-after-migration.sh",
        "bin/install-python-runtime.py",
        "bin/python-runtime.sh",
    ]:
        path = PROJECT / name
        files[name] = (
            path.read_bytes(),
            0o755 if name == "hammermigrate" or name.startswith("bin/") else 0o644,
        )
    for directory in ["docs", "examples"]:
        for path in sorted((PROJECT / directory).rglob("*")):
            if path.is_file():
                files[str(path.relative_to(PROJECT))] = (path.read_bytes(), 0o644)
    release = dict(
        jarSha256=digest(data), files={n: digest(v[0]) for n, v in files.items()}
    )
    files["release.json"] = ((json.dumps(release, indent=2) + "\n").encode(), 0o644)
    stream = io.BytesIO()
    with gzip.GzipFile(fileobj=stream, mode="wb", mtime=0) as compressed:
        with tarfile.open(fileobj=compressed, mode="w") as archive:
            for name, (contents, mode) in sorted(files.items()):
                info = tarfile.TarInfo(prefix + "/" + name)
                info.size = len(contents)
                info.mode = mode
                info.mtime = 0
                archive.addfile(info, io.BytesIO(contents))
    destination = output / (prefix + ".tar.gz")
    atomic(destination, stream.getvalue(), 0o644)
    atomic(
        output / (prefix + ".sha256"),
        (digest(stream.getvalue()) + "  " + destination.name + "\n").encode(),
        0o644,
    )
    print(
        json.dumps(
            dict(package=str(destination), sha256=digest(stream.getvalue())), indent=2
        )
    )
    return 0


def main(argv=None):
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("package")
    p.add_argument("--jar", type=Path)
    p.add_argument("--output", type=Path, default=PROJECT / "dist")
    for action in ("deploy", "check", "rollback"):
        p = sub.add_parser(action)
        p.add_argument("--profile", type=Path, required=True)
        if action == "deploy":
            p.add_argument("--jar", type=Path)
            p.add_argument("--dry-run", action="store_true")
        if action == "rollback":
            p.add_argument("--deployment", required=True)
    args = parser.parse_args(argv)
    if args.command == "package":
        return package(args)
    cfg = settings(args.profile)
    if args.command == "deploy":
        deploy(cfg, args)
    elif args.command == "rollback":
        rollback(cfg, args)
    else:
        return check(cfg)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (
        ValueError,
        OSError,
        subprocess.CalledProcessError,
        zipfile.BadZipFile,
    ) as error:
        print("HammerMigrate deployment failed:", error, file=sys.stderr)
        sys.exit(1)
