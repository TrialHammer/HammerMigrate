#!/usr/bin/env python3
"""Shared, startup-only offline rename coordinator. No live-server mutations."""

import argparse, collections, contextlib, datetime, gzip, json, os, re, sys, urllib.request, uuid
from pathlib import Path
from common import (
    Lock,
    Journal,
    load,
    save,
    write,
    sha,
    file_sha,
    dumps,
    decode,
    offline,
    patch,
    patch_bytes,
    safe_path,
    syncdir,
    ignored_path,
)
from identity import Identity
from login import premium_errors
from discovery import configs, quiescent, related_databases
from files import transform, moves, sparse_patch
from database import Database
import database, nbt
from scanning import ScanSession, file_inventory


def stamp():
    return (
        datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%S")
        + "-"
        + uuid.uuid4().hex[:8]
    )


def state(cfg):
    if cfg.get("stateDirectory"):
        return Path(cfg["stateDirectory"])
    return Path(cfg["networkRoot"]) / "migrate/HammerMigrate"


def jobs(cfg):
    p = state(cfg) / "queue.json"
    return load(p) if p.exists() else []


def blocking_failures(items):
    """Return failed jobs that may have changed storage.

    A distributed dry-run never enters the applying phase, so its failed
    discovery result cannot leave a partial mutation behind. It must not keep
    the external server launcher from starting the next queued job.
    """
    return [
        j
        for j in items
        if j.get("status") == "failed"
        and not j.get("dryRun")
        and not j.get("preflightBlocked")
    ]


def setjobs(cfg, items):
    save(state(cfg) / "queue.json", items)


def job_identity(cfg, job):
    return Identity(
        job["old"],
        job["new"],
        job["premium"],
        cfg.get("fieldRules", []),
        job.get("merge", False),
    )


def endpoint_public(e):
    return {k: v for k, v in e.items() if k not in ("user", "password", "ssl")}


def endpoint_runtime(cfg, e):
    if "_configuredIndex" in e:
        return dict(cfg["databaseEndpoints"][e["_configuredIndex"]], **e)
    if e.get("kind") in ("sqlite", "h2", "hsqldb"):
        return dict(e, **cfg.get("embeddedDatabases", {}).get(e["path"], {}))
    return e


def premium(ident):
    if not ident.premium:
        return
    url = "https://api.mojang.com/users/profiles/minecraft/" + ident.new
    with urllib.request.urlopen(url, timeout=10) as response:
        profile = json.load(response)
    if profile.get("name", "") != ident.new:
        raise ValueError(
            "Target must use the exact canonical Mojang name, including case"
        )
    ident.premium_uuid = str(uuid.UUID(profile["id"]))


def path_proof(p):
    if p.is_file():
        return {"sha256": file_sha(p)}
    result = []
    for child in sorted(p.rglob("*")):
        if child.is_symlink():
            raise ValueError("Symlink in moved directory " + str(p))
        if child.is_file():
            result.append([str(child.relative_to(p)), file_sha(child)])
        elif child.is_dir():
            result.append([str(child.relative_to(p)) + "/", None])
        else:
            raise ValueError("Nonregular entry in moved directory")
    return {"tree_sha256": sha(dumps(result).encode())}


def discover_plan(cfg, job, folder, fresh=False):
    passes = cfg.get("scanMaxPasses", 3)
    if isinstance(passes, bool) or not isinstance(passes, int) or not 1 <= passes <= 10:
        raise ValueError("scanMaxPasses must be an integer between 1 and 10")
    for attempt in range(passes):
        ident = job_identity(cfg, job)
        premium(ident)
        try:
            with ScanSession(cfg, ident, folder, fresh) as scan:
                report, plan = build_plan(cfg, job, Path(folder), ident, scan)
        except FileNotFoundError:
            if attempt + 1 == passes:
                raise
            continue
        if not report["coverage"].get("scopeChanged") and not report["coverage"].get(
            "changedDuringScan"
        ):
            return report, plan
        if attempt + 1 == passes:
            return report, plan
        print(
            "Storage scope changed; rediscovering and scanning current storage",
            flush=True,
        )
    raise AssertionError("Unreachable scan state")


def build_plan(cfg, job, folder, ident, scan):
    manifest = configs(cfg)
    for i, e in enumerate(cfg.get("databaseEndpoints", [])):
        for discovered in manifest["endpoints"]:
            if (
                discovered.get("host") == e.get("host")
                and discovered.get("database") == e.get("database")
                and int(discovered.get("port", 3306)) == int(e.get("port", 3306))
            ):
                discovered["_configuredIndex"] = i
    related_databases(cfg, manifest)
    if "_mysqlOfferedScope" in cfg:
        from discovery import ScopeChanged

        actual_scope = sorted(
            [ep.get("host"), int(ep.get("port", 3306)), ep["database"]]
            for ep in manifest["endpoints"]
        )
        if actual_scope != sorted(cfg["_mysqlOfferedScope"]):
            raise ScopeChanged("Database discovery changed; redistribute ownership")
    if "_mysqlScope" in cfg:
        manifest["endpoints"] = [
            ep
            for ep in manifest["endpoints"]
            if [ep.get("host"), int(ep.get("port", 3306)), ep["database"]]
            in cfg["_mysqlScope"]
        ]
    ignored = cfg.get("ignorePaths", [])
    manifest["files"] = [
        r for r in manifest["files"] if not ignored_path(cfg, r["path"])
    ]
    errors = [
        e for e in manifest["discoveryErrors"] if not ignored_path(cfg, e["path"])
    ]
    errors.extend(
        {"databaseDiscovery": error}
        for error in manifest.get("databaseDiscoveryErrors", [])
    )
    ignored_scan_errors = []
    ignored_scan_paths = set()
    actions = []
    physical = []
    relational = []
    source_evidence = []
    root = Path(cfg["networkRoot"])
    print(
        "Discovering",
        len(manifest["files"]),
        "files and",
        len(manifest["endpoints"]),
        "MySQL endpoints",
        flush=True,
    )
    initial_inventory = file_inventory(cfg, manifest)
    save(
        folder / "inventory.json",
        dict(
            manifest,
            endpoints=[endpoint_public(e) for e in manifest["endpoints"]],
            fileSignatures=initial_inventory,
        ),
    )
    scan.update(totalFiles=len(manifest["files"]), force=True)
    file_dbs = []
    covered_paths = set()
    failed_paths = set()
    external_owners = {}
    external_digests = {}
    physical_files = []
    for item in manifest["files"]:
        if item["format"] in ("sqlite", "h2", "hsqldb"):
            file_dbs.append(
                dict(
                    {"kind": item["format"], "path": item["path"]},
                    **cfg.get("embeddedDatabases", {}).get(item["path"], {})
                )
            )
        elif item["format"] != "external-chunk":
            physical_files.append(item)
    for item, result, error in scan.scan_files(physical_files):
        kind = item["format"]
        try:
            if error is not None:
                raise error
            actions.extend(result["actions"])
            physical.append(result["physical"])
            covered_paths.add(item["path"])
            for dep in result["dependencies"]:
                external_owners.setdefault(dep, []).append(item["path"])
            external_digests.update(result["dependencies"])
            if result["diagnostics"]["changes"]:
                source_evidence.append(item["path"])
        except Exception as exc:
            if kind == "region":
                ignored_scan_paths.add(item["path"])
                ignored_scan_errors.append(
                    {
                        "path": item["path"],
                        "error": str(exc)[:400],
                        "reason": "region scan error explicitly ignored",
                    }
                )
                continue
            failed_paths.add(item["path"])
            scan.progress["failedFiles"] += 1
            errors.append({"path": item["path"], "error": str(exc)[:400]})
    for item in manifest["files"]:
        if item["format"] != "external-chunk":
            continue
        try:
            if item["path"] in external_owners:
                covered_paths.add(item["path"])
                scan.progress["completedFiles"] += 1
                physical.append(
                    dict(
                        item,
                        scan="referenced-external-chunk",
                        parents=external_owners[item["path"]],
                        sha256=external_digests[item["path"]],
                        changed=any(a["path"] == item["path"] for a in actions),
                        residualChecked=True,
                    )
                )
                continue
            result = scan.scan_file(item)
            actions.extend(result["actions"])
            physical.append(result["physical"])
            covered_paths.add(item["path"])
            if result["diagnostics"]["changes"]:
                source_evidence.append(item["path"])
        except Exception as exc:
            failed_paths.add(item["path"])
            scan.progress["failedFiles"] += 1
            errors.append({"path": item["path"], "error": str(exc)[:400]})
    scan.update(
        phase="databases",
        totalDatabases=len(manifest["endpoints"]) + len(file_dbs),
        force=True,
    )
    for ep in manifest["endpoints"] + file_dbs:
        scan.update(
            phase="databases", current=ep.get("database", ep.get("path")), force=True
        )
        db = None
        try:
            db = Database(cfg, endpoint_runtime(cfg, ep), readonly=True)
            ops, inventory = db.plan(ident, progress=scan.update)
            if ops:
                db.validate(ops)
                actions.append(
                    {
                        "kind": "database",
                        "endpoint": endpoint_public(ep),
                        "operations": ops,
                        "schemaSha256": db.schema_signature(ops),
                        "identity": {
                            "old": ident.old,
                            "new": ident.new,
                            "premium": ident.premium,
                            "merge": ident.merge,
                            "premiumUuid": ident.premium_uuid,
                        },
                    }
                )
                source_evidence.append(ep.get("database", ep.get("path")))
            scan.progress["completedDatabases"] += 1
            if "path" in ep:
                covered_paths.add(ep["path"])
                scan.progress["completedFiles"] += 1
            relational.append(
                {
                    "endpoint": endpoint_public(ep),
                    "tables": inventory,
                    "changedRows": len(ops),
                    "validation": (
                        (
                            "engine-copy-migrate-and-rollback"
                            if db.kind != "mysql"
                            else "unique-keys-preflight; constraints-checked-in-transaction"
                        )
                        if ops
                        else "no changes"
                    ),
                }
            )
        except Exception as exc:
            if ep.get("kind") == "sqlite" and db is None:
                ignored_scan_paths.add(ep["path"])
                ignored_scan_errors.append(
                    {
                        "path": ep.get("path"),
                        "error": str(exc)[:500],
                        "reason": "SQLite open error explicitly ignored",
                    }
                )
                continue
            scan.progress["failedDatabases"] += 1
            if "path" in ep:
                failed_paths.add(ep["path"])
                scan.progress["failedFiles"] += 1
            errors.append({"endpoint": endpoint_public(ep), "error": str(exc)[:500]})
        finally:
            if db:
                db.rollback()
                db.close()
    rename = moves(cfg, ident)
    actions += rename
    if rename:
        source_evidence.extend(r["source"] for r in rename)
    # A target-name hit or game UUID hit anywhere authoritative blocks the entire job.
    if ident.premium and not cfg.get("_distributedPartial"):
        errors.extend(
            premium_errors(
                manifest.get("login", []),
                ident.auth_accounts,
                any(
                    Path(item["path"]).name == "premium_names.cache"
                    for item in manifest["files"]
                ),
            )
        )
    if len(ident.auth_accounts) > 1:
        errors.append(
            {
                "error": "Multiple source AuthMe accounts found; select the authoritative database scope"
            }
        )
    if not source_evidence and not cfg.get("_distributedPartial"):
        errors.append({"error": "No source player data found"})
    paths = [r["path"] for r in actions if r["kind"] == "file"]
    if len(paths) != len(set(paths)):
        errors.append({"error": "Same physical file planned by multiple adapters"})
    scan.update(phase="coverage-verification", force=True)
    final_manifest = configs(cfg)
    related_databases(cfg, final_manifest)
    if "_mysqlOfferedScope" in cfg:
        from discovery import ScopeChanged

        actual_scope = sorted(
            [ep.get("host"), int(ep.get("port", 3306)), ep["database"]]
            for ep in final_manifest["endpoints"]
        )
        if actual_scope != sorted(cfg["_mysqlOfferedScope"]):
            raise ScopeChanged(
                "Database discovery changed during scan; redistribute ownership"
            )
    if "_mysqlScope" in cfg:
        final_manifest["endpoints"] = [
            ep
            for ep in final_manifest["endpoints"]
            if [ep.get("host"), int(ep.get("port", 3306)), ep["database"]]
            in cfg["_mysqlScope"]
        ]
    final_inventory = file_inventory(cfg, final_manifest)
    changed_during_scan = sorted(
        path
        for path in initial_inventory.keys() | final_inventory.keys()
        if initial_inventory.get(path) != final_inventory.get(path)
    )
    scope_before = {
        key: sorted(dumps(v) for v in manifest.get(key, []))
        for key in ("files", "login", "listeners")
    }
    scope_after = {
        key: sorted(dumps(v) for v in final_manifest.get(key, []))
        for key in ("files", "login", "listeners")
    }

    def endpoints_snapshot(endpoints):
        return sorted(
            dumps(
                {
                    k: v
                    for k, v in endpoint_public(ep).items()
                    if k != "_configuredIndex"
                }
            )
            for ep in endpoints
        )

    scope_changed = scope_before != scope_after or endpoints_snapshot(
        manifest["endpoints"]
    ) != endpoints_snapshot(final_manifest["endpoints"])
    missing = sorted(
        {item["path"] for item in manifest["files"]}
        - covered_paths
        - failed_paths
        - ignored_scan_paths
    )
    if changed_during_scan or scope_changed:
        errors.append(
            {
                "error": "Storage changed during scan; rerun dry-run",
                "changedPaths": changed_during_scan,
                "scopeChanged": scope_changed,
            }
        )
    if missing:
        errors.append(
            {"error": "Discovered files lack a completed scan", "paths": missing}
        )
    if final_manifest["discoveryErrors"] or final_manifest.get(
        "databaseDiscoveryErrors"
    ):
        errors.append(
            {
                "error": "Discovery recheck failed",
                "discoveryErrors": final_manifest["discoveryErrors"],
                "databaseDiscoveryErrors": final_manifest.get(
                    "databaseDiscoveryErrors", []
                ),
            }
        )
    coverage = {
        "scope": "configured servers and discovered databases, with recorded exclusions",
        "discoveredFiles": len(manifest["files"]),
        "coveredFiles": len(covered_paths),
        "failedFiles": len(failed_paths),
        "unaccountedFiles": missing,
        "discoveredDatabases": len(manifest["endpoints"]) + len(file_dbs),
        "coveredDatabases": scan.progress["completedDatabases"],
        "failedDatabases": scan.progress["failedDatabases"],
        "changedDuringScan": changed_during_scan,
        "scopeChanged": scope_changed,
        "cacheHits": scan.progress["cacheHits"],
        "parsedFiles": scan.progress["parsedFiles"],
        "excludedEntries": len(manifest.get("skipped", [])),
        "preservedReferences": len(ident.preserved),
        "unresolvedReferences": len(ident.unknown),
        "collisionReferences": len(ident.collisions),
        "accounted": not (
            missing
            or failed_paths
            or changed_during_scan
            or scope_changed
            or scan.progress["failedDatabases"]
            or errors
        ),
    }
    coverage["ignoredFiles"] = sorted(ignored_scan_paths)
    coverage["complete"] = coverage["accounted"] and not ignored_scan_errors
    acceptable_coverage = (
        coverage["complete"]
        if cfg.get("requireCompleteCoverage", False)
        else coverage["accounted"]
    )
    report = {
        "version": 2,
        "state": "complete",
        "job": job,
        "coverage": coverage,
        "sourceUuid": ident.old_uuid,
        "targetUuid": ident.new_uuid,
        "premiumLogin": ident.premium,
        "premiumUuid": ident.premium_uuid,
        "loginConfiguration": manifest.get("login", []),
        "physical": physical,
        "databases": relational,
        "pathChanges": rename,
        "preserved": ident.preserved,
        "collisions": ident.collisions,
        "unresolved": ident.unknown,
        "errors": errors,
        "ignoredScanErrors": ignored_scan_errors,
        "authAccounts": ident.auth_accounts,
        "sourceEvidence": source_evidence,
        "ready": acceptable_coverage
        and not (errors or ident.collisions or ident.unknown),
        "actions": len(actions),
        "ignoredPaths": ignored,
        "skipped": manifest.get("skipped", []),
        "databaseDiscoveryErrors": manifest.get("databaseDiscoveryErrors", []),
    }
    report["completion"] = (
        "complete"
        if report["ready"] and coverage["complete"]
        else "with-exclusions" if report["ready"] else "blocked"
    )
    folder.mkdir(parents=True, exist_ok=True)
    payload = {
        "job": job,
        "actions": actions,
        "premiumUuid": ident.premium_uuid,
        "listeners": manifest["listeners"],
    }
    blob = gzip.compress(dumps(payload).encode(), mtime=0)
    write(folder / "plan.json.gz", blob)
    save(folder / "plan-sha256.json", {"sha256": sha(blob)})
    save(folder / "coverage.json", coverage)
    save(folder / "manifest.json", report)
    print(
        "Manifest",
        folder / "manifest.json",
        "ready=" + str(report["ready"]),
        "actions=" + str(len(actions)),
        "collisions=" + str(len(ident.collisions)),
        "unresolved=" + str(len(ident.unknown)),
        "errors=" + str(len(errors)),
        flush=True,
    )
    scan.update(
        state="complete",
        phase="complete",
        ready=report["ready"],
        manifest=str(folder / "manifest.json"),
        force=True,
    )
    return report, payload


def read_plan(folder):
    blob = (folder / "plan.json.gz").read_bytes()
    if sha(blob) != load(folder / "plan-sha256.json")["sha256"]:
        raise ValueError("Recorded plan checksum changed")
    return json.loads(gzip.decompress(blob), object_hook=decode)


def perform(cfg, action, reverse=False, proof=None):
    if action["kind"] == "file":
        p = safe_path(cfg["networkRoot"], action["path"])
        write(p, patch_bytes(p.read_bytes(), action["patch"], reverse))
    elif action["kind"] == "database":
        db = Database(cfg, endpoint_runtime(cfg, action["endpoint"]))
        try:
            if action.get("schemaSha256") != db.schema_signature(action["operations"]):
                raise ValueError("Database schema changed since migration plan")
            guard = action.get("identity")
            identity = job_identity(cfg, guard) if guard and not reverse else None
            if identity:
                identity.premium_uuid = guard.get("premiumUuid")
            db.mutate(action["operations"], reverse, identity=identity)
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()
    elif action["kind"] == "move":
        source = safe_path(
            cfg["networkRoot"], action["target" if reverse else "source"]
        )
        target = safe_path(
            cfg["networkRoot"], action["source" if reverse else "target"]
        )
        if source.exists() and target.exists():
            raise ValueError("Both move paths exist")
        current = source if source.exists() else target
        if not current.exists():
            raise ValueError("Both move paths missing")
        if path_proof(current) != proof:
            raise ValueError(
                "Moved content changed; rollback/migration stopped at " + str(current)
            )
        if source.exists():
            target.parent.mkdir(parents=True, exist_ok=True)
            os.rename(source, target)
            syncdir(source.parent)
            syncdir(target.parent)
    else:
        raise ValueError("Unknown action kind")


def rollback_run(cfg, folder):
    plan = read_plan(folder)
    journal = Journal(folder / "journal.jsonl")
    events = journal.events()
    if any(e["event"] == "rollback_complete" for e in events):
        return
    proof = {e["index"]: e["proof"] for e in events if e["event"] == "move_proof"}
    intents = [e["index"] for e in events if e["event"] == "intent"]
    undone = {e["index"] for e in events if e["event"] == "undone"}
    journal.append("rollback_started")
    for i in reversed(intents):
        if i in undone:
            continue
        perform(cfg, plan["actions"][i], True, proof.get(i))
        journal.append("undone", index=i)
    journal.append("rollback_complete")


def apply_run(cfg, folder, plan):
    journal = Journal(folder / "journal.jsonl")
    journal.append("started")
    try:
        for i, action in enumerate(plan["actions"]):
            proof = None
            if action["kind"] == "move":
                proof = path_proof(safe_path(cfg["networkRoot"], action["source"]))
                journal.append("move_proof", index=i, proof=proof)
            journal.append("intent", index=i)
            perform(cfg, action, proof=proof)
            journal.append("done", index=i)
        journal.append("committed")
    except BaseException:
        journal.append("failed")
        rollback_run(cfg, folder)
        raise


def process(cfg, current=None, fresh=False):
    items = jobs(cfg)
    pending = [
        j
        for j in items
        if j["status"] in ("queued", "running", "rollback_queued", "recovery_required")
    ]
    if blocking_failures(items):
        raise ValueError("Failed jobs require cancel <id> or retry <id> before startup")
    if not pending:
        return
    manifest = configs(cfg)
    held = quiescent(cfg, manifest, current)
    try:
        for job in pending:
            folder = state(cfg) / "runs" / job["id"]
            if job["status"] == "rollback_queued":
                rollback_run(cfg, folder)
                job["status"] = "rolled_back"
                setjobs(cfg, items)
                print("Rolled back", job["id"])
                continue
            if job["status"] in ("running", "recovery_required"):
                if (folder / "journal.jsonl").exists():
                    events = Journal(folder / "journal.jsonl").events()
                    if any(e["event"] == "committed" for e in events) and not any(
                        e["event"] == "rollback_started" for e in events
                    ):
                        job["status"] = "done"
                        setjobs(cfg, items)
                        continue
                    rollback_run(cfg, folder)
                job["status"] = "failed"
                job["error"] = (
                    "Interrupted job recovered; inspect run before requeueing"
                )
                setjobs(cfg, items)
                raise ValueError(job["error"])
            try:
                report, plan = discover_plan(cfg, job, folder, fresh=fresh)
                if not report["ready"]:
                    raise ValueError(
                        "Manifest contains collisions, unsupported matches or errors"
                    )
                # Repeat network quiescence just before the first intent.
                quiescent(cfg, manifest, current, world_locks=False)
                job["status"] = "running"
                setjobs(cfg, items)
                apply_run(cfg, folder, plan)
                job["status"] = "done"
                setjobs(cfg, items)
                print("Completed", job["id"])
            except Exception as exc:
                events = Journal(folder / "journal.jsonl").events()
                job["status"] = (
                    "recovery_required"
                    if any(e["event"] == "intent" for e in events)
                    and not any(e["event"] == "rollback_complete" for e in events)
                    else "failed"
                )
                job["error"] = str(exc)
                job["preflightBlocked"] = not any(
                    e["event"] == "intent" for e in events
                )
                setjobs(cfg, items)
                if not job["preflightBlocked"]:
                    raise
                print(
                    "Migration deferred; preflight made no changes:",
                    job["id"],
                    str(exc),
                    flush=True,
                )
    finally:
        for f in held:
            f.close()


def configure(path, args):
    if args.command == "init":
        if path.exists():
            raise ValueError("Config already exists")
        root = Path(args.root).resolve()
        servers = [
            p.name
            for p in root.iterdir()
            if p.is_dir()
            and not p.is_symlink()
            and ((p / "server.properties").exists() or (p / "BungeeCord.jar").exists())
        ]
        save(
            path,
            {
                "networkRoot": str(root),
                "servers": servers,
                "python": "python3",
                "startup": "external",
                "premiumDefault": False,
                "maxFileMiB": 256,
                "databaseEndpoints": [],
                "fieldRules": [],
                "embeddedDatabases": {},
                "discoverRelatedSchemas": True,
                "ignorePaths": [],
                "exclude": [
                    "logs",
                    "libraries",
                    ".paper-remapped",
                    "backups",
                    "backup",
                    "history-backups",
                ],
            },
        )
        print("Initialized", path, "servers=" + ",".join(servers))
        return None
    cfg = load(path)
    cfg["networkRoot"] = str((path.parent / cfg["networkRoot"]).resolve())
    if cfg.get("stateDirectory"):
        cfg["stateDirectory"] = str((path.parent / cfg["stateDirectory"]).resolve())
    if not cfg.get("servers"):
        raise ValueError("Configure at least one server")
    for s in cfg["servers"]:
        safe_path(cfg["networkRoot"], s)
    for rule in cfg.get("fieldRules", []):
        re.compile(rule["location"])
        if rule["role"] not in ("uuid", "name", "identity", "preserve", "unknown"):
            raise ValueError("Invalid field rule role")
    return cfg


def main(argv=None):
    os.umask(0o077)
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--jar", type=Path)
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("init")
    p.add_argument("--root", required=True)
    for command in ("queue", "scan", "dry-run"):
        p = sub.add_parser(command)
        p.add_argument("old")
        p.add_argument("new")
        p.add_argument("--premium", choices=["on", "off"])
        p.add_argument("--merge", action="store_true")
        if command != "queue":
            p.add_argument("--fresh", action="store_true")
    for command in ("status", "discover"):
        sub.add_parser(command)
    p = sub.add_parser("run")
    p.add_argument("--fresh", action="store_true")
    p = sub.add_parser("progress")
    p.add_argument("id", nargs="?")
    p = sub.add_parser("startup")
    p.add_argument("--server", required=True)
    p = sub.add_parser("launch")
    p.add_argument("--server", required=True)
    p.add_argument("--directory", type=Path, required=True)
    p.add_argument("process", nargs=argparse.REMAINDER)
    for command in ("rollback", "cancel", "retry"):
        p = sub.add_parser(command)
        p.add_argument("id")
        if command == "cancel":
            p.add_argument("--force", action="store_true")
    args = parser.parse_args(argv)
    if args.command == "dry-run":
        args.command = "scan"
    database.JAR = nbt.JAR = args.jar
    cfg = configure(args.config.resolve(), args)
    if cfg is None:
        return 0
    state(cfg).mkdir(parents=True, exist_ok=True)
    if args.command == "launch":
        from distributed import launch

        cfg["_configPath"] = str(args.config.resolve())
        return launch(cfg, args.server, args.directory, args.process)
    if args.command == "progress":
        found = False
        roots = [(None, state(cfg))]
        if cfg.get("deployment") == "distributed":
            roots = [(s, state(cfg) / "participants" / s) for s in cfg["servers"]]
        for server, root in roots:
            for path in sorted(root.glob("*/*/progress.json")):
                if not args.id or path.parent.name == args.id:
                    print(
                        dumps(
                            dict(
                                load(path),
                                id=path.parent.name,
                                area=path.parent.parent.name,
                                participant=server,
                            )
                        )
                    )
                    found = True
        if not found:
            print("No matching scan progress")
        return 0
    with Lock(state(cfg) / "coordinator.lock", blocking=args.command == "startup"):
        if args.command in ("queue", "scan"):
            ident = Identity(
                args.old,
                args.new,
                (
                    args.premium == "on"
                    if args.premium
                    else cfg.get("premiumDefault", False)
                ),
                cfg.get("fieldRules", []),
                args.merge,
            )
            items = jobs(cfg)
            reserved = {
                n.lower()
                for j in items
                if j["status"]
                in (
                    "queued",
                    "running",
                    "recovery_required",
                    "rollback_queued",
                    "failed",
                )
                for n in (j["old"], j["new"])
            }
            if (
                args.command == "queue" or cfg.get("deployment") == "distributed"
            ) and reserved & {
                ident.old.lower(),
                ident.new.lower(),
            }:
                raise ValueError("Name overlaps another pending job")
            job = {
                "id": stamp(),
                "old": ident.old,
                "new": ident.new,
                "premium": ident.premium,
                "merge": ident.merge,
                "status": "queued",
                "created": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            }
            if args.command == "queue":
                items.append(job)
                setjobs(cfg, items)
                print(
                    "Queued",
                    job["id"],
                    ident.old,
                    "->",
                    ident.new,
                    "offline UUID",
                    ident.new_uuid,
                    "premium=" + str(ident.premium),
                )
            else:
                if cfg.get("deployment") == "distributed":
                    job["dryRun"] = True
                    job["fresh"] = args.fresh
                    items.append(job)
                    setjobs(cfg, items)
                    print(
                        "Queued distributed dry-run",
                        job["id"],
                        "Restart all participants to scan",
                    )
                    return 0
                held = quiescent(cfg, configs(cfg))
                try:
                    report, _ = discover_plan(
                        cfg, job, state(cfg) / "scans" / job["id"], fresh=args.fresh
                    )
                finally:
                    for f in held:
                        f.close()
                return 0 if report["ready"] else 1
        elif args.command == "status":
            for j in jobs(cfg):
                print(
                    j["id"], j["old"], "->", j["new"], j["status"], j.get("error", "")
                )
        elif args.command == "discover":
            if cfg.get("deployment") == "distributed":
                raise ValueError(
                    "Use dry-run <old> <new> and start all participants for distributed discovery"
                )
            manifest = configs(cfg)
            for i, e in enumerate(cfg.get("databaseEndpoints", [])):
                for discovered in manifest["endpoints"]:
                    if (
                        discovered.get("host") == e.get("host")
                        and discovered.get("database") == e.get("database")
                        and int(discovered.get("port", 3306))
                        == int(e.get("port", 3306))
                    ):
                        discovered["_configuredIndex"] = i
            related_databases(cfg, manifest)
            inventory = []
            placeholder = Identity("HammerFrom", "HammerTo")
            for endpoint in manifest["endpoints"] + [
                {"kind": f["format"], "path": f["path"]}
                for f in manifest["files"]
                if f["format"] in ("sqlite", "h2", "hsqldb")
            ]:
                db = None
                try:
                    db = Database(cfg, endpoint_runtime(cfg, endpoint), readonly=True)
                    tables = db.tables()
                    for table in tables:
                        for column in table["columns"]:
                            location = (
                                (endpoint.get("database") or endpoint.get("path"))
                                + ":"
                                + table["name"]
                                + "."
                                + column["name"]
                            )
                            column["role"] = placeholder.role(
                                table["name"], column["name"], location
                            )
                    inventory.append(
                        {"endpoint": endpoint_public(endpoint), "tables": tables}
                    )
                except Exception as error:
                    inventory.append(
                        {
                            "endpoint": endpoint_public(endpoint),
                            "error": str(error)[:200],
                        }
                    )
                finally:
                    if db:
                        db.rollback()
                        db.close()
            manifest["databases"] = inventory
            manifest["endpoints"] = [endpoint_public(e) for e in manifest["endpoints"]]
            save(state(cfg) / "discovery.json", manifest)
            print(
                "Discovered",
                len(manifest["files"]),
                "files,",
                len(manifest["endpoints"]),
                "MySQL endpoints;",
                state(cfg) / "discovery.json",
            )
        elif args.command in ("cancel", "rollback", "retry"):
            items = jobs(cfg)
            job = next((j for j in items if j["id"] == args.id), None)
            if not job:
                raise ValueError("Unknown queue id")
            if args.command == "retry":
                if job["status"] != "failed":
                    raise ValueError(
                        "Retry requires a failed and fully rolled-back job"
                    )
                reserved = {
                    n.lower()
                    for other in items
                    if other is not job
                    and other["status"]
                    in (
                        "queued",
                        "running",
                        "rollback_queued",
                        "recovery_required",
                        "failed",
                    )
                    for n in (other["old"], other["new"])
                }
                if reserved & {job["old"].lower(), job["new"].lower()}:
                    raise ValueError("Retry overlaps another pending job")
                job["status"] = "retried"
                job = dict(
                    job,
                    id=stamp(),
                    status="queued",
                    created=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                )
                job.pop("error", None)
                job.pop("preflightBlocked", None)
                items.append(job)
            elif args.command == "cancel":
                interrupted_dry_run = bool(
                    job.get("distributedRound") and job.get("dryRun")
                )
                if job.get("distributedRound"):
                    if not args.force or not job.get("dryRun"):
                        raise ValueError(
                            "Active distributed round requires stopped participants "
                            "and cancel --force for a dry-run"
                        )
                    from distributed import active

                    protocol = state(cfg) / "distributed" / "launchers"
                    if any(
                        active(protocol / (server + ".lock"))
                        for server in cfg["servers"]
                    ):
                        raise ValueError(
                            "Stop every participant launcher before force-cancelling a dry-run"
                        )
                    job.pop("distributedRound", None)
                if job["status"] not in ("queued", "failed") and not (
                    args.force and interrupted_dry_run and job["status"] == "running"
                ):
                    raise ValueError(
                        "Cancel requires an untouched or fully rolled-back job"
                    )
                job["status"] = "cancelled"
            else:
                if job["status"] not in ("done", "recovery_required"):
                    raise ValueError(
                        "Only completed or interrupted jobs can be rolled back"
                    )
                later = items[items.index(job) + 1 :]
                if any(
                    j["status"]
                    in (
                        "done",
                        "queued",
                        "running",
                        "recovery_required",
                        "rollback_queued",
                    )
                    and {job["old"].lower(), job["new"].lower()}
                    & {j["old"].lower(), j["new"].lower()}
                    for j in later
                ):
                    raise ValueError("Roll back later dependent renames first")
                job["status"] = "rollback_queued"
                job.pop("distributedRound", None)
            setjobs(cfg, items)
            print("Updated", job["id"], job["status"])
        elif args.command in ("run", "startup"):
            if args.command == "run" and cfg.get("deployment") == "distributed":
                raise ValueError(
                    "Start every server through start-hammermigrate.sh to execute the distributed queue"
                )
            if (
                args.command == "startup"
                and cfg.get("startup", "external") != "embedded"
            ):
                items = jobs(cfg)
                if blocking_failures(items) or any(
                    j["status"]
                    in ("queued", "running", "recovery_required", "rollback_queued")
                    for j in items
                ):
                    raise ValueError(
                        "Pending jobs require pre-start: java -jar HammerMigrate.jar --config <shared-config> run"
                    )
            else:
                process(
                    cfg,
                    getattr(args, "server", None),
                    fresh=getattr(args, "fresh", False),
                )
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print("HammerMigrate blocked:", str(exc), file=sys.stderr)
        sys.exit(1)
