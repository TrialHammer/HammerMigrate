"""Durable content-checked file checkpoints and observable scan progress."""

import datetime
from collections import deque
from concurrent.futures import ThreadPoolExecutor
import gzip
import json
from pathlib import Path
import sqlite3
import threading
import time
import zlib

from common import decode, dumps, file_sha, patch, safe_path, save, sha
from files import sparse_patch, transform
from identity import Identity
import nbt

DIAGNOSTICS = ("changes", "collisions", "unknown", "preserved")


def file_inventory(cfg, manifest):
    result = {}
    for item in manifest["files"]:
        if item["format"] in ("sqlite", "h2", "hsqldb"):
            continue
        stat = safe_path(cfg["networkRoot"], item["path"]).stat()
        result[item["path"]] = [
            item["format"],
            stat.st_dev,
            stat.st_ino,
            stat.st_size,
            stat.st_mtime_ns,
            stat.st_ctime_ns,
        ]
    return result


class ScanSession:
    def __init__(self, cfg, identity, folder, fresh=False):
        self.cfg = cfg
        self.identity = identity
        self.folder = Path(folder)
        self.fresh = fresh
        engine = Path(__file__).resolve().parent
        rules = [
            (str(p.relative_to(engine)), file_sha(p))
            for p in sorted(engine.rglob("*.py"))
        ]
        self.namespace = sha(
            dumps(
                {
                    # Inventory, participants and database ownership are live scope,
                    # not NBT parsing rules. Changing them must not discard valid
                    # checkpoints for the physical files that remain in scope.
                    "networkRoot": str(Path(cfg["networkRoot"]).resolve()),
                    "fieldRules": identity.rules,
                    "old": identity.old,
                    "new": identity.new,
                    "premium": identity.premium,
                    "merge": identity.merge,
                    "premiumUuid": identity.premium_uuid,
                    "engine": rules,
                    "jar": file_sha(nbt.JAR) if nbt.JAR else None,
                }
            ).encode()
        )
        self.progress = {
            "version": 1,
            "state": "scanning",
            "phase": "discovery",
            "startedAt": self.now(),
            "completedFiles": 0,
            "cacheHits": 0,
            "parsedFiles": 0,
            "failedFiles": 0,
            "completedDatabases": 0,
            "failedDatabases": 0,
        }
        self.last_update = 0
        self.connection = None
        self.lock = threading.RLock()

    @staticmethod
    def now():
        return datetime.datetime.now(datetime.timezone.utc).isoformat()

    def __enter__(self):
        save(
            self.folder / "manifest.json",
            {
                "version": 2,
                "state": "scanning",
                "ready": False,
                "sourceUuid": self.identity.old_uuid,
                "targetUuid": self.identity.new_uuid,
                "coverage": {"complete": False},
                "progress": "progress.json",
            },
        )
        self.update(force=True)
        path = (
            Path(
                self.cfg.get(
                    "stateDirectory",
                    str(Path(self.cfg["networkRoot"]) / "migrate/HammerMigrate"),
                )
            )
            / "scan-cache.sqlite"
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(path, check_same_thread=False)
        self.connection.execute(
            "CREATE TABLE IF NOT EXISTS files(namespace TEXT, path TEXT, digest TEXT, payload BLOB, PRIMARY KEY(namespace,path))"
        )
        self.connection.commit()
        return self

    def __exit__(self, kind, error, traceback):
        if kind:
            self.update(state="interrupted", error=str(error)[:400], force=True)
        if self.connection:
            self.connection.close()

    def update(self, force=False, **values):
        with self.lock:
            self.progress.update(values)
            now = time.monotonic()
            if force or now - self.last_update >= 5:
                self.progress["updatedAt"] = self.now()
                save(self.folder / "progress.json", self.progress)
                print(
                    "Scan progress",
                    self.progress["phase"],
                    str(self.progress["completedFiles"])
                    + "/"
                    + str(self.progress.get("totalFiles", "?")),
                    "cached=" + str(self.progress["cacheHits"]),
                    self.progress.get("current", ""),
                    flush=True,
                )
                self.last_update = now

    def scan_files(self, items):
        """Scan a bounded window of files and yield results in discovery order."""
        workers = self.cfg.get("scanThreads", 1)
        if isinstance(workers, bool) or not isinstance(workers, int) or not 1 <= workers <= 32:
            raise ValueError("scanThreads must be an integer between 1 and 32")

        def scan(item):
            try:
                if item["format"] == "database":
                    raise ValueError("Unrecognized database format; supply adapter or exact ignorePaths entry")
                return self.scan_file(item), None
            except Exception as exc:
                return None, exc

        if workers == 1:
            for item in items:
                result, error = scan(item)
                yield item, result, error
            return
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="hammer-scan") as pool:
            pending = deque()
            for item in items:
                pending.append((item, pool.submit(scan, item)))
                if len(pending) >= workers * 2:
                    first, future = pending.popleft()
                    result, error = future.result()
                    yield first, result, error
            while pending:
                first, future = pending.popleft()
                result, error = future.result()
                yield first, result, error

    def fork(self):
        ident = Identity(
            self.identity.old,
            self.identity.new,
            self.identity.premium,
            self.identity.rules,
            self.identity.merge,
        )
        ident.premium_uuid = self.identity.premium_uuid
        ident.scan_progress = lambda: self.update()
        return ident

    def scan_file(self, item):
        rel, kind = item["path"], item["format"]
        self.update(phase="physical", current=rel)
        path = safe_path(self.cfg["networkRoot"], rel)
        if path.stat().st_size > int(self.cfg.get("maxFileMiB", 256)) * 1024**2:
            raise ValueError("File exceeds configured memory bound")
        raw = path.read_bytes()
        digest = sha(raw)
        cacheable = kind in ("nbt", "region", "external-chunk")
        cached = None
        if cacheable and not self.fresh:
            with self.lock:
                entry = self.connection.execute(
                    "SELECT payload FROM files WHERE namespace=? AND path=? AND digest=?",
                    (self.namespace, rel, digest),
                ).fetchone()
            if entry:
                try:
                    candidate = json.loads(
                        gzip.decompress(entry[0]), object_hook=decode
                    )
                    if all(
                        file_sha(safe_path(self.cfg["networkRoot"], p)) == value
                        for p, value in candidate["dependencies"].items()
                    ):
                        cached = candidate
                except (ValueError, OSError, EOFError, KeyError, zlib.error):
                    pass
        result = cached if cached is not None else self.analyze(item, path, raw)
        if cacheable and cached is None:
            payload = gzip.compress(dumps(result).encode(), mtime=0)
            with self.lock:
                self.connection.execute(
                    "INSERT OR REPLACE INTO files VALUES (?,?,?,?)",
                    (self.namespace, rel, digest, payload),
                )
                self.connection.commit()
        with self.lock:
            for key in DIAGNOSTICS:
                getattr(self.identity, key).extend(result["diagnostics"][key])
            self.identity.external_chunks.update(
                str(safe_path(self.cfg["networkRoot"], p)) for p in result["dependencies"]
            )
            self.progress["completedFiles"] += 1
            self.progress["cacheHits" if cached is not None else "parsedFiles"] += 1
            self.update()
        return result

    def analyze(self, item, path, raw):
        ident = self.fork()
        changed, externals = transform(path, item["format"], raw, ident)
        dependencies = {
            str(Path(p).relative_to(self.cfg["networkRoot"])): digest
            for p, digest in ident.external_fingerprints.items()
        }
        actions = []
        for p, before, after in [(path, raw, changed)] + externals:
            if before != after:
                change = (
                    sparse_patch(before, after)
                    if item["format"] == "region" and p == path
                    else patch(before, after)
                )
                actions.append(
                    {
                        "kind": "file",
                        "path": str(p.relative_to(self.cfg["networkRoot"])),
                        "patch": change,
                    }
                )
        # Parse the planned output again, including changed external chunks.
        # Source references in explicitly preserved fields are recorded separately.
        probe = self.fork()
        if item["format"] == "region":
            nbt.region(
                changed, path, probe, {str(p): after for p, _, after in externals}
            )
        else:
            transform(path, item["format"], changed, probe)
        residual = probe.changes + [
            hit for hit in probe.unknown if hit not in ident.unknown
        ]
        if residual:
            raise ValueError(
                "Planned output retains source identity requiring migration: "
                + dumps(residual[:5])
            )
        return {
            "physical": dict(
                item,
                changed=raw != changed,
                fields=ident.changes,
                sha256=sha(raw),
                residualChecked=True,
            ),
            "actions": actions,
            "dependencies": dependencies,
            "diagnostics": {key: getattr(ident, key) for key in DIAGNOSTICS},
        }
