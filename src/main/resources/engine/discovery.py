"""Discover live storage/config endpoints, retaining credential references only."""

import collections, json, os, re, socket, sqlite3, struct, zipfile
from pathlib import Path
import yaml
from yaml_storage import parse as parse_yaml
from common import safe_path, sha, ignored_path
from configuration import read as read_configuration

FORMATS = {
    ".yml": "yaml",
    ".yaml": "yaml",
    ".json": "json",
    ".dat": "nbt",
    ".nbt": "nbt",
    ".mca": "region",
    ".mcc": "external-chunk",
    ".db": "database",
    ".sqlite": "sqlite",
    ".sqlite3": "sqlite",
    ".properties": "properties",
    ".cache": "text-cache",
    ".csv": "csv",
    ".txt": "text",
    ".toml": "toml",
    ".conf": "config",
}
HISTORY = re.compile(
    r"(^|[-_.])(backup|backups|archive|archives|rollback|old)([-_.]|$)|\.dat_old$|\.bak($|[-.])",
    re.I,
)


def iter_files(cfg, skipped=None, issues=None):
    skipped = [] if skipped is None else skipped
    issues = [] if issues is None else issues
    root = Path(cfg["networkRoot"])
    excludes = set(cfg.get("exclude", [])) | {
        "HammerMigrate",
        "node_modules",
        "__pycache__",
        ".git",
        "bundler",
        "assets",
        "versions",
        "Language_Repo",
        "translations",
        "Translations",
        "FileBackups",
        "sellLogs",
        "moneyLog",
        "TradeLogs",
        "TransactionLogs",
    }
    for server in cfg["servers"]:
        base = safe_path(root, server)
        if not base.is_dir():
            raise ValueError("Missing configured server " + server)
        for directory, dirs, files in os.walk(base, followlinks=False):
            parent = Path(directory)
            for name in list(dirs):
                p = parent / name
                if (
                    name in excludes
                    or name == "cache"
                    and parent == base
                    or HISTORY.search(name)
                    or ignored_path(cfg, str(p.relative_to(root)))
                ):
                    skipped.append(
                        {
                            "path": str(p.relative_to(root)),
                            "reason": "excluded directory or historical snapshots",
                        }
                    )
                    dirs.remove(name)
                elif p.is_symlink():
                    issues.append(
                        {
                            "path": str(p.relative_to(root)),
                            "error": "Symlinked storage directory requires an explicit physical server root or ignorePaths",
                        }
                    )
                    dirs.remove(name)
            for name in sorted(files):
                p = parent / name
                rel = str(p.relative_to(root))
                if (
                    HISTORY.search(name)
                    or re.search(r"\.log(?:\.\d+)?$", name)
                    or ignored_path(cfg, rel)
                ):
                    skipped.append(
                        {"path": rel, "reason": "history, log, or explicit ignorePaths"}
                    )
                    continue
                if p.is_symlink():
                    if p.suffix.lower() not in (".jar", ".png", ".zip", ".class"):
                        issues.append(
                            {
                                "path": rel,
                                "error": "Symlinked storage file requires an explicit physical server root or ignorePaths",
                            }
                        )
                    else:
                        skipped.append(
                            {"path": rel, "reason": "symlinked executable/asset"}
                        )
                    continue
                # Historical snapshots remain owned by their renamed parent index.
                if any(
                    x in p.parts for x in ("InventoryRollbackPlus", "CoreProtect")
                ) and any(x in p.parts for x in ("backups", "backup")):
                    continue
                yield p


def at(obj, keys):
    for key in keys:
        obj = obj[key]
    return obj


class ScopeChanged(ValueError):
    """Read-only discovery changed and requires a new distributed assignment."""


def installed_plugins(root, server):
    """Read descriptors only; dormant plugin folders remain physical scan inputs."""
    plugins = {}
    for jar in sorted((root / server / "plugins").glob("*.jar")):
        try:
            with zipfile.ZipFile(jar) as archive:
                for descriptor in ("plugin.yml", "paper-plugin.yml", "bungee.yml"):
                    if descriptor not in archive.namelist():
                        continue
                    metadata = yaml.safe_load(archive.read(descriptor))
                    if isinstance(metadata, dict) and metadata.get("name"):
                        plugins[str(metadata["name"]).casefold()] = jar.name
                        break
        except (OSError, ValueError, zipfile.BadZipFile, yaml.YAMLError):
            continue
    return plugins


def configs(cfg):
    root = Path(cfg["networkRoot"])
    endpoints = {}
    plugins = {server: installed_plugins(root, server) for server in cfg["servers"]}
    ports = []
    files = []
    issues = []
    login = []
    skipped = []

    def enabled_config(obj, keys):
        if path_plugin_name[0].lower() == "fastlogin":
            login = obj.get("login")
            if isinstance(login, dict) and login.get("enabled") is False:
                return False
        chain = []
        current = obj
        for key in [None] + keys:
            if key is not None:
                current = current[key]
            if isinstance(current, dict):
                chain.append(current)
        for section in chain:
            normalized = {
                re.sub(r"[_-]\d+$", "", str(k))
                .lower()
                .replace("_", "")
                .replace("-", ""): v
                for k, v in section.items()
            }
            if any(
                normalized.get(flag) is False
                or str(normalized.get(flag)).lower() == "false"
                for flag in ("enabled", "use", "usemysql", "mysql")
            ):
                return False
            for selector in (
                "type",
                "engine",
                "backend",
                "storagemethod",
                "driver",
                "rdmsvendor",
                "method",
            ):
                value = normalized.get(selector)
                if isinstance(value, str) and value.lower() in (
                    "sqlite",
                    "h2",
                    "hsql",
                    "hsqldb",
                    "hypersql",
                    "file",
                    "flatfile",
                    "yaml",
                    "json",
                    "postgres",
                    "postgresql",
                ):
                    return False
        if path_plugin_name[0] == "XConomy":
            return str((obj.get("Settings") or {}).get("storage-type", "")).lower() in (
                "mysql",
                "mariadb",
            )
        return True

    path_plugin_name = [""]

    def add(c, path, keys, document):
        values = {
            re.sub(r"[_-]\d+$", "", str(k))
            .lower()
            .replace("_", "")
            .replace("-", ""): (k, v)
            for k, v in c.items()
        }

        def get(*aliases):
            for a in aliases:
                if a in values and isinstance(values[a][1], (str, int)):
                    return values[a]
            return None

        host = get("mysqlhost", "hostname", "host", "address", "ipaddress")
        db = get("mysqldatabase", "database", "databasename", "db")
        user = get("mysqlusername", "username", "user")
        password = get("mysqlpassword", "password", "pass", "userpassword")
        port = get("mysqlport", "port")
        if not all((host, db, user, password)):
            return
        server = path.relative_to(root).parts[0]
        parts = path.relative_to(root).parts
        plugin = parts[2] if len(parts) > 2 and parts[1] == "plugins" else None
        if plugins[server] and plugin and plugin.casefold() not in plugins[server]:
            skipped.append(
                {
                    "path": str(path.relative_to(root)),
                    "reason": "inactive plugin database configuration; retained files are still scanned",
                    "plugin": plugin,
                }
            )
            return
        if not enabled_config(document, keys):
            skipped.append(
                {
                    "path": str(path.relative_to(root)),
                    "reason": "disabled relational endpoint",
                    "keys": keys,
                }
            )
            return
        h = str(host[1])
        pn = int(port[1]) if port else 3306
        if ":" in h and not h.startswith(("jdbc:", "[")):
            h, ps = h.rsplit(":", 1)
            if not ps.isdigit():
                return
            pn = int(ps)
        if h.startswith("jdbc:") or not str(db[1]).strip():
            return
        key = (h, pn, str(db[1]))
        ignored = next(
            (
                item
                for item in cfg.get("ignoredDatabaseEndpoints", [])
                if item.get("host") == h
                and int(item.get("port", 3306)) == pn
                and item.get("database") == str(db[1])
            ),
            None,
        )
        if ignored:
            skipped.append(
                {
                    "path": str(path.relative_to(root)),
                    "reason": ignored.get(
                        "reason", "explicitly ignored database endpoint"
                    ),
                    "endpoint": {"host": h, "port": pn, "database": str(db[1])},
                }
            )
            return
        ref = {
            "kind": "mysql",
            "plugin": plugin,
            "tablePrefix": c.get("table-prefix", c.get("tablePrefix", "")),
            "host": h,
            "port": pn,
            "database": str(db[1]),
            "config": str(path.relative_to(root)),
            "keys": keys,
            "fields": {
                k: x[0]
                for k, x in [
                    ("host", host),
                    ("database", db),
                    ("user", user),
                    ("password", password),
                ]
            },
        }
        if key not in endpoints:
            endpoints[key] = ref

    for path in iter_files(cfg, skipped, issues):
        rel = str(path.relative_to(root))
        kind = FORMATS.get(path.suffix.lower(), "opaque")
        if kind == "nbt" and path.suffix.lower() == ".dat":
            with path.open("rb") as source:
                header = source.read(3)
            if (
                not header.startswith(
                    (b"\x0a", b"\x1f\x8b", b"x\x01", b"x\x9c", b"x\xda")
                )
                and "playerdata" not in path.parts
            ):
                kind = "opaque"
        if path.name.endswith(".mv.db"):
            kind = "h2"
        if path.name.endswith(".properties"):
            text = path.read_text(errors="replace")
            if ("hsqldb" in text.lower() or "hsql database" in text.lower()) and Path(
                str(path)[: -len(".properties")] + ".script"
            ).exists():
                kind = "hsqldb"
        if path.suffix.lower() in (".db", ".sqlite", ".sqlite3"):
            with path.open("rb") as f:
                if f.read(16) == b"SQLite format 3\x00":
                    kind = "sqlite"
        if path.name.endswith(
            (
                "-wal",
                "-shm",
                "-journal",
                ".lck",
                ".lock",
                ".log",
                ".data",
                ".script",
                ".backup",
            )
        ):
            skipped.append(
                {
                    "path": rel,
                    "reason": "database sidecar or lock; handled by database adapter",
                }
            )
            continue
        if re.search(
            r"\.jar(?:\.disabled|\.off|\d+)$", path.name, re.I
        ) or path.suffix.lower() in (
            ".jar",
            ".class",
            ".png",
            ".zip",
            ".gz",
            ".map",
            ".bd",
            ".bin",
            ".ogg",
            ".schem",
        ):
            skipped.append(
                {"path": rel, "reason": "executable, asset, or historical schematic"}
            )
            continue
        files.append(
            {
                "path": rel,
                "format": kind,
                "scan": (
                    "sector-nbt"
                    if kind == "region"
                    else (
                        "relational-columns"
                        if kind in ("sqlite", "h2", "hsqldb")
                        else "structured-fields"
                    )
                ),
                "methods": ["update-field", "rename-path", "inverse-patch"],
            }
        )
        is_config = path.suffix.lower() in (".yml", ".yaml") or (
            path.suffix.lower() in (".json", ".toml", ".properties")
            and path.stem.lower()
            in ("config", "configuration", "storage", "database", "settings")
            and "plugins" in path.relative_to(root).parts
        )
        if is_config:
            try:
                obj = read_configuration(path)
            except (ValueError, yaml.YAMLError, UnicodeError) as e:
                issues.append(
                    {"path": rel, "error": "Unreadable storage config: " + str(e)[:120]}
                )
                continue

            def visit(v, keys):
                if isinstance(v, dict):
                    add(v, path, keys, obj)
                    for k, item in v.items():
                        visit(item, keys + [k])
                elif isinstance(v, list):
                    for i, item in enumerate(v):
                        visit(item, keys + [i])

            config_parts = path.relative_to(root).parts
            path_plugin_name[0] = (
                config_parts[2]
                if len(config_parts) > 2 and config_parts[1] == "plugins"
                else path.parent.name
            )
            if not (path.parent.name == "LibertyBans" and path.name == "import.yml"):
                visit(obj, [])
            if path.name == "config.yml" and path.parent.name == "AuthMeBungee":
                login.append(
                    {
                        "type": "authme-bungee",
                        "path": rel,
                        "installed": not plugins[rel.split("/")[0]]
                        or "authmebungee" in plugins[rel.split("/")[0]],
                        "keepOfflineUuidCompatibility": (obj.get("premium") or {}).get(
                            "keepOfflineUuidCompatibility", False
                        ),
                    }
                )
            if path.name == "config.yml" and path.parent.name == "AuthMe":
                server = rel.split("/")[0]
                base = root / server
                props = base / "server.properties"
                properties = props.read_text() if props.exists() else ""
                spigot_path = base / "spigot.yml"
                spigot = (
                    parse_yaml(spigot_path.read_text()) if spigot_path.exists() else {}
                )
                paper_path = base / "config/paper-global.yml"
                paper = (
                    parse_yaml(paper_path.read_text()) if paper_path.exists() else {}
                )
                forwarding = bool(
                    (spigot.get("settings") or {}).get("bungeecord", False)
                ) or bool(
                    (
                        ((paper.get("proxies") or {}).get("velocity") or {}).get(
                            "enabled", False
                        )
                    )
                )
                login.append(
                    {
                        "type": "authme",
                        "path": rel,
                        "installed": not plugins[server] or "authme" in plugins[server],
                        "onlineMode": bool(
                            re.search(r"(?m)^online-mode=true\s*$", properties)
                        ),
                        "proxyForwarding": forwarding,
                        "proxyHook": bool(
                            (obj.get("Hooks") or {}).get("bungeecord", False)
                        ),
                        "packetEvents": not plugins[server]
                        or "packetevents" in plugins[server],
                        "enabled": (obj.get("settings") or {}).get(
                            "enablePremium", False
                        ),
                    }
                )
            if (
                path.parent == root / rel.split("/")[0]
                and path.name == "config.yml"
                and isinstance(obj, dict)
            ):
                for listener in obj.get("listeners", []):
                    host = str(listener.get("host", "0.0.0.0:25577"))
                    ports.append(
                        {
                            "server": rel.split("/")[0],
                            "port": int(host.rsplit(":", 1)[1]),
                        }
                    )
        if path.name == "server.properties":
            text = path.read_text()
            m = re.search(r"(?m)^server-port=(\d+)", text)
            ports.append(
                {"server": rel.split("/")[0], "port": int(m.group(1)) if m else 25565}
            )
    for e in cfg.get("databaseEndpoints", []):
        ref = dict(e)
        if ref["kind"] == "mysql":
            endpoints[(ref["host"], int(ref.get("port", 3306)), ref["database"])] = ref
    return {
        "version": 1,
        "networkRoot": str(root),
        "servers": cfg["servers"],
        "files": files,
        "endpoints": list(endpoints.values()),
        "listeners": ports,
        "discoveryErrors": issues,
        "login": login,
        "skipped": skipped,
        "installedPlugins": plugins,
    }


def credentials(cfg, endpoint):
    if "config" in endpoint:
        p = safe_path(cfg["networkRoot"], endpoint["config"])
        obj = read_configuration(p)
        v = at(obj, endpoint["keys"])
        f = endpoint["fields"]
        user = str(v[f["user"]])
        password = str(v[f["password"]])
    else:
        user = str(endpoint["user"])
        password = str(endpoint.get("password", ""))

    def expand(s):
        if s.startswith("env:"):
            return os.environ[s[4:]]
        return s

    return expand(user), expand(password)


def quiescent(cfg, manifest, current=None, world_locks=True):
    root = Path(cfg["networkRoot"])
    held = []
    for item in manifest["listeners"]:
        if current and (root / item["server"]).resolve() == Path(current).resolve():
            continue
        for host in set(
            ["127.0.0.1", item["server"]]
            + cfg.get("serverHosts", {}).get(item["server"], [])
        ):
            try:
                with socket.create_connection((host, item["port"]), timeout=0.35):
                    raise RuntimeError(
                        "Server still listening: "
                        + item["server"]
                        + ":"
                        + str(item["port"])
                    )
            except (ConnectionRefusedError, socket.timeout, socket.gaierror, OSError):
                pass
    if not world_locks:
        return held
    # Hold actual world locks for the entire migration, beyond TCP checks.
    import fcntl

    try:
        for server in cfg["servers"]:
            for p in (root / server).rglob("session.lock"):
                if p.is_symlink():
                    raise ValueError("Symlinked world session lock")
                f = p.open("r+b")
                held.append(f)
                fcntl.lockf(f, fcntl.LOCK_EX | fcntl.LOCK_NB, 0)
    except Exception:
        for f in held:
            f.close()
        raise
    return held


def related_databases(cfg, manifest):
    """Find still-used historical schemas on discovered endpoints within network prefixes."""
    if not cfg.get("discoverRelatedSchemas", True):
        return
    from database import Database

    prefixes = ["global_", "cryo_"] + [
        Path(server).name + "_" for server in cfg["servers"]
    ]
    patterns = cfg.get(
        "schemaPatterns",
        [r"^(?:" + "|".join(re.escape(v) for v in prefixes) + ")", r"^authme$"],
    )
    visited = set()
    endpoints = manifest["endpoints"]
    known = {
        (e.get("host"), int(e.get("port", 3306)), e["database"]) for e in endpoints
    }
    for endpoint in list(endpoints):
        # A new credential reference may have access to additional relevant schemas.
        credential_ref = (
            endpoint.get("config"),
            tuple(endpoint.get("keys", [])),
            endpoint.get("_configuredIndex"),
        )
        key = (endpoint.get("host"), int(endpoint.get("port", 3306)), credential_ref)
        if key in visited:
            continue
        visited.add(key)
        db = None
        try:
            db = Database(cfg, endpoint, readonly=True)
            schemas = db.execute(
                "SELECT SCHEMA_NAME AS name FROM information_schema.SCHEMATA"
            )["rows"]
            for row in schemas:
                name = row["name"]
                if name in (
                    "information_schema",
                    "performance_schema",
                    "mysql",
                    "sys",
                ) or not any(re.search(pattern, name) for pattern in patterns):
                    continue
                identity = (endpoint.get("host"), int(endpoint.get("port", 3306)), name)
                if identity in known:
                    continue
                known.add(identity)
                endpoints.append(
                    dict(endpoint, database=name, discoveredBy="related-schema-prefix")
                )
        except Exception as error:
            manifest.setdefault("databaseDiscoveryErrors", []).append(
                {
                    "host": endpoint.get("host"),
                    "database": endpoint["database"],
                    "error": str(error)[:200],
                }
            )
        finally:
            if db:
                db.rollback()
                db.close()
