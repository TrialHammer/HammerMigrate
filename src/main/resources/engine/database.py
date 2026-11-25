"""Per-row relational plans, disposable validation and transaction/CAS recovery."""

import collections, json, os, re, sqlite3, subprocess, sys, uuid, tempfile, shutil
from pathlib import Path
import pymysql
from common import dumps, decode, sha, safe_path
from discovery import credentials
from relational import merge_operations

JAR = None


def quote(x, kind):
    return (
        ("`" + x.replace("`", "``") + "`")
        if kind == "mysql"
        else '"' + x.replace('"', '""') + '"'
    )


def normalize(row):
    return dumps(row)


class Database:
    def __init__(self, cfg, endpoint, readonly=False):
        self.cfg = cfg
        self.endpoint = endpoint
        self.kind = endpoint["kind"]
        self.c = None
        self.rpc = None
        self.readonly = readonly
        self.rowid_tables = set()
        if self.kind == "mysql":
            user, password = credentials(cfg, endpoint)
            self.c = pymysql.connect(
                host=endpoint.get("host", "localhost"),
                unix_socket=endpoint.get("unixSocket"),
                port=int(endpoint.get("port", 3306)),
                user=user,
                password=password,
                database=endpoint["database"],
                charset="utf8mb4",
                autocommit=False,
                connect_timeout=8,
                read_timeout=180,
                cursorclass=pymysql.cursors.DictCursor,
                ssl=endpoint.get("ssl"),
            )
            self.execute("SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE")
            if readonly:
                self.execute("SET SESSION TRANSACTION READ ONLY")
                self.execute("START TRANSACTION WITH CONSISTENT SNAPSHOT")
        elif self.kind == "sqlite":
            path = safe_path(cfg["networkRoot"], endpoint["path"])
            self.c = sqlite3.connect(
                path.as_uri() + ("?mode=ro" if readonly else "?mode=rw"),
                uri=True,
                timeout=5,
            )
            self.c.row_factory = sqlite3.Row
            self.execute("PRAGMA foreign_keys=ON")
            if self.execute("PRAGMA quick_check")["rows"] != [{"quick_check": "ok"}]:
                raise ValueError("SQLite integrity check failed " + str(path))
        else:
            path = safe_path(cfg["networkRoot"], endpoint["path"])
            base = str(path)[:-6] if self.kind == "h2" else str(path)[:-11]
            url = (
                "jdbc:h2:file:"
                + base
                + ";IFEXISTS=TRUE;DB_CLOSE_ON_EXIT=FALSE"
                + (";ACCESS_MODE_DATA=r" if readonly else "")
                if self.kind == "h2"
                else "jdbc:hsqldb:file:"
                + base
                + ";ifexists=true;shutdown=true"
                + (";readonly=true" if readonly else "")
            )
            self.rpc = subprocess.Popen(
                ["java", "-cp", str(JAR), "ink.ziip.hammer.migrate.JdbcBridge"],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            default_user = (
                "" if self.kind == "h2" and "ChessCraft" in path.parts else "SA"
            )
            user, password = credentials(
                cfg, dict(endpoint, user=endpoint.get("user", default_user))
            )
            self.jdbc_user, self.jdbc_password = user, password
            self.rpc.stdin.write(
                dumps({"url": url, "user": user, "password": password}) + "\n"
            )
            self.rpc.stdin.flush()
            response = self.rpc.stdout.readline()
            if not response:
                error = self.rpc.stderr.read()
                self.rpc.wait(timeout=20)
                self.rpc.stdin.close()
                self.rpc.stdout.close()
                self.rpc.stderr.close()
                raise ValueError(
                    "JDBC connection failed: " + error.splitlines()[0][:400]
                    if error
                    else "JDBC connection terminated"
                )
            if not json.loads(response).get("ok"):
                raise ValueError("JDBC connection refused")

    def call(self, request):
        self.rpc.stdin.write(dumps(request) + "\n")
        self.rpc.stdin.flush()
        response = self.rpc.stdout.readline()
        if not response:
            raise ValueError("JDBC bridge terminated")
        out = json.loads(response, object_hook=decode)
        if not out.get("ok"):
            raise ValueError(out.get("error", "JDBC error"))
        return out

    def execute(self, sql, params=()):
        if self.rpc:
            return self.call({"action": "sql", "sql": sql, "params": list(params)})
        if self.kind == "mysql":
            with self.c.cursor() as q:
                q.execute(sql, params)
                return (
                    {"rows": list(q.fetchall()), "count": q.rowcount}
                    if q.description
                    else {"count": q.rowcount}
                )
        cursor = self.c.execute(sql, params)
        return (
            {"rows": [dict(r) for r in cursor.fetchall()], "count": cursor.rowcount}
            if cursor.description
            else {"count": cursor.rowcount}
        )

    def iter_query(self, sql, params=()):
        if self.kind == "mysql":
            with self.c.cursor(pymysql.cursors.SSDictCursor) as cursor:
                cursor.execute(sql, params)
                for row in cursor:
                    yield row
        elif self.kind == "sqlite":
            cursor = self.c.execute(sql, params)
            try:
                for row in cursor:
                    yield dict(row)
            finally:
                cursor.close()
        else:
            yield from self.execute(sql, params)["rows"]

    def commit(self):
        self.call({"action": "commit"}) if self.rpc else self.c.commit()

    def rollback(self):
        self.call({"action": "rollback"}) if self.rpc else self.c.rollback()

    def close(self):
        if self.rpc:
            try:
                self.call({"action": "close"})
            finally:
                self.rpc.stdin.close()
                self.rpc.wait(timeout=20)
                self.rpc.stdout.close()
                self.rpc.stderr.close()
        elif self.c:
            self.c.close()

    def tables(self):
        if self.rpc:
            tables = self.call({"action": "metadata"})["tables"]
            for table in tables:
                if not table["pk"]:
                    indexes = self.unique_indexes(table)
                    if indexes:
                        table["pk"] = min(indexes, key=len)
                        table["locator"] = "unique-key"
            return tables
        tables = []
        if self.kind == "mysql":
            for item in self.execute(
                "SELECT TABLE_NAME,ENGINE,TABLE_TYPE FROM information_schema.TABLES WHERE TABLE_SCHEMA=%s",
                (self.endpoint["database"],),
            )["rows"]:
                if item["TABLE_TYPE"] != "BASE TABLE":
                    continue
                name = item["TABLE_NAME"]
                cols = self.execute(
                    "SELECT COLUMN_NAME name,DATA_TYPE type,EXTRA extra FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s ORDER BY ORDINAL_POSITION",
                    (self.endpoint["database"], name),
                )["rows"]
                pk = self.execute(
                    "SELECT COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s AND CONSTRAINT_NAME='PRIMARY' ORDER BY ORDINAL_POSITION",
                    (self.endpoint["database"], name),
                )["rows"]
                tables.append(
                    {
                        "name": name,
                        "columns": cols,
                        "pk": [r["COLUMN_NAME"] for r in pk],
                        "engine": item["ENGINE"],
                    }
                )
        else:
            for item in self.execute(
                "SELECT name,sql FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            )["rows"]:
                cols = self.execute(
                    "PRAGMA table_xinfo(" + quote(item["name"], self.kind) + ")"
                )["rows"]
                keys = [
                    c["name"] for c in sorted(cols, key=lambda c: c["pk"]) if c["pk"]
                ]
                if (
                    not keys
                    and not any(
                        c["name"].lower() in ("rowid", "_rowid_", "oid", "__hm_rowid")
                        for c in cols
                    )
                    and "WITHOUT ROWID" not in (item["sql"] or "").upper()
                ):
                    self.rowid_tables.add(item["name"])
                    keys = ["__hm_rowid"]
                tables.append(
                    {
                        "name": item["name"],
                        "columns": [
                            {
                                "name": c["name"],
                                "type": c["type"],
                                "extra": "generated" if c["hidden"] else "",
                            }
                            for c in cols
                        ],
                        "pk": keys,
                        "ddl": item["sql"],
                    }
                )
        if self.kind == "mysql":
            for table in tables:
                if not table["pk"]:
                    indexes = self.unique_indexes(table)
                    if indexes:
                        table["pk"] = min(indexes, key=len)
                        table["locator"] = "unique-key"
                    elif self.semantic_table(
                        table["name"]
                    ).lower() == "playerparticles_settings" and any(
                        c["name"] == "player_uuid" for c in table["columns"]
                    ):
                        # PlayerParticles ships this singleton table without a SQL
                        # key. row() proves uniqueness; SERIALIZABLE protects the
                        # candidate scan and the exact row before any mutation.
                        table["pk"] = ["player_uuid"]
                        table["locator"] = "verified-plugin-identity"
        return tables

    def q(self, s):
        return quote(
            "_rowid_" if s == "__hm_rowid" and self.kind == "sqlite" else s, self.kind
        )

    def selection(self, table):
        if self.kind == "sqlite":
            self.tables()
            if table in self.rowid_tables:
                return '_rowid_ AS "__hm_rowid", *'
        return "*"

    def mark(self):
        return "%s" if self.kind == "mysql" else "?"

    def fk(self):
        if self.kind == "sqlite":
            return self.execute("PRAGMA foreign_key_check")["rows"]
        if self.rpc:
            result = {}
            for key, fields in self.jdbc_foreign_key_groups().items():
                first = fields[0]
                equality = " AND ".join(
                    "c." + self.q(f["childColumn"]) + "=p." + self.q(f["parentColumn"])
                    for f in fields
                )
                nonnull = " AND ".join(
                    "c." + self.q(f["childColumn"]) + " IS NOT NULL" for f in fields
                )
                result[key] = self.execute(
                    "SELECT COUNT(*) AS n FROM "
                    + self.q(first["childSchema"])
                    + "."
                    + self.q(first["child"])
                    + " c WHERE "
                    + nonnull
                    + " AND NOT EXISTS (SELECT 1 FROM "
                    + self.q(first["parentSchema"])
                    + "."
                    + self.q(first["parent"])
                    + " p WHERE "
                    + equality
                    + ")"
                )["rows"][0]
            return result
        return []

    def jdbc_foreign_key_groups(self):
        groups = collections.defaultdict(list)
        for field in self.foreign_keys():
            groups[(field["childSchema"], field["child"], field["name"])].append(field)
        return groups

    def row(self, table, pk, row, lock=False):
        sql = (
            "SELECT "
            + self.selection(table)
            + " FROM "
            + self.q(table)
            + " WHERE "
            + " AND ".join(self.q(k) + "=" + self.mark() for k in pk)
        )
        if lock and self.kind == "mysql":
            sql += " FOR UPDATE"
        found = self.execute(sql, [row[k] for k in pk])["rows"]
        if len(found) > 1:
            raise ValueError("Nonunique row locator")
        return found[0] if found else None

    def rows_for(self, meta, identity):
        if self.kind not in ("mysql", "sqlite"):
            return self.iter_query(
                "SELECT "
                + self.selection(meta["name"])
                + " FROM "
                + self.q(meta["name"])
            )
        clauses = []
        params = []
        mark = self.mark()
        for col in meta["columns"]:
            typ = col["type"].lower()
            name = self.q(col["name"])
            if (
                any(
                    t in typ
                    for t in ("char", "text", "json", "blob", "binary", "clob", "uuid")
                )
                or not typ
            ):
                clauses += ["LOWER(" + name + ") LIKE LOWER(" + mark + ")"] * 4
                params += [
                    "%" + identity.old + "%",
                    "%" + identity.new + "%",
                    "%" + identity.old_uuid + "%",
                    "%" + identity.new_uuid + "%",
                ]
                clauses += ["LOWER(" + name + ") LIKE LOWER(" + mark + ")"] * 2
                params += [
                    "%" + identity.old_uuid.replace("-", "") + "%",
                    "%" + identity.new_uuid.replace("-", "") + "%",
                ]
                clauses.append("LENGTH(" + name + ")>40")
                if any(t in typ for t in ("blob", "binary")) or not typ:
                    clauses += [
                        "INSTR(" + name + "," + mark + ")>0",
                        "INSTR(" + name + "," + mark + ")>0",
                    ]
                    params += [identity.raw_old, identity.raw_new]
                    # Compressed values have no searchable source bytes; inspect all nonempty blobs.
                    clauses += [name + " IS NOT NULL"]
            if "most" in col["name"].lower() or "least" in col["name"].lower():
                halves = [
                    int.from_bytes(raw[i : i + 8], "big", signed=True)
                    for raw in (identity.raw_old, identity.raw_new)
                    for i in (0, 8)
                ]
                clauses += [name + " IN (" + ",".join([mark] * 4) + ")"]
                params += halves
        if not clauses:
            return []
        return self.iter_query(
            "SELECT "
            + self.selection(meta["name"])
            + " FROM "
            + self.q(meta["name"])
            + " WHERE "
            + " OR ".join(clauses),
            params,
        )

    def semantic_table(self, name):
        plugin = str(self.endpoint.get("plugin", "")).casefold()
        prefix = self.endpoint.get("tablePrefix")
        families = {
            "playerpoints": ("points", "username_cache", "migrations"),
            "playerparticles": ("settings", "group", "particle", "fixed", "migrations"),
        }
        if (
            plugin in families
            and prefix
            and name.casefold().startswith(str(prefix).casefold())
        ):
            suffix = name[len(prefix) :].casefold()
            if suffix in families[plugin]:
                return plugin + "_" + suffix
        return name

    def plan(self, identity, progress=None):
        ops = []
        inventory = []
        tables = self.tables()
        for meta in tables:
            table = meta["name"]
            semantic = self.semantic_table(table)
            location = (
                (self.endpoint.get("database") or self.endpoint.get("path"))
                + ":"
                + table
            )
            if progress:
                progress(current=location, force=True)
            total_rows = next(
                iter(
                    self.execute("SELECT COUNT(*) AS n FROM " + self.q(table))["rows"][
                        0
                    ].values()
                )
            )
            changes = []
            matches = self.rows_for(meta, identity)
            candidate_count = 0
            candidate_rows = []
            for row in matches:
                candidate_rows.append(row)
                candidate_count += 1
                if progress and candidate_count % 1000 == 0:
                    progress(current=location, candidateRows=candidate_count)
                after = dict(row)
                name_cols = [
                    k
                    for k in row
                    if k.lower()
                    in ("username", "realname", "name", "playername", "player_name")
                ]
                uuid_col = next((k for k in row if k.lower() == "uuid"), None)
                if (
                    uuid_col
                    and identity.role(semantic, uuid_col, location + "." + uuid_col)
                    == "uuid"
                    and any(
                        isinstance(row[k], str)
                        and row[k].lower() == identity.old.lower()
                        for k in name_cols
                    )
                ):
                    bound = identity.raw_uuid(row[uuid_col])
                    if bound and bound != identity.raw_old:
                        raise ValueError(
                            "Source name refers to another UUID at " + location
                        )
                for col in meta["columns"]:
                    name = col["name"]
                    value = row.get(name)
                    where = location + "." + name
                    if col.get("extra") and (
                        col["extra"].lower() == "generated"
                        or "virtual generated" in col["extra"].lower()
                        or "stored generated" in col["extra"].lower()
                    ):
                        continue
                    after[name] = identity.cell(value, semantic, name, where)
                # Pair two signed BIGINT UUID halves only in fields explicitly named UUID.
                lower = {k.lower(): k for k in row}
                for name in row:
                    if not name.lower().endswith(("most", "_most")):
                        continue
                    tail = "least" if not name.lower().endswith("_most") else "_least"
                    prefix = name[:-4] if tail == "least" else name[:-5]
                    other = lower.get((prefix + tail).lower())
                    if (
                        other
                        and isinstance(row[name], int)
                        and isinstance(row[other], int)
                    ):
                        raw = (row[name] & ((1 << 64) - 1)).to_bytes(8, "big") + (
                            row[other] & ((1 << 64) - 1)
                        ).to_bytes(8, "big")
                        new = identity.scalar(
                            raw,
                            "uuid" if "uuid" in name.lower() else "unknown",
                            location + "." + name,
                        )
                        if new != raw:
                            after[name] = int.from_bytes(new[:8], "big", signed=True)
                            after[other] = int.from_bytes(new[8:], "big", signed=True)
                # Premium enrollment is separate from the game identity; never replace it with an offline UUID.
                usercols = [k for k in row if k.lower() in ("username", "realname")]
                prem = next((k for k in row if k.lower() == "premiumuuid"), None)
                if prem and any(
                    isinstance(row[k], str) and row[k].lower() == identity.old.lower()
                    for k in usercols
                ):
                    bound = row[prem]
                    uuid_col = next((k for k in row if k.lower() == "uuid"), None)
                    real_col = next((k for k in row if k.lower() == "realname"), None)
                    if real_col and row[real_col] and row[real_col] != identity.old:
                        raise ValueError(
                            "Source AuthMe realname casing differs from requested offline identity"
                        )
                    if (
                        uuid_col
                        and row[uuid_col]
                        and identity.raw_uuid(row[uuid_col]) != identity.raw_old
                    ):
                        raise ValueError(
                            "Source AuthMe UUID differs from the requested offline identity"
                        )
                    identity.auth_accounts.append(
                        {"location": location, "sourcePremiumUuid": bound}
                    )
                    for login_col in row:
                        if login_col.lower() == "username":
                            after[login_col] = identity.new.lower()
                    for session_col in row:
                        if session_col.lower() in ("islogged", "hassession"):
                            after[session_col] = 0
                    if (
                        identity.premium
                        and bound
                        and str(bound).replace("-", "").lower()
                        != str(identity.premium_uuid).replace("-", "").lower()
                    ):
                        raise ValueError(
                            "New Mojang name belongs to another premium identity"
                        )
                    if identity.premium and not identity.premium_uuid:
                        raise ValueError("Missing verified target Mojang UUID")
                    after[prem] = identity.premium_uuid if identity.premium else None
                if after != row:
                    generated = [
                        c["name"]
                        for c in meta["columns"]
                        if c.get("extra", "").lower() == "generated"
                        or "virtual generated" in c.get("extra", "").lower()
                        or "stored generated" in c.get("extra", "").lower()
                    ]
                    if generated:
                        if table.lower() == "libertybans_names" and [
                            k.lower() for k in generated
                        ] == ["lower_name"]:
                            name_key = next(k for k in after if k.lower() == "name")
                            after[generated[0]] = after[name_key].lower()
                        else:
                            raise ValueError(
                                "Generated fields require an expression adapter on "
                                + location
                            )
                    if not meta["pk"]:
                        raise ValueError("No primary key on touched table " + location)
                    if self.kind == "mysql" and meta["engine"] != "InnoDB":
                        raise ValueError("Nontransactional touched table " + location)
                    if any(row[k] is None for k in meta["pk"]):
                        raise ValueError(
                            "Nullable unique locator on touched row " + location
                        )
                    operation = {
                        "table": table,
                        "pk": meta["pk"],
                        "before": row,
                        "after": after,
                        "generatedColumns": generated,
                    }
                    if (
                        self.kind == "h2"
                        and table.upper() == "CHESSCRAFT_PLAYERS"
                        and [k.upper() for k in meta["pk"]] == ["ID"]
                        and row[meta["pk"][0]] != after[meta["pk"][0]]
                    ):
                        operation["method"] = "insert-repoint-delete"
                    changes.append(operation)
            changes = merge_operations(
                self,
                identity,
                dict(meta, semanticTable=semantic),
                changes,
                candidate_rows,
                location,
            )
            inventory.append(
                {
                    "table": table,
                    "semanticTable": semantic,
                    "columns": [
                        dict(
                            column=c["name"],
                            type=c["type"],
                            role=identity.role(
                                semantic, c["name"], location + "." + c["name"]
                            ),
                        )
                        for c in meta["columns"]
                    ],
                    "primaryKey": meta["pk"],
                    "candidateRows": candidate_count,
                    "totalRows": total_rows,
                    "scanComplete": True,
                    "scanMethod": (
                        "all-rows"
                        if self.rpc
                        else "identity-candidates-and-encoded-values"
                    ),
                    "changedRows": len(changes),
                    "method": sorted(
                        {
                            op.get("method", "transactional-update/primary-key-move")
                            for op in changes
                        }
                    ),
                }
            )
            ops += changes
        if tables != self.tables():
            raise ValueError("Database schema changed during scan")
        return ops, inventory

    def schema_signature(self, ops):
        touched = sorted({o["table"] for o in ops})
        structures = []
        if self.kind == "mysql":
            for table in touched:
                structures.append(
                    self.execute("SHOW CREATE TABLE " + self.q(table))["rows"]
                )
            structures.append(
                self.execute(
                    "SELECT TRIGGER_NAME,EVENT_OBJECT_TABLE,ACTION_STATEMENT FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA=%s",
                    (self.endpoint["database"],),
                )["rows"]
            )
        elif self.kind == "sqlite":
            for table in touched:
                structures.append(
                    self.execute(
                        "SELECT type,name,sql FROM sqlite_master WHERE tbl_name=? ORDER BY type,name",
                        (table,),
                    )["rows"]
                )
        else:
            for meta in self.tables():
                if meta["name"] in touched:
                    structures.append(
                        dict(meta, uniqueIndexes=self.unique_indexes(meta))
                    )
            structures.append(self.call({"action": "triggers"})["tables"])
        structures.append(self.foreign_keys())
        return sha(dumps(structures).encode())

    def foreign_keys(self):
        if self.kind == "mysql":
            return self.execute(
                "SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, REFERENCED_TABLE_SCHEMA, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME, CONSTRAINT_NAME, ORDINAL_POSITION FROM information_schema.KEY_COLUMN_USAGE WHERE REFERENCED_TABLE_NAME IS NOT NULL AND (TABLE_SCHEMA=%s OR REFERENCED_TABLE_SCHEMA=%s) ORDER BY TABLE_SCHEMA,TABLE_NAME,CONSTRAINT_NAME,ORDINAL_POSITION",
                (self.endpoint["database"], self.endpoint["database"]),
            )["rows"]
        if self.kind == "sqlite":
            return {
                t["name"]: self.execute(
                    "PRAGMA foreign_key_list(" + self.q(t["name"]) + ")"
                )["rows"]
                for t in self.tables()
            }
        return self.call({"action": "foreign_keys"})["keys"]

    def mysql_fk_counts(self):
        groups = collections.defaultdict(list)
        for fk in self.foreign_keys():
            groups[
                (fk["TABLE_SCHEMA"], fk["TABLE_NAME"], fk["CONSTRAINT_NAME"])
            ].append(fk)
        result = {}
        for key, fields in groups.items():
            first = fields[0]
            child = self.q(first["TABLE_SCHEMA"]) + "." + self.q(first["TABLE_NAME"])
            parent = (
                self.q(first["REFERENCED_TABLE_SCHEMA"])
                + "."
                + self.q(first["REFERENCED_TABLE_NAME"])
            )
            equality = " AND ".join(
                "c."
                + self.q(f["COLUMN_NAME"])
                + "=p."
                + self.q(f["REFERENCED_COLUMN_NAME"])
                for f in fields
            )
            nonnull = " AND ".join(
                "c." + self.q(f["COLUMN_NAME"]) + " IS NOT NULL" for f in fields
            )
            rows = self.execute(
                "SELECT COUNT(*) AS n FROM "
                + child
                + " c WHERE "
                + nonnull
                + " AND NOT EXISTS (SELECT 1 FROM "
                + parent
                + " p WHERE "
                + equality
                + ")"
            )["rows"]
            result[key] = rows[0]["n"]
        return result

    def unique_indexes(self, meta):
        if self.kind == "mysql":
            rows = self.execute("SHOW INDEX FROM " + self.q(meta["name"]))["rows"]
            groups = collections.defaultdict(list)
            for row in rows:
                if not row["Non_unique"]:
                    groups[row["Key_name"]].append(row)
            result = []
            for rows in groups.values():
                if any(r.get("Sub_part") or not r["Column_name"] for r in rows):
                    raise ValueError(
                        "Prefix/function unique index needs an explicit adapter on "
                        + meta["name"]
                    )
                result.append(
                    [
                        r["Column_name"]
                        for r in sorted(rows, key=lambda r: r["Seq_in_index"])
                    ]
                )
            if meta.get("locator") == "verified-plugin-identity":
                result.append(meta["pk"])
            return result
        if self.rpc:
            return self.call({"action": "unique_indexes", "table": meta["name"]})[
                "indexes"
            ]
        result = [meta["pk"]] if meta["pk"] else []
        for index in self.execute("PRAGMA index_list(" + self.q(meta["name"]) + ")")[
            "rows"
        ]:
            if index["unique"]:
                columns = self.execute(
                    "PRAGMA index_info(" + self.q(index["name"]) + ")"
                )["rows"]
                if index.get("partial") or any(c["name"] is None for c in columns):
                    continue  # The full SQLite clone enforces expressions and partial indexes.
                result.append([c["name"] for c in columns])
        return result

    def check_unique(self, ops):
        tables = {t["name"]: t for t in self.tables()}
        for name in {o["table"] for o in ops}:
            changes = [o for o in ops if o["table"] == name]
            for columns in self.unique_indexes(tables[name]):
                for op in changes:
                    if op["after"] is None or any(
                        op["after"][k] is None for k in columns
                    ):
                        continue
                    sql = (
                        "SELECT "
                        + self.selection(name)
                        + " FROM "
                        + self.q(name)
                        + " WHERE "
                        + " AND ".join(self.q(k) + "=" + self.mark() for k in columns)
                    )
                    found = self.execute(sql, [op["after"][k] for k in columns])["rows"]
                    for row in found:
                        if row != op["before"] and not any(
                            row == other["before"] for other in changes
                        ):
                            raise ValueError(
                                "Destination unique key already exists on " + name
                            )
                keys = [
                    tuple(o["after"][k] for k in columns)
                    for o in changes
                    if o["after"] is not None
                    and all(o["after"][k] is not None for k in columns)
                ]
                if len(keys) != len(set(keys)):
                    raise ValueError("Rename produces duplicate unique key on " + name)

    def semantic_integrity(self):
        """Validate plugin references that do not have declared SQL foreign keys."""
        tables = {self.semantic_table(t["name"]): t["name"] for t in self.tables()}
        result = {}
        for child, column, parent in (
            ("playerparticles_particle", "group_uuid", "playerparticles_group"),
            ("playerparticles_fixed", "particle_uuid", "playerparticles_particle"),
        ):
            if child in tables and parent in tables:
                result[child] = self.execute(
                    "SELECT DISTINCT c."
                    + self.q(column)
                    + " AS orphan FROM "
                    + self.q(tables[child])
                    + " c LEFT JOIN "
                    + self.q(tables[parent])
                    + " p ON c."
                    + self.q(column)
                    + "=p."
                    + self.q("uuid")
                    + " WHERE c."
                    + self.q(column)
                    + " IS NOT NULL AND p."
                    + self.q("uuid")
                    + " IS NULL"
                )["rows"]
                result[child] = sorted(dumps(r) for r in result[child])
        for table, columns in (
            ("playerparticles_group", ["owner_uuid", "name"]),
            ("playerpoints_points", ["uuid"]),
        ):
            if table in tables:
                result[table + "#duplicates"] = sorted(
                    dumps(r)
                    for r in self.execute(
                        "SELECT "
                        + ",".join(self.q(c) for c in columns)
                        + ",COUNT(*) AS n FROM "
                        + self.q(tables[table])
                        + " GROUP BY "
                        + ",".join(self.q(c) for c in columns)
                        + " HAVING COUNT(*)>1"
                    )["rows"]
                )
        return result

    def matches_projection(self, ops, side, lock=False):
        expected = {}
        locators = {}
        for op in ops:
            for row in (op["before"], op["after"]):
                if row is not None:
                    key = (op["table"], dumps([row[k] for k in op["pk"]]))
                    locators[key] = (op, row)
            row = op[side]
            if row is not None:
                key = (op["table"], dumps([row[k] for k in op["pk"]]))
                if key in expected and expected[key] != row:
                    raise ValueError("Conflicting planned row projection")
                expected[key] = row
        return all(
            self.row(op["table"], op["pk"], row, lock) == expected.get(key)
            for key, (op, row) in locators.items()
        )

    def mutate(self, ops, reverse=False, identity=None):
        if reverse:
            ops = [dict(o, before=o["after"], after=o["before"]) for o in ops]
        self.rollback()
        if self.kind == "sqlite":
            self.execute("BEGIN IMMEDIATE")
            self.execute("PRAGMA defer_foreign_keys=ON")
        baseline = self.mysql_fk_counts() if self.kind == "mysql" else self.fk()
        semantic_before = self.semantic_integrity()
        # Check the entire row projection: an account merge may delete a target
        # row and move the source into the same primary key in one transaction.
        if self.matches_projection(ops, "after", lock=True):
            self.rollback()
            return
        if not self.matches_projection(ops, "before", lock=True):
            raise ValueError(
                "Row changed since manifest or partially committed database batch"
            )
        if identity is not None and not reverse:
            from identity import Identity

            current_identity = Identity(
                identity.old,
                identity.new,
                identity.premium,
                identity.rules,
                identity.merge,
            )
            current_identity.premium_uuid = identity.premium_uuid
            current_ops, _ = self.plan(current_identity)
            if (
                current_identity.collisions
                or current_identity.unknown
                or sorted(dumps(o) for o in current_ops)
                != sorted(dumps(o) for o in ops)
            ):
                raise ValueError("Database identity scope changed since scan")
        self.check_unique(ops)
        # MySQL has immediate foreign keys. Only this connection suspends checking,
        # with explicit inbound/outbound validation before commit and normal checks restored.
        if self.kind == "mysql":
            self.execute("SET SESSION FOREIGN_KEY_CHECKS=0")
        try:
            deletes = [op for op in ops if op["after"] is None]
            inserts = [op for op in ops if op["before"] is None]
            for op in deletes:
                result = self.execute(
                    "DELETE FROM "
                    + self.q(op["table"])
                    + " WHERE "
                    + " AND ".join(self.q(k) + "=" + self.mark() for k in op["pk"]),
                    [op["before"][k] for k in op["pk"]],
                )
                if result["count"] != 1:
                    raise ValueError("Unexpected merged row count")
            rekeys = [op for op in ops if op.get("method") == "insert-repoint-delete"]
            for op in rekeys:
                columns = [
                    k for k in op["after"] if k not in op.get("generatedColumns", [])
                ]
                result = self.execute(
                    "INSERT INTO "
                    + self.q(op["table"])
                    + " ("
                    + ",".join(self.q(k) for k in columns)
                    + ") VALUES ("
                    + ",".join(self.mark() for k in columns)
                    + ")",
                    [op["after"][k] for k in columns],
                )
                if result["count"] != 1:
                    raise ValueError("Unexpected staged insert count on " + op["table"])
            for op in ops:
                if op in rekeys or op in deletes or op in inserts:
                    continue
                before, after = op["before"], op["after"]
                columns = [
                    k
                    for k in after
                    if before[k] != after[k] and k not in op.get("generatedColumns", [])
                ]
                current = self.row(op["table"], op["pk"], before)
                if current is None and self.row(op["table"], op["pk"], after) == after:
                    continue  # An ON UPDATE CASCADE already performed this complete edit.
                locator = before if current is not None else after
                result = self.execute(
                    "UPDATE "
                    + self.q(op["table"])
                    + " SET "
                    + ",".join(self.q(k) + "=" + self.mark() for k in columns)
                    + " WHERE "
                    + " AND ".join(self.q(k) + "=" + self.mark() for k in op["pk"]),
                    [after[k] for k in columns] + [locator[k] for k in op["pk"]],
                )
                if result["count"] != 1:
                    raise ValueError("Unexpected update count on " + op["table"])
            for op in inserts:
                columns = [
                    k for k in op["after"] if k not in op.get("generatedColumns", [])
                ]
                self.execute(
                    "INSERT INTO "
                    + self.q(op["table"])
                    + " ("
                    + ",".join(self.q(k) for k in columns)
                    + ") VALUES ("
                    + ",".join(self.mark() for k in columns)
                    + ")",
                    [op["after"][k] for k in columns],
                )
            for op in rekeys:
                for fields in self.jdbc_foreign_key_groups().values():
                    first = fields[0]
                    if first["parent"] != op["table"] or not any(
                        op["before"][f["parentColumn"]]
                        != op["after"][f["parentColumn"]]
                        for f in fields
                    ):
                        continue
                    refs = self.execute(
                        "SELECT COUNT(*) AS n FROM "
                        + self.q(first["childSchema"])
                        + "."
                        + self.q(first["child"])
                        + " WHERE "
                        + " AND ".join(
                            self.q(f["childColumn"]) + "=" + self.mark() for f in fields
                        ),
                        [op["before"][f["parentColumn"]] for f in fields],
                    )["rows"][0]
                    if any(refs.values()):
                        raise ValueError(
                            "Unmigrated references prevent retiring " + op["table"]
                        )
                result = self.execute(
                    "DELETE FROM "
                    + self.q(op["table"])
                    + " WHERE "
                    + " AND ".join(self.q(k) + "=" + self.mark() for k in op["pk"]),
                    [op["before"][k] for k in op["pk"]],
                )
                if result["count"] != 1:
                    raise ValueError("Unexpected retired row count on " + op["table"])
            if not self.matches_projection(ops, "after"):
                raise ValueError("Final row verification failed")
            final = self.mysql_fk_counts() if self.kind == "mysql" else self.fk()
            if final != baseline:
                raise ValueError("Foreign-key integrity changed")
            if self.semantic_integrity() != semantic_before:
                raise ValueError("Plugin reference integrity changed")
            if self.kind == "sqlite" and self.execute("PRAGMA quick_check")["rows"] != [
                {"quick_check": "ok"}
            ]:
                raise ValueError("SQLite integrity failure")
            self.commit()
        except BaseException:
            self.rollback()
            raise
        finally:
            if self.kind == "mysql":
                self.execute("SET SESSION FOREIGN_KEY_CHECKS=1")

    def validate(self, ops):
        touched = {o["table"] for o in ops}
        if self.kind == "mysql":
            triggers = self.execute(
                "SELECT EVENT_OBJECT_TABLE AS table_name FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA=%s",
                (self.endpoint["database"],),
            )["rows"]
            if any(r["table_name"] in touched for r in triggers):
                raise ValueError(
                    "Touched MySQL table has triggers; a side-effect adapter is required"
                )
            self.mysql_fk_counts()
        elif self.kind == "sqlite":
            triggers = self.execute(
                "SELECT tbl_name FROM sqlite_master WHERE type='trigger'"
            )["rows"]
            if any(r["tbl_name"] in touched for r in triggers):
                raise ValueError(
                    "Touched SQLite table has triggers; a side-effect adapter is required"
                )
        elif self.call({"action": "triggers"})["tables"]:
            if touched & set(self.call({"action": "triggers"})["tables"]):
                raise ValueError(
                    "Touched JDBC table has triggers; a side-effect adapter is required"
                )
        self.check_unique(ops)
        if self.kind == "sqlite":
            # Preserve actual indexes, collations, triggers and constraints in the disposable copy.
            clone = Database.__new__(Database)
            clone.cfg = self.cfg
            clone.endpoint = self.endpoint
            clone.kind = "sqlite"
            clone.rpc = None
            clone.readonly = False
            clone.rowid_tables = set()
            clone.c = sqlite3.connect(":memory:")
            clone.c.row_factory = sqlite3.Row
            try:
                self.c.backup(clone.c)
                clone.execute("PRAGMA foreign_keys=ON")
                clone.mutate(ops)
                clone.mutate(ops, True)
                if not clone.matches_projection(ops, "before"):
                    raise ValueError("Copy rollback verification failed")
            finally:
                clone.close()
        elif self.rpc:
            self.validate_jdbc_copy(ops)

    def validate_jdbc_copy(self, ops):
        source = safe_path(self.cfg["networkRoot"], self.endpoint["path"])
        with tempfile.TemporaryDirectory(prefix="hammermigrate-jdbc-") as directory:
            root = Path(directory)
            if self.kind == "h2":
                shutil.copy2(source, root / source.name)
            else:
                prefix = str(source)[:-11]
                for suffix in (
                    ".properties",
                    ".script",
                    ".data",
                    ".backup",
                    ".log",
                    ".lobs",
                ):
                    path = Path(prefix + suffix)
                    if path.is_file():
                        shutil.copy2(path, root / path.name)
                    elif path.is_dir():
                        shutil.copytree(path, root / path.name)
            endpoint = dict(
                self.endpoint,
                path=source.name,
                user=self.jdbc_user,
                password=self.jdbc_password,
            )
            clone = Database({"networkRoot": directory}, endpoint)
            try:
                clone.mutate(ops)
                clone.mutate(ops, True)
                if not clone.matches_projection(ops, "before"):
                    raise ValueError("JDBC copy rollback mismatch")
            finally:
                clone.close()
