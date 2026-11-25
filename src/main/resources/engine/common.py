"""Durable state and inverse patches. Only touched byte spans and SQL rows are stored."""

import base64, datetime, decimal, fcntl, hashlib, json, os, tempfile, uuid
from pathlib import Path


def encode(x):
    if isinstance(x, bytes):
        return {"$bytes": base64.b64encode(x).decode()}
    if isinstance(x, decimal.Decimal):
        return {"$decimal": str(x)}
    if isinstance(x, (datetime.datetime, datetime.date, datetime.time)):
        return {"$date": x.isoformat(), "$kind": type(x).__name__}
    if isinstance(x, datetime.timedelta):
        return {"$timedelta": x.total_seconds()}
    raise TypeError(type(x).__name__)


def decode(x):
    if set(x) == {"$bytes"}:
        return base64.b64decode(x["$bytes"])
    if set(x) == {"$decimal"}:
        return decimal.Decimal(x["$decimal"])
    if set(x) == {"$date", "$kind"}:
        return getattr(datetime, x["$kind"]).fromisoformat(x["$date"])
    if set(x) == {"$timedelta"}:
        return datetime.timedelta(seconds=x["$timedelta"])
    return x


def parse_json(text):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("Duplicate JSON mapping key")
            result[key] = value
        return result

    return json.loads(text, object_pairs_hook=unique)


def dumps(x):
    return json.dumps(x, ensure_ascii=False, default=encode, sort_keys=True)


def load(p):
    return json.loads(Path(p).read_text(), object_hook=decode)


def sha(b):
    return hashlib.sha256(b).hexdigest()


def file_sha(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def offline(name):
    return str(
        uuid.UUID(
            bytes=hashlib.md5(("OfflinePlayer:" + name).encode()).digest(), version=3
        )
    )


def syncdir(p):
    fd = os.open(p, os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def write(p, data):
    p = Path(p)
    p.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".hmigrate-", dir=p.parent)
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        if p.exists():
            st = p.stat()
            os.chmod(tmp, st.st_mode & 0o777)
            os.chown(tmp, st.st_uid, st.st_gid)
        os.replace(tmp, p)
        syncdir(p.parent)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def save(p, data):
    write(p, (dumps(data) + "\n").encode())


class Lock:
    def __init__(self, p, blocking=False):
        self.p = p
        self.blocking = blocking

    def __enter__(self):
        self.p.parent.mkdir(parents=True, exist_ok=True)
        self.f = self.p.open("a+")
        fcntl.flock(self.f, fcntl.LOCK_EX | (0 if self.blocking else fcntl.LOCK_NB))
        return self

    def __exit__(self, *args):
        self.f.close()


def safe_path(root, rel):
    root = Path(root).resolve()
    p = root / rel
    if ".." in Path(rel).parts or Path(rel).is_absolute():
        raise ValueError("Unsafe relative path " + rel)
    for part in [p, *p.parents]:
        if part == root:
            break
        if part.is_symlink():
            raise ValueError("Symlink in migration path " + str(p))
    if not p.resolve().is_relative_to(root):
        raise ValueError("Path escapes root")
    return p


def ignored_path(cfg, rel):
    return any(
        rel == item.rstrip("/") or rel.startswith(item.rstrip("/") + "/")
        for item in cfg.get("ignorePaths", [])
    )


def patch(before, after):
    start = 0
    end = 0
    while start < min(len(before), len(after)) and before[start] == after[start]:
        start += 1
    while (
        end < min(len(before), len(after)) - start
        and before[len(before) - end - 1] == after[len(after) - end - 1]
    ):
        end += 1
    return {
        "start": start,
        "old": before[start : len(before) - end if end else len(before)],
        "new": after[start : len(after) - end if end else len(after)],
        "before": sha(before),
        "after": sha(after),
    }


def patch_bytes(raw, change, reverse=False):
    if "pieces" in change:
        want = change["after" if reverse else "before"]
        done = change["before" if reverse else "after"]
        if sha(raw) == done:
            return raw
        if sha(raw) != want:
            raise ValueError("File changed since manifest / migration")
        for piece in sorted(change["pieces"], key=lambda x: x["start"], reverse=True):
            old, new = (
                (piece["new"], piece["old"])
                if reverse
                else (piece["old"], piece["new"])
            )
            pos = piece["start"]
            if raw[pos : pos + len(old)] != old:
                raise ValueError("Sector precondition failed")
            raw = raw[:pos] + new + raw[pos + len(old) :]
        if sha(raw) != done:
            raise ValueError("Sector patch verification failed")
        return raw
    old, new = (
        (change["new"], change["old"]) if reverse else (change["old"], change["new"])
    )
    want = change["after" if reverse else "before"]
    done = change["before" if reverse else "after"]
    if sha(raw) == done:
        return raw
    if sha(raw) != want:
        raise ValueError("File changed since manifest / migration")
    pos = change["start"]
    if raw[pos : pos + len(old)] != old:
        raise ValueError("Patch precondition failed")
    out = raw[:pos] + new + raw[pos + len(old) :]
    if sha(out) != done:
        raise ValueError("Patch verification failed")
    return out


class Journal:
    def __init__(self, path):
        self.path = Path(path)

    def events(self):
        if not self.path.exists():
            return []
        raw = self.path.read_bytes()
        # A truncated intent may precede no mutation, but is retained as an error.
        return [
            json.loads(line, object_hook=decode)
            for line in raw.splitlines(keepends=True)
            if line.endswith(b"\n")
        ]

    def append(self, event, **data):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if self.path.exists():
            raw = self.path.read_bytes()
            if raw and not raw.endswith(b"\n"):
                with self.path.open("r+b") as damaged:
                    damaged.truncate(raw.rfind(b"\n") + 1)
                    damaged.flush()
                    os.fsync(damaged.fileno())
        with self.path.open("ab") as f:
            f.write(
                (
                    dumps(
                        dict(
                            event=event,
                            at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                            **data
                        )
                    )
                    + "\n"
                ).encode()
            )
            f.flush()
            os.fsync(f.fileno())
        syncdir(self.path.parent)
