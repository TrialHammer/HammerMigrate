"""Offline migration across containers sharing only the coordination directory."""

import contextlib
import ctypes
import json
import select
import sys
import fcntl
import os
from pathlib import Path
import re
import signal
import subprocess
import time
import uuid

from common import dumps, file_sha, load, save, sha
from database import Database
from discovery import configs, quiescent, related_databases, ScopeChanged
import worker

PENDING = {"queued", "running", "rollback_queued", "recovery_required"}


@contextlib.contextmanager
def locked(path, blocking=True):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX | (0 if blocking else fcntl.LOCK_NB))
        yield handle


def active(path):
    try:
        with locked(path, blocking=False):
            return False
    except BlockingIOError:
        return True


def local_config(cfg, server, directory):
    directory = Path(directory).resolve()
    if directory.parent == directory:
        raise ValueError("Server data must be mounted in a directory such as /data")
    result = dict(cfg)
    result.update(
        networkRoot=str(directory.parent),
        servers=[directory.name],
        stateDirectory=str(worker.state(cfg) / "participants" / server),
        _distributedPartial=True,
    )
    result["serverHosts"] = {
        directory.name: cfg.get("serverHosts", {}).get(server, [server])
    }
    result["ignorePaths"] = [
        directory.name + value[len(server) :]
        for value in cfg.get("ignorePaths", [])
        if value == server or value.startswith(server + "/")
    ]
    result["embeddedDatabases"] = {
        directory.name + key[len(server) :]: value
        for key, value in cfg.get("embeddedDatabases", {}).items()
        if key.startswith(server + "/")
    }
    result.setdefault(
        "schemaPatterns",
        [
            r"^(?:global_|cryo_|"
            + "|".join(re.escape(s) + "_" for s in cfg["servers"])
            + ")",
            r"^authme$",
        ],
    )
    return result


def endpoint_key(endpoint):
    return [endpoint.get("host"), int(endpoint.get("port", 3306)), endpoint["database"]]


def dry_run_failure(session):
    return (
        session.get("phase") == "failed"
        and not session.get("recoveryRequired")
        and session.get("identity", {}).get("dryRun", False)
    )


def aggregate(cfg, job, round_dir):
    reports = {}
    errors = []
    for server in cfg["servers"]:
        path = (
            worker.state(cfg)
            / "participants"
            / server
            / "runs"
            / job["id"]
            / "manifest.json"
        )
        report = load(path) if path.exists() else {}
        reports[server] = {
            "manifest": str(path),
            "coverage": report.get("coverage", {"complete": False}),
            "ready": report.get("ready", False),
            "actions": report.get("actions", 0),
        }
        if not report.get("ready", False):
            errors.append(
                {"server": server, "error": "Participant manifest is blocked"}
            )
    actual = [
        load(Path(v["manifest"])) if Path(v["manifest"]).exists() else {}
        for v in reports.values()
    ]
    if not any(r.get("sourceEvidence") for r in actual):
        errors.append({"error": "No source player data found on any participant"})
    auth = [a for r in actual for a in r.get("authAccounts", [])]
    if len(auth) > 1:
        errors.append({"error": "Multiple authoritative AuthMe accounts found"})
    if job["premium"]:
        login = [entry for r in actual for entry in r.get("loginConfiguration", [])]
        errors.extend(
            worker.premium_errors(
                login,
                auth,
                any(
                    Path(p["path"]).name == "premium_names.cache"
                    for r in actual
                    for p in r.get("physical", [])
                ),
            )
        )
        if len({r.get("premiumUuid") for r in actual}) != 1:
            errors.append(
                {"error": "Mojang identity changed between participant scans"}
            )
    report = {
        "version": 2,
        "deployment": "distributed",
        "job": job,
        "participants": reports,
        "errors": errors,
        "coverage": {
            "complete": all(
                r.get("coverage", {}).get("complete", False) for r in actual
            ),
            "discoveredFiles": sum(
                r.get("coverage", {}).get("discoveredFiles", 0) for r in actual
            ),
            "coveredFiles": sum(
                r.get("coverage", {}).get("coveredFiles", 0) for r in actual
            ),
        },
        "ready": not errors and all(r.get("ready", False) for r in actual),
        "participantResults": load(round_dir / "session.json")["results"],
        "databaseAssignments": load(round_dir / "session.json").get("assignments", {}),
    }
    folder = worker.state(cfg) / ("scans" if job.get("dryRun") else "runs") / job["id"]
    report["completion"] = (
        "complete"
        if report["ready"] and report["coverage"]["complete"]
        else "with-exclusions" if report["ready"] else "blocked"
    )
    save(folder / "manifest.json", report)
    return report


def coordinate(cfg, protocol):
    """Called under the same lock used by queue commands. All writes are durable."""
    items = worker.jobs(cfg)
    if worker.blocking_failures(items):
        raise ValueError("Failed jobs require retry or cancel before startup")
    job = next((j for j in items if j["status"] in PENDING), None)
    if not job:
        return None
    registrations = {}
    for server in cfg["servers"]:
        path = protocol / "registrations" / (server + ".json")
        if not path.exists() or not active(protocol / "launchers" / (server + ".lock")):
            return {"phase": "waiting", "missing": server}
        registrations[server] = load(path)
        if active(protocol / "online" / (server + ".lock")):
            return {"phase": "waiting", "online": server}
    if len({v["configSha256"] for v in registrations.values()}) != 1:
        raise ValueError("Participants loaded different shared configurations")
    tokens = {s: r["instance"] for s, r in registrations.items()}
    round_id = job.get("distributedRound")
    if round_id:
        round_dir = protocol / "rounds" / round_id
        session = load(round_dir / "session.json")
        if set(session["members"]) != set(tokens) and (
            session.get("writeAttempted")
            or session["phase"] in ("applying", "recovering", "rolling_back")
        ):
            raise ValueError(
                "Participant topology changed after writes; restore original participants for recovery"
            )
        if session["members"] != tokens and session["phase"] in (
            "discovering",
            "scanning",
        ):
            session.update(
                phase="discovering", members=tokens, results={}, assignments={}
            )
            save(round_dir / "session.json", session)
        elif session["members"] != tokens and session["phase"] not in (
            "done",
            "failed",
        ):
            session.update(
                phase="recovering",
                members=tokens,
                error="Participant restarted during migration",
                results={},
            )
            save(round_dir / "session.json", session)
    else:
        round_id = worker.stamp()
        round_dir = protocol / "rounds" / round_id
        session = {
            "id": round_id,
            "job": job["id"],
            "members": tokens,
            "phase": (
                "rolling_back" if job["status"] == "rollback_queued" else "discovering"
            ),
            "results": {},
            "assignments": {},
        }
        job["distributedRound"] = round_id
        save(round_dir / "session.json", session)
        worker.setjobs(cfg, items)
    phase = session["phase"]
    results = session["results"]
    if (
        phase == "scanning"
        and len(results) == len(tokens)
        and any(v.get("rescan") for v in results.values())
        and session.get("rescans", 0) < cfg.get("scanMaxPasses", 3) - 1
    ):
        session.update(
            phase="discovering",
            results={},
            assignments={},
            rescans=session.get("rescans", 0) + 1,
        )
    elif phase == "discovering" and len(results) == len(tokens):
        if any("error" in v for v in results.values()):
            session.update(phase="failed", error="Participant discovery failed")
        else:
            owners = {}
            assignments = {s: [] for s in tokens}
            for server in cfg["servers"]:
                offer = load(round_dir / server / "discovery.json")
                for endpoint in offer["endpoints"]:
                    key = endpoint["serverIdentity"] + [endpoint["database"]]
                    if dumps(key) not in owners:
                        owners[dumps(key)] = server
                        assignments[server].append(endpoint_key(endpoint))
            session.update(phase="scanning", results={}, assignments=assignments)
    elif phase == "scanning" and len(results) == len(tokens):
        report = aggregate(cfg, job, round_dir)
        if any("error" in v for v in results.values()):
            session.update(phase="failed", error="Participant scan failed")
        else:
            if not report["ready"]:
                session.update(phase="failed", error="Distributed manifest is blocked")
            elif job.get("dryRun"):
                session["phase"] = "done"
            else:
                session.update(phase="applying", results={})
                job["status"] = "running"
                session["writeAttempted"] = True
    elif phase == "cancelling" and len(results) == len(tokens):
        if session.get("cancelNeedsRecovery"):
            session.update(
                phase="recovering",
                results={},
                error="Migration cancelled by operator",
            )
        else:
            session["phase"] = "cancelled"
    elif phase == "applying" and len(results) == len(tokens):
        if any("error" in v for v in results.values()):
            session.update(
                phase="recovering",
                results={},
                error="A participant failed to apply its plan",
            )
        else:
            session["phase"] = "done"
    elif phase in ("recovering", "rolling_back") and len(results) == len(tokens):
        if any("error" in v for v in results.values()):
            session.update(
                phase="failed",
                recoveryRequired=True,
                error="Distributed rollback needs recovery",
            )
        elif phase == "recovering" and session.get("cancelRequested"):
            session["phase"] = "cancelled"
        elif phase == "recovering":
            session.update(
                phase="failed",
                error=session.get("error", "Interrupted migration recovered"),
            )
        else:
            session["phase"] = "done"
    if session["phase"] == "cancelled":
        job["status"] = "cancelled"
        job.pop("distributedRound", None)
    elif session["phase"] == "done":
        job["status"] = (
            "rolled_back"
            if phase == "rolling_back"
            else "scanned" if job.get("dryRun") else "done"
        )
        job["distributed"] = True
        job.pop("distributedRound", None)
    elif session["phase"] == "failed":
        job["status"] = (
            "recovery_required" if session.get("recoveryRequired") else "failed"
        )
        job["error"] = session["error"]
        job["preflightBlocked"] = not session.get("writeAttempted") and phase in (
            "discovering",
            "scanning",
        )
        if not session.get("recoveryRequired"):
            job.pop("distributedRound", None)
    save(round_dir / "session.json", session)
    worker.setjobs(cfg, items)
    return dict(session, roundDirectory=str(round_dir), identity=job)


def participant_step(cfg, local, server, session, held):
    phase = session["phase"]
    job = session["identity"]
    round_dir = Path(session["roundDirectory"])
    folder = worker.state(local) / "runs" / job["id"]
    if phase == "discovering":
        manifest = configs(local)
        if not held:
            held.extend(quiescent(local, manifest))
        for i, endpoint in enumerate(local.get("databaseEndpoints", [])):
            for item in manifest["endpoints"]:
                if endpoint_key(item) == endpoint_key(endpoint):
                    item["_configuredIndex"] = i
        related_databases(local, manifest)
        if manifest["discoveryErrors"] or manifest.get("databaseDiscoveryErrors"):
            raise ValueError(
                "Local discovery failed: "
                + dumps(
                    manifest.get("discoveryErrors", [])
                    + manifest.get("databaseDiscoveryErrors", [])
                )
            )
        for endpoint in manifest["endpoints"]:
            db = Database(
                local, worker.endpoint_runtime(local, endpoint), readonly=True
            )
            try:
                identity = db.execute(
                    "SELECT @@hostname AS hostname, @@port AS port, @@server_id AS server_id"
                )["rows"][0]
                endpoint["serverIdentity"] = [
                    str(identity["hostname"]),
                    int(identity["port"]),
                    int(identity["server_id"]),
                ]
            finally:
                db.rollback()
                db.close()
        manifest["endpoints"] = [
            worker.endpoint_public(e) for e in manifest["endpoints"]
        ]
        save(round_dir / server / "discovery.json", manifest)
    elif phase == "scanning":
        local["_mysqlScope"] = session["assignments"][server]
        offer = load(round_dir / server / "discovery.json")
        local["_mysqlOfferedScope"] = [endpoint_key(e) for e in offer["endpoints"]]
        report, _ = worker.discover_plan(
            local, job, folder, fresh=job.get("fresh", False)
        )
        if not report["ready"]:
            raise ValueError(
                "Local manifest is blocked: " + str(folder / "manifest.json")
            )
    elif phase == "applying":
        quiescent(local, configs(local), world_locks=False)
        worker.apply_run(local, folder, worker.read_plan(folder))
    elif phase in ("rolling_back", "recovering"):
        if not held:
            held.extend(quiescent(local, configs(local)))
        if (folder / "journal.jsonl").exists():
            worker.rollback_run(local, folder)


def serve(command, directory, online):
    child = None

    def forward(signum, frame):
        if child is not None:
            child.send_signal(signum)
        else:
            raise SystemExit(128 + signum)

    previous = {s: signal.signal(s, forward) for s in (signal.SIGTERM, signal.SIGINT)}
    try:
        # Migration journals use 077. The game creates public map files and
        # must not inherit that mask, including direct worker/CLI launches.
        child = subprocess.Popen(command, cwd=directory, umask=0o022)
        return child.wait()
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)
        online.close()


def startup_command(cfg, protocol, line):
    """Handle one operator command while the server is still offline."""
    parts = line.strip().split()
    if not parts:
        return
    action = parts[0].lower()
    if action == "help":
        print(
            "HammerMigrate commands: status | cancel <job-id> | "
            "retry <job-id> | rollback <job-id>",
            flush=True,
        )
        return
    if action == "status" and len(parts) == 1:
        with locked(worker.state(cfg) / "coordinator.lock"):
            for job in worker.jobs(cfg):
                print(
                    "HammerMigrate",
                    job["id"],
                    job["status"],
                    job.get("error", ""),
                    flush=True,
                )
        return
    if len(parts) != 2 or action not in ("cancel", "retry", "rollback"):
        print("HammerMigrate: use help for startup commands", flush=True)
        return
    try:
        if action != "cancel":
            # Queue operations acquire the coordinator lock themselves.
            worker.main(["--config", cfg["_configPath"], action, parts[1]])
            return
        with locked(worker.state(cfg) / "coordinator.lock"):
            items = worker.jobs(cfg)
            job = next((j for j in items if j["id"] == parts[1]), None)
            if not job:
                raise ValueError("Unknown queue id")
            round_id = job.get("distributedRound")
            if round_id:
                path = protocol / "rounds" / round_id / "session.json"
                session = load(path)
                phase = session["phase"]
                if phase in ("recovering", "rolling_back"):
                    raise ValueError("Recovery is in progress; wait for it to finish")
                if phase not in ("discovering", "scanning", "applying", "cancelling"):
                    raise ValueError("This round needs recovery before cancellation")
                if phase != "cancelling":
                    session.update(
                        phase="cancelling",
                        results={},
                        cancelRequested=True,
                        cancelNeedsRecovery=phase == "applying",
                    )
                    save(path, session)
                print(
                    "HammerMigrate cancellation requested; waiting for all participants",
                    flush=True,
                )
            elif job["status"] in ("queued", "failed"):
                job["status"] = "cancelled"
                worker.setjobs(cfg, items)
                print("HammerMigrate cancelled", job["id"], flush=True)
            else:
                raise ValueError("This job needs recovery before cancellation")
    except (ValueError, OSError) as error:
        print("HammerMigrate:", error, flush=True)


def start_step(cfg, local, server, session, held, coordinator_fd):
    """Run a phase in its own process group so an operator can interrupt it."""
    read_fd, write_fd = os.pipe()
    parent_pid = os.getpid()
    pid = os.fork()
    if pid == 0:
        os.close(read_fd)
        os.close(coordinator_fd)
        try:
            os.setsid()
            libc = ctypes.CDLL(None, use_errno=True)
            if libc.prctl(1, signal.SIGKILL) != 0 or os.getppid() != parent_pid:
                os._exit(1)
            result = {}
            try:
                participant_step(cfg, local, server, session, held)
            except Exception as error:
                result["error"] = str(error)[:1000]
                if isinstance(error, ScopeChanged):
                    result["rescan"] = True
            os.write(write_fd, json.dumps(result).encode())
        finally:
            os.close(write_fd)
            os._exit(0)
    os.close(write_fd)
    return pid, read_fd


def record_step_result(server, instance, child_session, session, result):
    """Persist a completed step even when another participant is absent."""
    path = Path(child_session["roundDirectory"]) / "session.json"
    current = load(path)
    if (
        current["phase"] == child_session["phase"]
        and current["members"].get(server) == instance
    ):
        current["results"][server] = result
        save(path, current)
        if session.get("id") == child_session["id"]:
            session["results"][server] = result


def stop_step(pid, read_fd):
    try:
        os.killpg(pid, signal.SIGKILL)
    except ProcessLookupError:
        try:
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    os.waitpid(pid, 0)
    os.close(read_fd)


def launch(cfg, server, directory, command):
    if cfg.get("deployment") != "distributed":
        raise ValueError(
            "Per-server launch requires deployment=distributed in /network shared config"
        )
    if server not in cfg["servers"] or not re.fullmatch(r"[A-Za-z0-9_-]+", server):
        raise ValueError("Unknown participant " + server)
    if cfg.get("startup", "external") != "external":
        raise ValueError("Distributed launch requires startup=external")
    if command and command[0] == "--":
        command = command[1:]
    if not command:
        raise ValueError("Supply the original server startup command after --")
    local = local_config(cfg, server, directory)
    protocol = worker.state(cfg) / "distributed"
    instance = uuid.uuid4().hex
    engine = Path(__file__).resolve().parent
    config_digest = sha(
        dumps(
            {
                "config": cfg,
                "engine": [
                    (str(p.relative_to(engine)), file_sha(p))
                    for p in sorted(engine.rglob("*.py"))
                ],
                "jar": file_sha(worker.nbt.JAR) if worker.nbt.JAR else None,
            }
        ).encode()
    )
    held = []
    held_round = None
    last_notice = 0
    child = None
    stdin_open = True
    input_buffer = b""

    def stop(signum, frame):
        raise SystemExit(128 + signum)

    old_signals = {s: signal.signal(s, stop) for s in (signal.SIGTERM, signal.SIGINT)}
    try:
        with contextlib.ExitStack() as lifetime:
            with locked(worker.state(cfg) / "coordinator.lock"):
                lifetime.enter_context(
                    locked(protocol / "launchers" / (server + ".lock"), blocking=False)
                )
                save(
                    protocol / "registrations" / (server + ".json"),
                    {"instance": instance, "configSha256": config_digest},
                )
            print(
                "HammerMigrate startup console: help | status | cancel <job-id>",
                flush=True,
            )
            while True:
                if stdin_open:
                    try:
                        readable, _, _ = select.select([sys.stdin], [], [], 0)
                        if readable:
                            chunk = os.read(sys.stdin.fileno(), 4096)
                            if not chunk:
                                stdin_open = False
                            else:
                                input_buffer += chunk
                                while b"\n" in input_buffer:
                                    line, input_buffer = input_buffer.split(b"\n", 1)
                                    startup_command(
                                        cfg, protocol, line.decode(errors="replace")
                                    )
                    except (ValueError, OSError):
                        stdin_open = False
                with locked(
                    worker.state(cfg) / "coordinator.lock"
                ) as coordinator_handle:
                    # A participant can stop immediately even while another node is absent.
                    for queued in worker.jobs(cfg):
                        round_id = queued.get("distributedRound")
                        if not round_id:
                            continue
                        path = protocol / "rounds" / round_id / "session.json"
                        current = load(path)
                        if (
                            current["phase"] == "cancelling"
                            and current["members"].get(server) == instance
                        ):
                            if child is not None:
                                stop_step(*child)
                                child = None
                            if server not in current["results"]:
                                current["results"][server] = {}
                                save(path, current)
                            break
                    try:
                        session = coordinate(cfg, protocol)
                    except ValueError as error:
                        session = {"phase": "blocked", "error": str(error)}
                    if session is None and child is None:
                        online_path = protocol / "online" / (server + ".lock")
                        online_path.parent.mkdir(parents=True, exist_ok=True)
                        online = online_path.open("a+")
                        fcntl.flock(online, fcntl.LOCK_EX | fcntl.LOCK_NB)
                        break
                    phase = session["phase"]
                    if child is not None and phase not in (
                        "waiting",
                        "blocked",
                        child_session["phase"],
                    ):
                        stop_step(*child)
                        child = None
                    if phase == "cancelling":
                        if child is not None:
                            stop_step(*child)
                            child = None
                        if server not in session["results"]:
                            path = Path(session["roundDirectory"]) / "session.json"
                            current = load(path)
                            current["results"][server] = {}
                            save(path, current)
                    elif child is not None:
                        pid, read_fd = child
                        finished, _ = os.waitpid(pid, os.WNOHANG)
                        if finished:
                            raw = os.read(read_fd, 8192)
                            os.close(read_fd)
                            child = None
                            result = (
                                json.loads(raw)
                                if raw
                                else {"error": "Participant process terminated"}
                            )
                            record_step_result(
                                server, instance, child_session, session, result
                            )
                    if (
                        child is None
                        and phase
                        in {
                            "discovering",
                            "scanning",
                            "applying",
                            "recovering",
                            "rolling_back",
                        }
                        and server not in session["results"]
                    ):
                        if held_round != session["id"]:
                            for handle in held:
                                handle.close()
                            held.clear()
                            held_round = session["id"]
                        if phase == "discovering" and not held:
                            try:
                                held.extend(quiescent(local, configs(local)))
                            except Exception as error:
                                path = Path(session["roundDirectory"]) / "session.json"
                                current = load(path)
                                current["results"][server] = {
                                    "error": str(error)[:1000]
                                }
                                save(path, current)
                                session["results"][server] = current["results"][server]
                        if server not in session["results"]:
                            child_session = session
                            child = start_step(
                                cfg,
                                local,
                                server,
                                session,
                                held,
                                coordinator_handle.fileno(),
                            )
                if phase in ("done", "cancelled", "failed") and child is None:
                    for handle in held:
                        handle.close()
                    held.clear()
                if time.monotonic() - last_notice >= 15:
                    detail = session.get(
                        "error", session.get("missing", session.get("online", ""))
                    )
                    print(
                        "HammerMigrate participant", server, phase, detail, flush=True
                    )
                    last_notice = time.monotonic()
                time.sleep(float(cfg.get("participantPollSeconds", 1)))
            for handle in held:
                handle.close()
            held.clear()
            return serve(command, directory, online)
    finally:
        if child is not None:
            stop_step(*child)
        for handle in held:
            handle.close()
        for sig, handler in old_signals.items():
            signal.signal(sig, handler)
