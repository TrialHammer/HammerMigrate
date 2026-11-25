"""Exercise the actual Paper/Bungee plugin command and startup gate in a temporary server."""

import argparse
import json
from pathlib import Path
import queue
import shutil
import socket
import subprocess
import tempfile
import threading
import time


def exercise(args):
    with tempfile.TemporaryDirectory(prefix="hammermigrate-platform-") as directory:
        root = Path(directory)
        server = root / args.platform
        (server / "plugins").mkdir(parents=True)
        shutil.copy2(args.plugin_jar, server / "plugins/HammerMigrate.jar")
        shutil.copy2(args.server_jar, server / "server.jar")
        for name in ("libraries", "versions", "cache"):
            source = getattr(args, name)
            if source:
                shutil.copytree(source, server / name)
        sock = socket.socket()
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
        sock.close()
        config = root / "network.json"
        config.write_text(
            json.dumps(
                {
                    "networkRoot": str(root),
                    "servers": [args.platform],
                    "startup": "external",
                }
            )
        )
        if args.platform == "bungee":
            # JSON is also valid YAML. Every network destination stays inside the test host.
            (server / "config.yml").write_text(
                json.dumps(
                    {
                        "online_mode": False,
                        "ip_forward": True,
                        "listeners": [
                            {
                                "host": f"127.0.0.1:{port}",
                                "motd": "Isolated verification",
                                "max_players": 1,
                                "priorities": ["lobby"],
                                "query_enabled": False,
                                "ping_passthrough": False,
                            }
                        ],
                        "servers": {
                            "lobby": {
                                "address": "127.0.0.1:1",
                                "motd": "fixture",
                                "restricted": False,
                            }
                        },
                    }
                )
            )
            (server / "modules.yml").write_text("version: 2\nmodules: []\n")
        else:
            (server / "eula.txt").write_text("eula=true\n")
            (server / "server.properties").write_text(
                f"server-ip=127.0.0.1\nserver-port={port}\nonline-mode=false\nmax-players=1\nview-distance=2\nsimulation-distance=2\nspawn-protection=0\ngenerate-structures=false\nlevel-type=minecraft:flat\n"
            )
        command = [
            "java",
            "-Xms128M",
            "-Xmx768M",
            "-Dhammermigrate.config=" + str(config),
            "-jar",
            "server.jar",
        ]
        if args.platform == "paper":
            command.append("--nogui")
        for pending in (False, True):
            output = []
            process = subprocess.Popen(
                command,
                cwd=server,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
            )
            lines = queue.Queue()

            def read():
                for line in process.stdout:
                    output.append(line)
                    lines.put(line)

            thread = threading.Thread(target=read, daemon=True)
            thread.start()
            try:
                deadline = time.monotonic() + args.timeout
                listening = queued = blocked = False
                while time.monotonic() < deadline:
                    try:
                        line = lines.get(timeout=0.2)
                    except queue.Empty:
                        if process.poll() is not None:
                            break
                        continue
                    ready = (
                        "Listening on" in line
                        if args.platform == "bungee"
                        else "Done (" in line
                    )
                    if ready:
                        listening = True
                        if not pending:
                            process.stdin.write(
                                "hmigrate queue TianyaOVO TianyaQAQ --premium off\n"
                            )
                            process.stdin.flush()
                    if "Queued " in line:
                        queued = True
                        process.stdin.write(
                            "end\n" if args.platform == "bungee" else "stop\n"
                        )
                        process.stdin.flush()
                    if "Pending jobs require pre-start" in line:
                        blocked = True
                if pending:
                    if (
                        not blocked
                        or listening
                        or process.poll() is None
                        or process.returncode == 0
                    ):
                        raise AssertionError(
                            "Pending queue must exit before server readiness"
                        )
                elif not (listening and queued):
                    raise AssertionError("Console queue command failed")
            except BaseException:
                print("".join(output))
                raise
            finally:
                if process.poll() is None:
                    process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
                thread.join(timeout=2)
                process.stdin.close()
                process.stdout.close()
        jobs = json.loads((root / "migrate/HammerMigrate/queue.json").read_text())
        if jobs[0]["status"] != "queued":
            raise AssertionError("Console queue was not persisted")
        print(
            args.platform
            + " plugin loaded, command queued, pending migration exited startup with a nonzero status"
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--platform", choices=["paper", "bungee"], required=True)
    parser.add_argument("--server-jar", type=Path, required=True)
    parser.add_argument("--plugin-jar", type=Path, required=True)
    for name in ("libraries", "versions", "cache"):
        parser.add_argument("--" + name, type=Path)
    parser.add_argument("--timeout", type=int, default=90)
    exercise(parser.parse_args())
