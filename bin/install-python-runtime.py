#!/usr/bin/env python3
"""Install the pinned shared runtime from the host before starting containers."""

import argparse
import concurrent.futures
import hashlib
import json
from pathlib import Path
import shutil
import tarfile
import tempfile
import urllib.request

BUILD = "20261110"
VERSION = "3.12.16"
ARTIFACTS = {
    "gnu": "6a9e62dad033790aef722dbd370e0ce76ce17b73627ba3ae679f537049350b39",
    "musl": "e32685cf248d2f33b249816120774143b4b57578cb30de5b8dba3e54963765c3",
}


def prepare(platform, digest, staging):
    name = f"cpython-{VERSION}+{BUILD}-x86_64-unknown-linux-{platform}-install_only_stripped.tar.gz"
    url = f"https://github.com/astral-sh/python-build-standalone/releases/download/{BUILD}/{name}"
    archive = staging / name
    request = urllib.request.Request(url, headers={"User-Agent": "HammerMigrate"})
    checksum = hashlib.sha256()
    with urllib.request.urlopen(request, timeout=120) as response, archive.open(
        "wb"
    ) as target:
        for block in iter(lambda: response.read(1024 * 1024), b""):
            checksum.update(block)
            target.write(block)
    if checksum.hexdigest() != digest:
        raise ValueError("Runtime checksum mismatch for " + name)
    unpacked = staging / platform
    with tarfile.open(archive) as source:
        source.extractall(unpacked, filter="data")
    target = staging / "runtime" / ("linux-x86_64-" + platform)
    (unpacked / "python").rename(target)
    print("Verified and extracted " + name, flush=True)
    return {"platform": platform, "url": url, "sha256": digest}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument(
        "--certificates", type=Path, default=Path("/etc/ssl/certs/ca-certificates.crt")
    )
    args = parser.parse_args()
    destination = args.directory.resolve()
    if destination.exists():
        raise ValueError("Runtime directory already exists: " + str(destination))
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix=".python-install-", dir=destination.parent
    ) as temporary:
        staging = Path(temporary)
        runtime = staging / "runtime"
        runtime.mkdir()
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            futures = [
                executor.submit(prepare, platform, digest, staging)
                for platform, digest in ARTIFACTS.items()
            ]
            artifacts = [future.result() for future in futures]
        shutil.copyfile(
            Path(__file__).with_name("python-runtime.sh"), runtime / "python"
        )
        (runtime / "python").chmod(0o755)
        shutil.copyfile(args.certificates, runtime / "ca-certificates.crt")
        (runtime / "manifest.json").write_text(
            json.dumps(
                {
                    "python": VERSION,
                    "build": BUILD,
                    "architecture": "x86_64",
                    "artifacts": artifacts,
                },
                indent=2,
            )
            + "\n"
        )
        runtime.rename(destination)
    print("Installed " + str(destination), flush=True)


if __name__ == "__main__":
    main()
