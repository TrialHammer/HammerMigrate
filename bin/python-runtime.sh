#!/bin/sh
set -eu

runtime_root=$(CDPATH= cd -- "${0%/*}" && pwd)
runtime_platform=linux-x86_64-gnu
if [ -e /lib/ld-musl-x86_64.so.1 ]; then
  runtime_platform=linux-x86_64-musl
fi

unset PYTHONHOME
export SSL_CERT_FILE="${SSL_CERT_FILE:-$runtime_root/ca-certificates.crt}"
exec "$runtime_root/$runtime_platform/bin/python3" "$@"
