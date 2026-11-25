#!/usr/bin/env bash
set -euo pipefail

# Public map output needs readable files and traversable directories.
# The migration worker sets its own private mask independently.
umask 022

migration_server=${HAMMERMIGRATE_SERVER:?Set HAMMERMIGRATE_SERVER to the participant name}
migration_directory=${HAMMERMIGRATE_DIRECTORY:-$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)}
migration_shared=${HAMMERMIGRATE_SHARED:-/network/HammerMigrate}
migration_config="$migration_shared/network.json"
migration_jar="$migration_directory/plugins/HammerMigrate.jar"

if [[ ! -r "$migration_config" || ! -r "$migration_jar" ]]; then
  printf '%s\n' "HammerMigrate requires $migration_config and $migration_jar" >&2
  exit 1
fi
if (( $# == 0 )); then
  printf '%s\n' 'Supply the original startup command, for example java -Xmx8G -jar server.jar --nogui' >&2
  exit 2
fi
if (( $# >= 2 )) && [[ ${1##*/} == bash && ${2##*/} == java ]]; then
  shift
fi

migration_engine=$(java -jar "$migration_jar" --extract "$migration_shared")
migration_python=$(java -jar "$migration_jar" --python "$migration_config")
export PYTHONPATH="$migration_engine/vendor"
export PYTHONDONTWRITEBYTECODE=1
export JDK_JAVA_OPTIONS="${JDK_JAVA_OPTIONS:+$JDK_JAVA_OPTIONS }-Dhammermigrate.config=$migration_config"
exec "$migration_python" -B "$migration_engine/worker.py" \
  --config "$migration_config" --jar "$migration_jar" \
  launch --server "$migration_server" --directory "$migration_directory" -- \
  bash -c 'umask 022; exec "$@"' bash "$@"
