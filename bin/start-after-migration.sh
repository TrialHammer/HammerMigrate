#!/usr/bin/env bash
set -euo pipefail

# Public map output needs readable files and traversable directories.
# The migration worker sets its own private mask independently.
umask 022
if (( $# < 2 )); then
  printf '%s\n' 'Usage: start-after-migration.sh /absolute/network.json command [arguments...]' >&2
  exit 2
fi
migration_config=$1
shift
migration_project=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
migration_jar=${HAMMERMIGRATE_JAR:-"$migration_project/target/HammerMigrate-1.0.0.jar"}
java -jar "$migration_jar" --config "$migration_config" run
exec "$@"
