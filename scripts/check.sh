#!/usr/bin/env bash
# Run the safety model and the liveness model.
# Both must be accepted by TLC. Traces are not kept.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

JAVA="${JAVA:-java}"
JAR="${TLA2TOOLS_JAR:-$ROOT/.tools/tla2tools.jar}"
URL="https://github.com/tlaplus/tlaplus/releases/download/v1.7.4/tla2tools.jar"

if ! "$JAVA" -version >/dev/null 2>&1; then
  echo "Java 11 or newer is required. Install a JDK, or set JAVA=/path/to/java." >&2
  exit 1
fi

if [[ ! -f "$JAR" ]]; then
  mkdir -p "$(dirname "$JAR")"
  echo "Downloading tla2tools.jar (TLA+ 1.7.4)"
  curl -fsSL -o "$JAR" "$URL"
fi

run_model() {
  local name="$1"
  local cfg="$2"
  local metadir
  local log
  local status
  metadir="$(mktemp -d)"
  log="$(mktemp)"
  echo "---- ${name} (${cfg})"
  set +e
  "$JAVA" -XX:+UseParallelGC -cp "$JAR" tlc2.TLC \
    -workers 1 \
    -fpmem 0.1 \
    -nowarning \
    -cleanup \
    -metadir "$metadir" \
    -config "$cfg" \
    VelocityCounter.tla >"$log" 2>&1
  status=$?
  set -e
  rm -rf "$metadir"
  if [[ "$status" -ne 0 ]] || ! grep -q "No error has been found" "$log"; then
    echo "FAIL ${name}: expected TLC to accept the model (exit ${status})" >&2
    tail -n 40 "$log" >&2
    rm -f "$log"
    exit 1
  fi
  grep "No error has been found" "$log"
  rm -f "$log"
  echo "ok ${name}"
}

run_model safety Safety.cfg
run_model liveness Liveness.cfg
echo "Both models passed."
