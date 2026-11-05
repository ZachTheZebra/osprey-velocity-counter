#!/bin/bash
# Run one Osprey worker over the two demo events, then check Redis.
set -euo pipefail
cd /osprey
. .venv/bin/activate
# Image PYTHONPATH=/osprey hides the split namespace, and the editable
# finders ship with empty mappings. Name both source trees.
unset LD_PRELOAD
export PYTHONPATH=/osprey/osprey_worker/src:/osprey/osprey_rpc/src
rm -rf /tmp/plugin
mkdir -p /tmp/plugin
cp -a /velocity/plugin/osprey_velocity /velocity/plugin/pyproject.toml /tmp/plugin/
# The venv has no pip. System pip would install where this python cannot see it.
# uv run would put the example plugins back, so install this package only.
cd /tmp
uv pip install --python /osprey/.venv/bin/python /tmp/plugin
# Those plugins are path files plus entry points, not packages pip can uninstall.
# Left in place they open Postgres and replace the input stream.
site=/osprey/.venv/lib/python3.11/site-packages
rm -rf \
  "$site/__editable__.example_plugins-0.1.0.pth" \
  "$site/__editable__.example_atproto_plugins-0.1.0.pth" \
  "$site/example_plugins-0.1.0.dist-info" \
  "$site/example_atproto_plugins-0.1.0.dist-info"
# cwd is on sys.path. /osprey itself contains the example plugin directories.
python - <<'PY'
import importlib
import osprey.rpc
import osprey.worker
for name in ("example_plugins", "example_atproto_plugins", "atproto_plugin"):
    try:
        importlib.import_module(name)
    except ModuleNotFoundError:
        continue
    raise SystemExit(name + " is still importable")
PY
python /osprey/osprey_worker/src/osprey/worker/cli/sinks.py run-rules-sink --no-pooled \
  > /tmp/worker.log 2>&1 || {
  cat /tmp/worker.log
  exit 1
}
python /velocity/check.py /tmp/worker.log
cat /tmp/worker.log
