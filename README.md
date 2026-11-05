# osprey-velocity-counter

A velocity counter for Osprey. When an event arrives it records one tick per entity. A rule later asks how many ticks an entity has inside a window.

This repository holds the TLA+ model of that counter. Two events share one user, one IP, and one API key. Each event is a human visit or an API call. The worker writes one key per step and can crash between keys. A later delivery writes the missing key and leaves the finished key alone. An API call is recorded under `api-client` and the IP key, and the user key stays empty of that event. A count is how many stored timestamps sit inside the requested window. Cleanup deletes only timestamps the longest window can no longer see.

Run the model checker:

```bash
./scripts/check.sh
```

Java 11 or newer is required. The script downloads the TLA+ tools on first run.

`osprey_velocity/counter.lua` is that counter for one Redis key. The caller chooses the key. `apply` stores the event id at its own timestamp and leaves an existing score alone. `count` returns how many stored timestamps sit inside `now - window < timestamp <= now`. `cleanup` deletes timestamps at or before `now - retention`.

```bash
python3 tests/test_counter.py
```

`redis-server` must be on `PATH`. The test starts its own server and stops it.

The plugin chooses the keys and calls that script. Config is `VELOCITY_COUNTERS`, a comma-separated list of `json.path:entity_type:behavior:1` where `1` means an API call rewrites that field to `api-client`. A missing field is skipped. A missing traffic-source stamp counts as a human visitor. `RecordingStream` records each event before the rules see it. `VelocityCount` only reads. A Redis error on that read returns 0 and logs an error, so the rule does not fire blind. A Redis error on the write is logged and raised, so the delivery can retry the unfinished key.

```bash
python3 tests/test_plugin.py
```

`RecordingStream` does not replace Osprey's Kafka source. Wrap the stream your own input hook already returns. The demo is separate: it sets `OSPREY_INPUT_STREAM_SOURCE=plugin` and `VELOCITY_EVENTS_FILE`, so this package reads two events from a file.

```bash
./demo/up.sh
```

Docker is required. The script clones Osprey into `~/.cache/osprey-velocity-demo` and builds the worker image.

A rule fire copies one key's sorted set into Postgres and places a hold on the entity. Later ticks stay out of that copy. Cleanup leaves the held entity's Redis ticks in place. An analyst can place a hold with no snapshot. `clear` keeps the rows for `VELOCITY_HOLD_GRACE` seconds. `confirm` keeps them for `VELOCITY_SNAPSHOT_RETENTION` seconds. After that the rows are marked purged and a deletion log records who destroyed them. There is no default for either duration.

```bash
python3 tests/test_audit.py
```

That test needs Docker and `psycopg`. It starts its own Postgres and Redis and stops them.
