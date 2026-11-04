# osprey-velocity-counter

A velocity counter for Osprey. When an event arrives it records one tick per entity. A rule later asks how many ticks an entity has inside a window.

This repository holds the TLA+ model of that counter. Two events share one user, one IP, and one API key. Each event is a human visit or an API call. The worker writes one key per step and can crash between keys. A later delivery writes the missing key and leaves the finished key alone. An API call is recorded under `api-client` and the IP key, and the user key stays empty of that event. A count is how many stored timestamps sit inside the requested window. Cleanup deletes only timestamps the longest window can no longer see.

Run the model checker:

```bash
./scripts/check.sh
```

Java 11 or newer is required. The script downloads the TLA+ tools on first run.

`lua/counter.lua` is that counter for one Redis key. The caller chooses the key. `apply` stores the event id at its own timestamp and leaves an existing score alone. `count` returns how many stored timestamps sit inside `now - window < timestamp <= now`. `cleanup` deletes timestamps at or before `now - retention`.

```bash
python3 tests/test_counter.py
```

`redis-server` must be on `PATH`. The test starts its own server and stops it.

The plugin chooses the keys and calls that script. Config is `VELOCITY_COUNTERS`, a comma-separated list of `json.path:entity_type:behavior:1` where `1` means an API call rewrites that field to `api-client`. A missing field is skipped. A missing traffic-source stamp counts as a human visitor. `RecordingStream` records each event before the rules see it. `VelocityCount` only reads.

```bash
python3 tests/test_plugin.py
```

`RecordingStream` does not replace Osprey's Kafka source. Wrap the stream your own input hook already returns. The Docker demo wiring is not in this package yet.
