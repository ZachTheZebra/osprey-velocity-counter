# osprey-velocity-counter

A velocity counter for Osprey. When an event arrives it records one tick per entity. A rule later asks how many ticks an entity has inside a window.

This repository holds the TLA+ model of that counter. Two events share one user, one IP, and one API key. Each event is a human visit or an API call. The worker writes one key per step and can crash between keys. A later delivery writes the missing key and leaves the finished key alone. An API call is recorded under `api-client` and the IP key, and the user key stays empty of that event. A count is how many stored timestamps sit inside the requested window. Cleanup deletes only timestamps the longest window can no longer see.

Run the model checker:

```bash
./scripts/check.sh
```

Java 11 or newer is required. The script downloads the TLA+ tools on first run.
