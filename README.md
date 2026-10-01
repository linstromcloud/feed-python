# Feed for Python

Feed telemetry client for Python 3.9 and newer. Supports Windows, macOS, and Linux.

## How your calls become tables

- Feed stores events as rows, grouped by stream name and schema.
- The name passed to `emit` or `log` identifies the stream and becomes its logical table name. Event fields, current state fields, and system fields become columns.
- `schema_hash` hashes the stream name and the names and wire types of its event and state fields.

`set_state` and `remove_state` write and remove fields attached to every event until overwritten or removed.

Supported types: `bool`, `int64`, `float64`, `string`, homogeneous arrays, structs, and nullable scalars.

## Installation

```sh
uv add "feed-python @ git+https://github.com/linstromcloud/feed-python.git"
```

Or with pip:

```sh
python -m pip install "feed-python @ git+https://github.com/linstromcloud/feed-python.git"
```

## API usage

A feed is a logging destination inside a project. `feed.init()` starts a `Client` that owns state, channels, and background delivery. The context manager calls `finish()` on exit.

```python
import feed

with feed.init(
    ingest_url="https://ingest.example.com/v1/sensors",
    api_key="your-api-key",
) as client:
    client.set_state("sensor", "room_1")
    client.log("readings", {"temperature": 21.4, "humidity": 0.6})
```

Use the explicit `emit` form for more control over field types, including nullable fields:

```python
import feed

with feed.init(
    ingest_url="https://ingest.example.com/v1/sensors",
    api_key="your-api-key",
) as client:
    client.set_state("sensor", "room_1")
    fields = (
        feed.EventBuilder()
        .add_float("temperature", 21.4)
        .add_optional_float("humidity", 0.6)
        .build()
    )
    client.emit("readings", fields)
```

`ingest_url` identifies the feed. Environment defaults are `FEED_INGEST_URL` and `FEED_API_KEY`.

For direct endpoint configuration, use `feed.Client(feed.Config(...))`. See [configuration fields](src/feed/config.py), the [typed example](examples/basic.py), and the [UV example](examples/uv/README.md).

`EventBuilder.add` infers types. Typed methods include `add_bool`, `add_int`, `add_float`, `add_string`, their `add_*_array` forms, and `add_optional_*` scalar forms. `build()` returns the fields and clears the builder.

## Interactive login

Sign in, list the feeds your account can use, and select a default:

```sh
feed login https://api.example.com
feed list
feed use "Your project/telemetry"
```

With UV, prefix these commands with `uv run`. Create feeds in the project UI.

Login prints a browser URL and asks for a one-time code. Credentials are saved under `~/.config/feed/` and shared by processes using the same home directory.

Call `feed.init()` to use the selected feed and saved credentials. Pass `feed.init("Your project/another_feed")` or set `FEED` to select another feed.

## What goes in state vs. events

A field goes in `set_state` if both:

- You use it to filter or group events when querying the data.
- It applies to every event while that state is set.

Everything else goes in `emit`.

Common state values: `device_id`, `build_version`, `platform`, `region`, and `environment`.

Each event captures the current state snapshot. `set_state` infers typed structs for dictionaries and typed arrays for lists and tuples. Explicit setters include `set_state_float`, `set_state_string_array`, and `set_state_optional_int`. Use `has_state` to inspect a field and `remove_state` to remove it.

## Repeated values compress well

Parquet stores values by column and can encode repeated values with dictionaries and run lengths. Fields that stay constant across many events, such as `device_id` and `build_version`, take little additional space per row.

## Things to avoid

- **Avoid reusing field names between events and state.** Event fields override state fields with the same name.
- **Use ASCII letters, digits, and underscores for stream and field names.** Names are lowercased.
- **Avoid [system column](#system-columns) names.** Colliding user fields are exposed with a `data__` prefix.
- **Give standalone nulls and empty arrays explicit types.** Use `add_optional_float("value", None)` or `add_string_array("labels", [])`. Typed integer fields use signed 64-bit values; floats must be finite.

## Lifecycle and health

- `is_running`: ingestion is enabled and the worker has not finished.
- `worker_state`: `INITIALIZING`, `FETCHING_BLACKLIST`, `RUNNING`, or `FINISHED`.
- `emit` and `log` return `True` when an event enters the memory queue. Disabled or stopped clients, invalid channel handles, full queues, rate limits, and oversized events return `False`. Invalid names and values raise exceptions.
- `emit_wait` and `log_wait` wait for queue capacity up to the supplied timeout.
- `flush(timeout=10)` waits for accepted events. `finish(timeout=10)` also stops admission and shuts down the worker.

The worker saves events to disk before uploading. A crash can lose events still in memory. Check the delivery report when delivery matters:

```python
report = client.finish(timeout=30)
if not report.successful:
    raise RuntimeError(
        f"pending={report.persisted_pending}, "
        f"unsaved={report.unsaved}, failed={report.failed}"
    )
```

A successful report means every covered record was acknowledged or explicitly filtered. Lake ingestion happens downstream. See [delivery and recovery](docs/delivery.md) for limits, persistence, retries, and `feed status` / `feed sync`.

`feed.init(enabled=False)` needs no credentials and returns a client whose emit and log methods return `False`.

## Schema evolution and the hash

Adding or removing an event or state field, or changing its type, creates a new schema hash. Changing field values keeps the same hash.

The sink controls how versions appear in queries: separate schema views or union views of compatible schemas. Incompatible types use separate generations.

## System columns

Every row also gets system fields:

- `session_id`: UUID identifying the client instance.
- `session_sequence_num`: sequence number within the session and channel.
- `channel`: the channel used for emission.
- `schema_name`: the stream name passed to `emit` or `log`.
- `feed_id`: endpoint identifier in query views; exported data uses `game_id`.
- `server_timestamp`: when the server accepted the event.

## Channels (advanced)

Each channel has its own queue, sequence numbers, rate limit, and upload priority. Channels share the client's memory budget and spool quota. Lower numeric priorities upload first when capacity is contended.

```python
with feed.init(channels=[feed.ChannelSettings("alerts", priority=-1)]) as client:
    alerts = client.channel("alerts")
    fields = feed.EventBuilder().add_string("message", "hot").build()
    client.emit_on(alerts, "alarm", fields)
```

`emit` and `log` use `default`. Channel lookup is case-insensitive; unknown names resolve to `default`.

## Dictionary logging (optional)

`log` infers fields from a dictionary. Each top-level key becomes a separate column. Dictionary-valued fields become typed structs; arrays keep their inferred types.

```python
client.log("readings", {"temperature": 21.4, "details": {"unit": "celsius"}})
client.log({"healthy": True})  # Uses the stream named "log".
```

### Example: joining two tables

In this example, the application chooses two tables: `sessions` for configuration and `readings` for measurements.

```python
with feed.init() as client:
    client.log("sessions", {
        "config": {
            "sample_interval_seconds": 1.0,
            "thresholds": {"temperature": 25.0},
        },
    })
    for sample, temperature in enumerate((21.0, 21.2, 21.1)):
        client.log("readings", {"sample": sample, "temperature": temperature})
```

Join the records on their shared `session_id` in downstream analysis.

## Blacklisting (advanced)

Server-provided blacklist rules filter events before upload. A rule selects a `schema_hash` or `"*"` for all schemas; its optional `match` object requires matching field values on the event, including attached state.

The worker fetches rules before uploading and retries indefinitely if the fetch fails. Accepted events continue to persist locally. Matching events count as `filtered` in delivery reports.

## License

MIT. See [LICENSE](LICENSE).
