# Feed for Python

Log measurements and structured events from Python. A feed is a shared logging
destination inside a project. Feed handles authentication, batching, retries,
and delivery in a background thread; application code stays synchronous.

## Get started

```sh
uv add "feed @ git+https://github.com/linstromcloud/feed-python.git"
```

Sign in to an Analyze deployment and list the feeds you can use:

```sh
uv run feed login https://analyze.example.com
uv run feed list
```

`feed login` prints a browser URL and asks for a one-time code. On a cluster,
open the URL on any machine and paste the code into the login-node terminal.
Your credentials are saved under `~/.config/feed/` and safely shared by jobs
using the same home directory.

`feed list` prints copyable `project/feed` references and their current status.
If one feed is available, login selects it automatically. Otherwise, choose a
default once:

```sh
uv run feed use "Diffusion study/training"
```

Create feeds in the Analyze project UI. Every project member with logging
permission sees the same feeds after running `feed list`.

Then log a run without repeating the selected feed:

```python
import feed

with feed.init(
    name="width-256-seed-7",
    config={"width": 256, "seed": 7, "optimizer": {"lr": 3e-4}},
    tags=["ablation"],
) as run:
    for step in range(1_000):
        run.log("train", {
            "step": step,
            "loss": loss,
            "accuracy": accuracy,
        })

    run.log("validation", {
        "step": 999,
        "loss": validation_loss,
        "accuracy": validation_accuracy,
    })
```

The context manager flushes before the process exits. `run.id` is the UUID that
links every row produced by that run.

Pass the reference directly to override the saved default for one process:

```python
with feed.init("Diffusion study/evaluation", name="held-out") as run:
    run.log("metrics", {"dataset": "test", "accuracy": 0.94})
```

The `FEED` environment variable provides the same override. If several feeds
are available and no default is selected, `feed.init()` lists the choices
instead of guessing a destination.

For a complete UV environment and runnable example, see
[`examples/uv`](examples/uv/README.md).

## The run API

The high-level API has four concepts:

- **Project** — the authorization boundary that contains one or more feeds.
- **Feed** — a shared logging destination selected as `project/feed`.
- **Run** — one process or logical unit of work, with optional name, config,
  tags, and group.
- **Stream name** — a named collection whose name becomes its logical query
  table.
- **Record** — one native typed row appended with `log`.

Feed assigns no special meaning to fields such as `step`, and never increments
them implicitly. Put whatever coordinates and values belong to one observation
in the record:

```python
run.log("benchmark", {"iteration": 20, "throughput": 412.8})

run.log("simulation", {
    "replicate": 3,
    "elapsed_seconds": 0.0,
    "temperature": 21.4,
    "pressure": 100.8,
})
```

The default stream is `log`:

```python
run.log({"elapsed_seconds": 10.0, "objective": 0.42})
```

Pass a stream name when the record belongs to a named collection:

```python
run.log("validation", {"step": 999, "loss": 0.41})
```

Records can contain nested values without switching APIs:

```python
run.log(
    "attention_diagnostics",
    {
        "layer": 8,
        "matrix": [[0.1, 0.2], [0.3, 0.4]],
        "summary": {"mean": 0.25, "labels": ["a", "b"]},
    },
)
```

The first argument becomes the wire schema name and, ultimately, the logical
table name. Feed supports booleans, integers, floats, strings, homogeneous
arrays, nested dictionaries, and homogeneous nested lists. Run configuration
uses Feed's dynamic `variant` type, so different runs may use different nested
config shapes without splitting the run schema.

Names are case-insensitive and must contain only ASCII letters, digits, and
underscores. Integers must fit in signed 64 bits, floats must be finite, and
arrays must contain one consistent type. Feed rejects ambiguous values such as
`None` and empty arrays because their wire type cannot be inferred. Convert
library-specific scalar objects, such as NumPy or PyTorch scalars, to ordinary
Python values (for example with `.item()`) before logging them.

To turn logging off without changing application control flow, pass
`enabled=False`. This needs no login or endpoint, starts no background thread,
and makes `log()` and `log_wait()` return `False` without inspecting the record.
`flush()` and `finish()` return a successful empty delivery report.

## Delivery behavior

`log` builds the event, checks its complete schema and data against the size
limit, then encodes and admits it to memory without disk or HTTP I/O. `True`
means memory admission. The background worker persists records
before uploading them. Persistence runs when queued bytes reach 256 KiB or the
one-second interval expires; channel event thresholds and explicit flushes can
also trigger it. Network failures, authentication failures, and unavailable
filtering rules do not stop local persistence.

A crash can lose records still awaiting persistence. The interval is a target,
not a maximum loss window: slow or failed storage can extend it. The context
manager calls `finish()` to save queued records and wait for delivery before
the process exits:

```python
report = run.finish(timeout=30)
if not report.successful:
    raise RuntimeError(
        f"delivery incomplete: persisted={report.persisted_pending}, "
        f"unsaved={report.unsaved}, failed={report.failed}"
    )
```

For producers that must apply backpressure, use `log_wait`, then inspect
`flush()` before advancing the source cursor:

```python
accepted = run.log_wait("export", {"records": 500}, timeout=30)
report = run.flush(timeout=30)
if not accepted or not report.successful:
    raise RuntimeError("Feed delivery is incomplete")
```

The defaults bound encoded queue data to 16 MiB per run and each encoded event,
including its schema, to 4 MiB. Oversized events return `False` with a warning;
`log_wait` rejects a completed event that cannot fit without waiting for queue
capacity. Queued records and records awaiting persistence share the memory
budget. Size is checked once after construction; type validation and event
construction can allocate memory before that check. Upload encoding/compression
and the spool's filename index require additional working memory. These limits
do not establish an OS process-memory limit.

The shared spool defaults to 1 GiB at `~/.local/state/feed/spool/`, or under
`XDG_STATE_HOME` when set. `FEED_SPOOL_DIR` overrides its location. The quota
includes conservative file/metadata charges and outstanding reservations across
all runs and projects. The worker reserves space for its pending writes and
releases unused credit. Idle runs reserve only their bookkeeping space (32 KiB
per run), with no event allowance. A full spool leaves accepted records in the
bounded memory queue; further calls return `False` when that queue fills.
`log_wait` waits for queue space, and `finish` reports any remaining `unsaved`
records. Persistence preserves channel order. Existing records are never evicted
to admit new ones.

Configure these limits when starting a run:

```python
run = feed.init(
    max_spool_bytes=1024**3,
    memory_queue_bytes=32 * 1024**2,
    max_event_bytes=8 * 1024**2,
    persist_interval_seconds=1.0,
    persist_threshold_bytes=256 * 1024,
)
```

The event limit must be at most half the queue byte budget and at most 64 MiB.
An explicit `max_spool_bytes` updates the shared directory's budget under its
quota lock; it cannot lower the budget below stored and reserved usage. Omit it
to use the existing budget, or the 1 GiB default for a new directory.

Transient failures retry with capped, jittered backoff and honor `Retry-After`.
Waiting batches retain ticket lists in memory; their payloads stay on disk.
Retry timing resets after restart. Upload workers reuse their HTTP sessions.
The legacy `max_retries`, `max_retry_queue_depth`, and
`max_blacklist_fetch_attempts` options remain accepted; they no longer discard
durable records or disable recording. Channel priorities, rate limits, queue
counts, upload thresholds, and per-channel/global upload slots still apply.
The run API gives metadata priority over data. Priority selects waiting uploads;
it cannot preempt an in-flight HTTP request.

An HTTP 413 splits a batch. An individually oversized or permanently rejected
upload remains in a `.failed` file with its error. Later eligible metrics can
upload while that record is failed, delayed, or in flight. Overall capacity
still bounds admission during an outage.

`flush()` waits for the records admitted before its call. `finish()` closes
admission, prioritizes persistence, and waits for uploads within its deadline.
If disk writes cannot finish within that time, `unsaved` reports the remaining
memory records. A stalled filesystem call may continue in the daemon worker.
Reports distinguish `delivered`, `filtered`, `failed`, `persisted_pending`, and
`unsaved`; `pending` includes both persisted and unsaved records. `dropped` is
the legacy name for `failed`. A successful report requires remote
acknowledgement or explicit filtering for every covered record. It does not
mean the data has reached the lake. Recovered runs are separate from a new
run's delivery report.

### Recover saved records

While a run is active, delivery resumes when the endpoint becomes available.
A new `feed.init()` also recovers inactive spools for its selected deployment,
project, and feed. Cached member credentials supply stable IDs. API-key runs
match the original URL, feed reference, endpoint, and key fingerprint. Other
projects remain untouched.

After a job exits, inspect and synchronize every saved destination:

```sh
feed status
feed sync
feed sync --timeout 10 --json
```

`feed sync` resolves each destination using the current login or the matching
`FEED_API_KEY`. It attempts each outstanding record once, including previously
failed records, and splits oversized batches. Unavailable destinations retain
their records while the command continues with other runs. The timeout applies
to each HTTP request, not the complete pass. A subsequent invocation can retry
remaining work. An empty or fully delivered pass exits zero; pending records,
failed records, active owners, and errors produce a nonzero exit status.

An active run keeps exclusive ownership. Sync requests a flush from its worker
and reports the run as active; it does not start a competing uploader. Both
commands accept `--spool-dir` and `--json`. Status describes files already on
disk; unpersisted memory records remain visible through the producing run's
delivery report.

Recovery preserves session IDs, channel sequences, captured data, and schemas.
Uploads require a valid ingestion acknowledgement before deleting saved records.
If the server accepts a request but its response is lost, recovery can send it
again with the same identity. Delivery is at least once; downstream identity
deduplication handles those repeated attempts.

Spools use private directories and files, immutable event files, atomic rename,
and synced writes. They store destination metadata and an API-key fingerprint,
never access tokens, refresh tokens, or API keys. A shared filesystem must
provide working POSIX `flock`, atomic rename, and `fsync` semantics. The process
and quota tests run on local POSIX storage; validate these semantics on the
cluster's actual shared filesystem. Node-local temporary storage does not
survive deletion of that storage.

Concurrent processes may use the same cached login. Refresh-token rotation is
protected by a file lock. Set `FEED_CREDENTIALS_FILE` if each process needs a
different credential location.

Schema and field names are lowercased. Changing an event's columns or their
types creates a new physical schema version.

## License

MIT. See [`LICENSE`](LICENSE).
