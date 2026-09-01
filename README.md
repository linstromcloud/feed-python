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

`log` enqueues without waiting for HTTP and returns whether the data was
accepted locally. A background worker batches and uploads records based on size
and time thresholds.

Transient network failures and server errors retry with jittered exponential
backoff. Oversized batches are split automatically. Queues and retries are held
in memory, so the context manager—or an explicit `finish()`—is important before
the process exits:

```python
report = run.finish(timeout=30)
if not report.successful:
    raise RuntimeError(
        f"delivery incomplete: dropped={report.dropped}, pending={report.pending}"
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

Concurrent processes may use the same cached login. Refresh-token rotation is
protected by a file lock. Set `FEED_CREDENTIALS_FILE` if each process needs a
different credential location.

Schema and field names are lowercased. Changing an event's columns or their
types creates a new physical schema version.

## License

MIT. See [`LICENSE`](LICENSE).
