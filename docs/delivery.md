# Delivery and recovery

[Client overview](../README.md)

`emit` and `log` build the event, check its complete schema and data against the
size limit, then encode and admit it to memory without disk or HTTP I/O. `True`
means memory admission. The background worker persists records
before uploading them. Persistence runs when queued bytes reach 256 KiB or the
one-second interval expires; channel event thresholds and explicit flushes can
also trigger it. Network failures, authentication failures, and unavailable
filtering rules do not stop local persistence.

A crash can lose records still awaiting persistence. The interval is a target,
not a maximum loss window: slow or failed storage can extend it. The context
manager calls `finish()` to save queued records and wait for delivery before
the process exits. The default wait has no deadline. When work remains, the
client prints `[feed] Syncing remaining feeds, cancel with Ctrl+C.` to stdout.
Set a timeout to bound the wait:

```python
report = client.finish(timeout=30)
if not report.successful:
    raise RuntimeError(
        f"delivery incomplete: persisted={report.persisted_pending}, "
        f"unsaved={report.unsaved}, failed={report.failed}"
    )
```

Enabled clients print their session ID on initialization. Client messages use
the `[feed]` prefix. `finish()` prints a final status and delivered records, adding
filtered, failed, pending-on-disk, and unsaved counts when nonzero. These counts
include earlier completed `flush()` calls and exclude records recovered from
other sessions. Ctrl+C prints the available counts with a cancelled status.
Disabled clients remain quiet.

For producers that must apply backpressure, use `emit_wait` or `log_wait`, then
inspect `flush()` before advancing the source cursor:

```python
accepted = client.log_wait("export", {"records": 500}, timeout=30)
report = client.flush(timeout=30)
if not accepted or not report.successful:
    raise RuntimeError("Feed delivery is incomplete")
```

The defaults limit each channel queue to 1,024 records,
encoded queue data to 16 MiB per session and each encoded event,
including its schema, to 4 MiB. Oversized events return `False` with a warning;
`log_wait` rejects a completed event that cannot fit without waiting for queue
capacity. Queued records and records awaiting persistence share the memory
budget. Size is checked once after construction; type validation and event
construction can allocate memory before that check. Upload encoding/compression
and the spool's record index require additional working memory. These limits
do not establish an OS process-memory limit.

The shared spool defaults to 1 GiB at `~/.local/state/feed/spool/`, or under
`XDG_STATE_HOME` when set. `FEED_SPOOL_DIR` overrides its location. The quota
includes conservative file/metadata charges and outstanding reservations across
all sessions and projects. The worker reserves space for its pending writes and
releases unused credit. Idle sessions reserve only their bookkeeping space (32 KiB
per session), with no event allowance. A full spool leaves accepted records in the
bounded memory queue; further calls return `False` when that queue fills.
`log_wait` waits for queue space, and `finish` reports any remaining `unsaved`
records. Persistence preserves channel order. Existing records are never evicted
to admit new ones.

Persistence groups up to 500 records from one channel into an immutable batch
of at most 256 KiB; a larger individual record occupies its own batch. Shared
checkpoints store filtering decisions, retry identities, acknowledgements, and
failures. The quota reserves space for checkpoint replacements. A partially
acknowledged batch retains its payload until every record is acknowledged;
recovery skips the acknowledged records.

Configure these limits when starting a session:

```python
client = feed.init(
    channels=[feed.ChannelSettings("default", queue_capacity=65536)],
    max_spool_bytes=1024**3,
    memory_queue_bytes=64 * 1024**2,
    max_event_bytes=8 * 1024**2,
    persist_interval_seconds=1.0,
    persist_threshold_bytes=256 * 1024,
)
```

Size both the channel count and memory byte budget for a burst of records.
Increasing these limits buffers more input while persistence catches up;
non-blocking calls still return `False` when either limit is reached.

The event limit must be at most half the queue byte budget and at most 64 MiB.
An explicit `max_spool_bytes` updates the shared directory's budget under its
quota lock; it cannot lower the budget below stored and reserved usage. Omit it
to use the existing budget, or the 1 GiB default for a new directory.

Transient failures retry with capped, jittered backoff and honor `Retry-After`.
Waiting batches retain ticket lists in memory; their payloads stay on disk.
Retry timing resets after restart. Upload workers reuse their HTTP sessions.
Retry counts are unlimited. `max_retries`, `max_retry_queue_depth`, and
`max_blacklist_fetch_attempts` do not limit retries. Channel priorities, rate
limits, queue counts, upload thresholds, and per-channel/global upload slots
control delivery. Lower numeric channel priorities select waiting uploads first;
they cannot preempt an in-flight HTTP request.

An HTTP 413 splits a batch. An individually oversized or permanently rejected
upload remains in the spool with its error. Later eligible records can
upload while that record is failed, delayed, or in flight. Overall capacity
still bounds admission during an outage.

`flush()` waits for the records admitted before its call. `finish()` closes
admission, prioritizes persistence, and waits for pending uploads and matching
recovery sessions. An explicit timeout limits that wait. Ctrl+C cancels it and
propagates `KeyboardInterrupt`; a Ctrl+C inside the context also cancels exit
delivery. Persisted records remain recoverable. Permanently rejected records
remain failed and do not extend the wait.

If delivery stops before disk writes finish, `unsaved` reports the remaining
memory records when a report is returned. Cancellation can lose these records
on process exit. A stalled filesystem call may continue in the daemon worker.
Reports distinguish `delivered`, `filtered`, `failed`, `persisted_pending`, and
`unsaved`; `pending` includes both persisted and unsaved records. `dropped`
aliases `failed`. A successful report requires remote acknowledgement or
explicit filtering for every covered record. It does not
mean the data has reached the lake. Recovered sessions are separate from a new
client's delivery report.

Valid server counts settle the batch even when returned blacklist rules do not
explain all drops. The client warns and counts unattributed drops as `delivered`.
If the rules match more events than the server dropped, the client labels the
entire batch `delivered`.

## Recover saved records

While a session is active, delivery resumes when the endpoint becomes available.
A new `feed.init()` also recovers inactive spools for its selected deployment,
project, and feed. Cached member credentials supply stable IDs. API-key sessions
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
their records while the command continues with other sessions. The timeout applies
to each HTTP request, not the complete pass. A subsequent invocation can retry
remaining work. An empty or fully delivered pass exits zero; pending records,
failed records, active owners, and errors produce a nonzero exit status.

An active session keeps exclusive ownership. Sync requests a flush from its worker
and reports the session as active; it does not start a competing uploader. Both
commands accept `--spool-dir` and `--json`. Status describes files already on
disk; unpersisted memory records remain visible through the producing client's
delivery report.

Recovery preserves session IDs, channel sequences, encoded payloads, and wire
schemas. Uploads require a valid ingestion acknowledgement before deleting
saved records.
If the server accepts a request but its response is lost, recovery can send it
again with the same identity. Delivery is at least once; downstream identity
deduplication handles those repeated attempts.

Feed supports Windows, macOS, and Linux. Spools use immutable event batches,
atomic replacement, and synced file writes. They store destination metadata
and an API-key fingerprint, never access tokens, refresh tokens, or API keys.
Process coordination uses Windows byte-range locks or POSIX `flock`. A shared
filesystem must support these locks, atomic replacement, and file `fsync`;
validate these semantics on the cluster's actual shared filesystem.

Spool format 2 supports batched storage. Recovery also reads format 1. A shared
spool upgrades when no format 1 session owns it; active format 1 sessions keep
that root on format 1. Clients that only support format 1 reject an upgraded
root. Use separate spool directories when running those client versions together.

POSIX writes also sync parent directories. Windows supports recovery after a
process exits; directory changes are not explicitly synced, so recent creates,
replacements, and deletions can be lost after an OS crash or power failure.
Node-local temporary storage does not survive deletion of that storage.

POSIX files and directories use owner-only permissions. Windows uses inherited
filesystem ACLs; keep spool and credential paths in a user-private directory.

Concurrent processes may use the same cached login. Refresh-token rotation is
protected by a file lock. Set `FEED_CREDENTIALS_FILE` if each process needs a
different credential location.
