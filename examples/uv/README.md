# Feed with UV

This example installs the repository checkout as an editable UV dependency,
signs in, and logs events in one session.

From the `feed-python` repository root, create the example environment and
sign in:

```sh
cd examples/uv
uv sync
```

Replace the example URL with the deployment you want to use. Sign in once and
list the feeds where your account has logging permission:

```sh
uv run feed login https://api.example.com
uv run feed list
```

If several feeds are listed, select a default once, then run the example:

```sh
uv run feed use "Your project/telemetry"
uv run python main.py
```

The example logs device configuration to `devices`, temperatures to `readings`,
and a health flag to `status`. These records share a `session_id` and the `sensor`
state field.

The context manager flushes before the process exits. Feed keeps the
login under `~/.config/feed`, outside the UV environment, so later sessions using
the same home directory reuse it.
