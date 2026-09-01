# Feed with UV

This example installs the repository checkout as an editable UV dependency,
signs in to an Analyze deployment, and sends a small run.

From the `feed-python` repository root, create the example environment and
sign in:

```sh
cd examples/uv
uv sync
```

Replace the example URL with the deployment you want to use. Sign in once and
list the feeds where your account has logging permission:

```sh
uv run feed login https://analyze.example.com
uv run feed list
```

If several feeds are listed, select a default once, then run the example:

```sh
uv run feed use "Your project/training"
uv run python main.py
```

The context manager flushes the run metadata and the `train`, `validation`,
and `held_out` streams before the process exits. Feed keeps the
login under `~/.config/feed`, outside the UV environment, so later runs using
the same home directory reuse it.
