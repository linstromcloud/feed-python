"""Send a small project-scoped run through Feed."""

from __future__ import annotations

import os

import feed


def required_environment(name: str, example: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise SystemExit(f"Set {name}, for example: export {name}={example}")
    return value


def main() -> None:
    project = required_environment("FEED_PROJECT", "your-organization/your-project")

    with feed.init(
        project=project,
        name="uv-smoke-test",
        config={"model": {"width": 64, "blocks": [2, 2]}, "lr": 1e-3},
        tags=["uv", "smoke-test"],
    ) as run:
        print("run_id =", run.id)
        run.log("train", {"step": 0, "loss": 1.0, "accuracy": 0.25})
        run.log("train", {"step": 1, "loss": 0.5, "accuracy": 0.75})

        run.log(
            "validation",
            {
                "step": 1,
                "checkpoint": "step-1",
                "validation_loss": 0.6,
                "accuracy": 0.7,
            },
        )
        run.log("held_out", {"accuracy": 0.8, "f1": 0.76})

    print("finished and flushed")


if __name__ == "__main__":
    main()
