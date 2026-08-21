"""Minimal project-scoped logging example."""

import os

import feed


def main() -> None:
    project = os.environ.get("FEED_PROJECT")
    if not project:
        raise SystemExit("Set FEED_PROJECT to your organization/project")

    with feed.init(project=project, name="basic-example") as run:
        print("run_id =", run.id)
        for step, loss in enumerate((1.0, 0.72, 0.51)):
            run.log("train", {"step": step, "loss": loss})
        run.log("evaluation", {"split": "held_out", "accuracy": 0.83})

    print("finished and flushed")


if __name__ == "__main__":
    main()
