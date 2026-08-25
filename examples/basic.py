"""Minimal project-scoped logging example."""

import feed


def main() -> None:
    with feed.init(name="basic-example") as run:
        print("run_id =", run.id)
        for step, loss in enumerate((1.0, 0.72, 0.51)):
            run.log("train", {"step": step, "loss": loss})
        run.log("evaluation", {"split": "held_out", "accuracy": 0.83})

    print("finished and flushed")


if __name__ == "__main__":
    main()
