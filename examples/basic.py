"""Emit typed events with shared session state."""

import feed


def main() -> None:
    with feed.init() as client:
        client.set_state("sensor", "room_1")
        for temperature in (21.0, 21.2, 21.1):
            client.emit(
                "readings",
                feed.EventBuilder().add_float("temperature", temperature).build(),
            )
        client.log("status", {"healthy": True})


if __name__ == "__main__":
    main()
