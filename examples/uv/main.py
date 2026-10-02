"""Log project-scoped events in a Feed session."""

from __future__ import annotations

import feed


def main() -> None:
    with feed.init() as client:
        client.set_state("sensor", "room_1")
        client.log(
            "devices",
            {
                "config": {
                    "sample_interval_seconds": 1.0,
                    "units": {"temperature": "celsius"},
                },
            },
        )
        for temperature in (21.0, 21.2, 21.1):
            client.log("readings", {"temperature": temperature})
        client.log("status", {"healthy": True})


if __name__ == "__main__":
    main()
