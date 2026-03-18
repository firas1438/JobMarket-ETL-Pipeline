from __future__ import annotations

import argparse

from app.pipeline.run_batch import main as batch_main
from app.streaming.consumer import main as consumer_main
from app.streaming.producer import main as producer_main


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Job Market Intelligence Pipeline entrypoint")
    p.add_argument(
        "command",
        choices=["batch", "producer", "consumer"],
        help="Which component to run",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    if args.command == "batch":
        return batch_main()
    if args.command == "producer":
        return producer_main()
    if args.command == "consumer":
        return consumer_main()
    raise RuntimeError(f"Unknown command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())

