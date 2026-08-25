"""Command-line entry point for ``python -m nanoq``."""

import argparse
import logging

from .broker import Broker


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the NanoQ message queue broker")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--db", default="db/nanoq.db")
    parser.add_argument("--visibility-timeout", type=float, default=30.0)
    arguments = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    broker = Broker(
        arguments.host,
        arguments.port,
        arguments.db,
        arguments.visibility_timeout,
    )
    host, port = broker.address
    logging.info("NanoQ listening on %s:%s (database: %s)", host, port, arguments.db)
    try:
        broker.serve_forever()
    except KeyboardInterrupt:
        logging.info("NanoQ stopping")
    finally:
        broker.close()


if __name__ == "__main__":
    main()
