"""Railway entrypoint for the lease-based sync worker."""

import logging
import sys

from api.services.syncs.stream_worker import run_forever


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    stream=sys.stdout,
)


if __name__ == "__main__":
    run_forever()
