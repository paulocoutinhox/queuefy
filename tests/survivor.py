"""a process that is killed on purpose, so the suite can ask what the next one finds"""

import asyncio
import sys
from datetime import timedelta
from pathlib import Path

from queuefy.app import Queuefy
from queuefy.worker import Worker
from tests.fleet import build_store

# short, so the run this process was holding when it was killed comes back while the test is still watching
LEASE = timedelta(seconds=1)


def build(url: str, output: str):
    store, closing = build_store(url)
    app = Queuefy(store)

    @app.task("slow", max_attempts=3, timeout=300)
    async def slow():
        Path(output).joinpath("started").write_text("started")

        await asyncio.sleep(300)

    @app.task("nightly", cron="0 1 * * *", max_attempts=3)
    async def nightly():
        Path(output).joinpath("nightly").write_text("ran")

    return app, closing


async def main(url: str, output: str, seconds: float) -> None:
    app, closing = build(url, output)
    worker = Worker(app, poll=0.02, lease=LEASE, grace=0.5)
    polling = asyncio.create_task(worker.run())

    try:
        await asyncio.sleep(seconds)
        worker.stop()
        await polling
    finally:
        await closing()


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1], sys.argv[2], float(sys.argv[3])))
