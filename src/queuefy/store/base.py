from abc import ABC, abstractmethod
from datetime import datetime, timedelta

from queuefy.run import Run, RunStatus

# how many rows past the asked-for limit a claim looks at, so ten workers wanting one task do not all reach for the same row
CLAIM_SPREAD = 5

# how many expired leases one pass takes back: a whole cluster dying expires everything it held at once, and one statement over all of it is a transaction that holds the store while every other worker waits on it
RECLAIM_BATCH = 500

# the longest a worker may be called, which is what every store sizes the column it keeps that name in. a pod is named after its deployment and its namespace, and a name that does not fit is a worker whose every claim the database refuses
WORKER_NAME_LIMIT = 128

# the longest a task may be called, and the longest a key may be. a value past what the column holds is a write the database refuses — and on one that does not refuse it, a value quietly cut short, which is two occurrences of two different minutes becoming one run
TASK_NAME_LIMIT = 255
KEY_LIMIT = 255

# the longest a queue may be called
QUEUE_LIMIT = 64

# redis keeps one lane per priority a queue has seen and a claim walks all of them, so one drawn from a clock or an id is a lane a second for ever
PRIORITY_LIMIT = 1000

# the most attempts a run may be allowed, which is what an `Integer` column holds
# mysql and postgres both keep one in four bytes and refuse a count past it outright, while memory, sqlite and redis every one of them take it — so a task allowed more is one that works against every store a suite reaches and raises on every enqueue where the runs really live
ATTEMPT_LIMIT = 2**31 - 1

# a message is whatever the code being run put in it, so there is nowhere to refuse one and the worker cuts it to this instead
# sized for being the only record of what broke, because nothing logs a handler that raised
ERROR_LIMIT = 4096
ERROR_TYPE_LIMIT = 128

# what mysql keeps a whole number inside a json value as one, and what it quietly turns into a double either side of
# a payload of 10**40 is read back off mysql as 9.999999999999998e+39 while every other store reads back the number that was written, so the handler is called on a value nobody enqueued and nothing anywhere says so
WHOLE_FLOOR = -(2**63)
WHOLE_CEILING = 2**64 - 1


class Store(ABC):
    """where runs live. every method that changes a run is conditional on the state that run was in, which is what makes two workers safe without a lock anywhere. what holds a run is an attempt and never a name: a worker meets the lease it lost on its own next pass, claims the very same run again under the very same name, and from then on both of the attempts it is running answer to a condition written on the name alone"""

    @abstractmethod
    async def setup(self) -> None:
        """build whatever the store needs to hold runs, and do nothing when it is already there"""

    @abstractmethod
    async def add(self, run: Run) -> Run | None:
        """writes the run, and answers nothing when its key is already taken — which is how an occurrence stays single"""

    @abstractmethod
    async def claim(self, worker: str, queues: tuple[str, ...], limit: int, lease: timedelta, moment: datetime) -> list[Run]:
        """takes up to `limit` due runs for this worker, and never hands one to two of them"""

    @abstractmethod
    async def heartbeat(self, run_id, worker: str, attempt: int, lease: timedelta, moment: datetime) -> bool:
        """pushes the lease of the attempt this worker is running, and answers false once that attempt no longer holds the run"""

    @abstractmethod
    async def complete(self, run_id, worker: str, attempt: int, moment: datetime, result: dict | None) -> bool:
        """closes as done the run the attempt this worker is running holds"""

    @abstractmethod
    async def fail(self, run_id, worker: str, attempt: int, moment: datetime, error: str, error_type: str) -> bool:
        """closes as failed the run the attempt this worker is running holds, which is the end of it"""

    @abstractmethod
    async def retry_later(self, run_id, worker: str, attempt: int, due_at: datetime, error: str, error_type: str) -> bool:
        """puts the run this attempt holds back in the queue, due when the policy says, with the attempt it just spent standing"""

    @abstractmethod
    async def release(self, run_id, worker: str, attempt: int, due_at: datetime, error: str, error_type: str) -> bool:
        """puts the run this attempt holds back in the queue and gives the attempt back with it, for a worker that never had anything to try"""

    @abstractmethod
    async def reclaim(self, moment: datetime) -> int:
        """a worker that died holds a lease nobody will ever drop, so what ran out goes back to the queue or fails for good"""

    @abstractmethod
    async def cancel(self, run_id, moment: datetime) -> bool:
        """drops a run that has not started, and answers false for one already running or over"""

    @abstractmethod
    async def get(self, run_id) -> Run | None:
        """one run by id"""

    @abstractmethod
    async def find(self, key: str) -> Run | None:
        """one run by the key that made it unique"""

    @abstractmethod
    async def purge(self, before: datetime, limit: int) -> int:
        """drops settled runs that finished before an instant, up to `limit` of them, because a queue nobody prunes grows for ever"""

    @abstractmethod
    async def count(self, status: RunStatus | None = None, queue: str | None = None) -> int:
        """how deep the queue is, which is the one number an operator watches"""
