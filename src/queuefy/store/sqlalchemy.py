import asyncio
import logging
import random
from datetime import datetime, timedelta

from sqlalchemy import JSON, BigInteger, Column, DateTime, Double, Index, Integer, MetaData, String, Table, TypeDecorator, and_, delete, func, select, update
from sqlalchemy.dialects.mysql import DATETIME as MYSQL_DATETIME
from sqlalchemy.dialects.mysql import VARCHAR as MYSQL_VARCHAR
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from queuefy.clock import as_utc, naive_utc
from queuefy.retry import RetryPolicy
from queuefy.run import SETTLED, Run, RunStatus
from queuefy.store.base import CLAIM_SPREAD, ERROR_LIMIT, ERROR_TYPE_LIMIT, KEY_LIMIT, QUEUE_LIMIT, RECLAIM_BATCH, TASK_NAME_LIMIT, WORKER_NAME_LIMIT, Store

logger = logging.getLogger(__name__)

# the store keeps its own metadata, so building its table never touches a table of the application around it
metadata = MetaData()

# innodb answers contention by rolling one side back and asking for the transaction again, which is the handling mysql documents and not a workaround: 1213 is a deadlock and 1205 is a lock it waited too long for
CONTENDED = frozenset({1205, 1213})

TRIES = 8

# short to begin with, because the other side only needs to finish the statement it is already in — and doubling, because contention on a hot queue comes in bursts that outlast a schedule only ever adding milliseconds. measured against innodb with forty claims and a reclaim on the same rows: five tries growing by ten milliseconds left outcomes the store refused on every single run, and eight doubling ones left none
BACKOFF = 0.005

# and drawn, for the same reason a retry delay is drawn: every transaction innodb rolled back works the same wait out from the same numbers, so a schedule they all share brings the whole herd back in lockstep to deadlock on each other again and spend every try doing it
SPREAD = 1.0


def identifier(length: int) -> String:
    """a column that says which run this is, compared code point by code point wherever the runs live. mysql builds one under `utf8mb4_0900_ai_ci` unless it is told otherwise, which folds case and accents away before it compares — so 'invoice:Bob@shop.com' and 'invoice:bob@shop.com' were one occurrence behind the unique index and the second caller was handed the run of the first, a worker serving 'mail' claimed and ran what was written to 'Mail', and a name that only read like another worker's closed the run that worker was still holding. `utf8mb4_0900_bin` is the collation that compares the code points and pads nothing, which is what memory, sqlite, postgres and redis each already do"""
    return String(length).with_variant(MYSQL_VARCHAR(length, charset="utf8mb4", collation="utf8mb4_0900_bin"), "mysql")


def contended(error: DBAPIError) -> bool:
    codes = getattr(error.orig, "args", ())

    return bool(codes) and codes[0] in CONTENDED


async def under_contention(work):
    """runs a write again when the database asked for it, and lets anything else through untouched"""
    for attempt in range(TRIES - 1):
        try:
            return await work()
        except DBAPIError as error:
            if not contended(error):
                raise

            await asyncio.sleep(BACKOFF * (2**attempt) * (1 + random.uniform(0, SPREAD)))

    return await work()


class UtcDateTime(TypeDecorator):
    """the database holds naive utc and the application reads aware utc, because mysql keeps no offset and a store that guesses one runs everything an hour late"""

    impl = DateTime
    cache_ok = True

    def load_dialect_impl(self, dialect):
        """mysql **rounds** a datetime with no fractional precision, so a run due at 10:00:00.9 is stored due at 10:00:01 — a second in the future, where nothing claims it until the second turns"""
        if dialect.name == "mysql":
            return dialect.type_descriptor(MYSQL_DATETIME(fsp=6))

        return dialect.type_descriptor(DateTime())

    def process_bind_param(self, value, dialect):
        return naive_utc(value)

    def process_result_value(self, value, dialect):
        return as_utc(value)


runs = Table(
    "queuefy_run",
    metadata,
    Column("id", BigInteger().with_variant(Integer, "sqlite"), primary_key=True, autoincrement=True),
    Column("key", identifier(KEY_LIMIT), nullable=True, unique=True),
    Column("name", String(TASK_NAME_LIMIT), nullable=False),
    Column("queue", identifier(QUEUE_LIMIT), nullable=False, default="default"),
    Column("payload", JSON, nullable=False, default=dict),
    Column("status", String(16), nullable=False, default=RunStatus.PENDING.value),
    Column("priority", Integer, nullable=False, default=0),
    Column("due_at", UtcDateTime, nullable=False),
    Column("attempts", Integer, nullable=False, default=0),
    Column("max_attempts", Integer, nullable=False, default=1),
    # a python float is a double, and the generic float is a single-precision `FLOAT` on mysql: a timeout of 12345.678 came back 12345.7 and one of 3599.999999 came back 3600.0, where memory, sqlite, postgres and redis every one of them read back the number that was written. past what a single holds it is not read back changed but refused outright, which is the same call writing a run on four stores and raising on the fifth
    Column("timeout", Double, nullable=True),
    Column("retry_policy", String(32), nullable=False, default=RetryPolicy.EXPONENTIAL.value),
    Column("retry_delay", Double, nullable=False, default=5.0),
    Column("max_retry_delay", Double, nullable=False, default=3600.0),
    Column("worker", identifier(WORKER_NAME_LIMIT), nullable=True),
    Column("lease_until", UtcDateTime, nullable=True),
    Column("created_at", UtcDateTime, nullable=False),
    Column("started_at", UtcDateTime, nullable=True),
    Column("finished_at", UtcDateTime, nullable=True),
    Column("result", JSON, nullable=True),
    Column("error", String(ERROR_LIMIT), nullable=True),
    Column("error_type", String(ERROR_TYPE_LIMIT), nullable=True),
    # the one query a worker runs on every poll, answered by the index instead of by a scan of everything ever queued
    Index("queuefy_run_ready", "queue", "status", "priority", "due_at"),
    Index("queuefy_run_lease", "status", "lease_until"),
    # what the pruning asks for, so dropping a week of settled runs is not a scan of everything ever queued
    Index("queuefy_run_settled", "status", "finished_at"),
    # sqlite hands out the highest id it can see plus one, so pruning the newest settled run gives its id to the next run written. a caller holding an id from before the pruning then reads, and cancels, somebody else's run — which the sequences of postgres and the persisted counter of innodb never do
    sqlite_autoincrement=True,
)


def to_run(row) -> Run:
    return Run(
        id=row.id,
        key=row.key,
        name=row.name,
        queue=row.queue,
        payload=row.payload,
        status=RunStatus(row.status),
        priority=row.priority,
        due_at=row.due_at,
        attempts=row.attempts,
        max_attempts=row.max_attempts,
        timeout=row.timeout,
        retry_policy=RetryPolicy(row.retry_policy),
        retry_delay=row.retry_delay,
        max_retry_delay=row.max_retry_delay,
        worker=row.worker,
        lease_until=row.lease_until,
        created_at=row.created_at,
        started_at=row.started_at,
        finished_at=row.finished_at,
        result=row.result,
        error=row.error,
        error_type=row.error_type,
    )


def to_values(run: Run) -> dict:
    """every field of the run and not the ones a fresh one happens to fill, because a field the write leaves out is one this store reads back as nothing while memory and redis read back what they were given"""
    return {
        "key": run.key,
        "name": run.name,
        "queue": run.queue,
        "payload": run.payload,
        "status": run.status.value,
        "priority": run.priority,
        "due_at": run.due_at,
        "attempts": run.attempts,
        "max_attempts": run.max_attempts,
        "timeout": run.timeout,
        "retry_policy": run.retry_policy.value,
        "retry_delay": run.retry_delay,
        "max_retry_delay": run.max_retry_delay,
        "worker": run.worker,
        "lease_until": run.lease_until,
        "created_at": run.created_at,
        "started_at": run.started_at,
        "finished_at": run.finished_at,
        "result": run.result,
        "error": run.error,
        "error_type": run.error_type,
    }


class SqlAlchemyStore(Store):
    """runs held in the database the application already has, which is what lets ten workers on ten machines agree without any of them talking to the others"""

    def __init__(self, engine: AsyncEngine) -> None:
        self.engine = engine
        self.sessions = async_sessionmaker(bind=engine, expire_on_commit=False)

    async def setup(self) -> None:
        """ten replicas boot together and `create_all` asks whether the table is there and then creates it, which is a question and a statement with a gap between them — the losers are told the table already exists and the process ends before it ever polls"""
        try:
            async with self.engine.begin() as connection:
                await connection.run_sync(metadata.create_all)
        except DBAPIError:
            # asking again is what settles it, and nothing here reads an error message: with the table there this does nothing, and with it still missing this raises for whatever the real reason was
            async with self.engine.begin() as connection:
                await connection.run_sync(metadata.create_all)

    async def add(self, run: Run) -> Run | None:
        """the one write every worker aims at the same key at the same instant, and innodb answers a duplicate two transactions race for with a deadlock as often as with a duplicate-key error"""
        return await under_contention(lambda: self.insert(run))

    async def insert(self, run: Run) -> Run | None:
        async with self.sessions() as session:
            try:
                written = await session.execute(runs.insert().values(**to_values(run)))
                await session.commit()
            except IntegrityError:
                await session.rollback()

                # the key is taken, which is exactly what a second worker writing the same occurrence is supposed to hit. a run with no key never raced anybody for one, so what the database refused there is a real failure and swallowing it hands the caller somebody else's run
                if run.key is None:
                    raise

                return None

            run.id = written.inserted_primary_key[0]

            return run

    async def claim(self, worker: str, queues: tuple[str, ...], limit: int, lease: timedelta, moment: datetime) -> list[Run]:
        """the candidates are read without a lock and each one is taken with a conditional write, so the database decides the winner and nothing is held between the two"""
        async with self.sessions() as session:
            ready = select(runs.c.id).where(runs.c.queue.in_(queues), runs.c.status == RunStatus.PENDING.value, runs.c.due_at <= moment).order_by(runs.c.priority.desc(), runs.c.due_at.asc(), runs.c.id.asc()).limit(limit * CLAIM_SPREAD)
            candidates = list((await session.execute(ready)).scalars())
            taken = []

            for run_id in candidates:
                if len(taken) == limit:
                    break

                try:
                    claimed = await self.take(session, run_id, worker, lease, moment)
                except Exception:
                    # what this batch has already won belongs to this worker, and it is alive and holding it: every one of those runs has its attempt spent and its lease running, so a batch that broke halfway through must not take them with it. left to raise, what meets them next is the reclaim their leases run out into — and with `max_attempts` at one that fails them outright under `LeaseExpired`, their handler never called and nothing anywhere saying so
                    # every failure and not only one the driver raised: a row is taken and committed one at a time, so the connection goes back to the pool between two of them, and a pool with nothing left answers the next one with a `TimeoutError` sqlalchemy raises itself. that is an ordinary batch under ordinary load losing every run it had already won, with nothing having gone wrong anywhere
                    if not taken:
                        raise

                    logger.exception("[queuefy] %s could not go on claiming, and answers with the %s runs it already holds", worker, len(taken))

                    break

                if claimed is not None:
                    taken.append(claimed)

            return taken

    async def take(self, session, run_id, worker: str, lease: timedelta, moment: datetime) -> Run | None:
        """one row, taken with a write conditional on the state it was in, so the database is what decides the winner — and read back inside the very transaction that took it, so a row this worker cannot read is one it never took. committed first and read afterwards, the read is a second transaction with the world between them: a connection that went away there, or a pool with nothing left to hand it one, left the run claimed with its attempt spent and handed to nobody, and what met it next was the reclaim its lease ran out into — which with `max_attempts` at one failed it under `LeaseExpired`, its handler never called and nothing anywhere saying so"""

        async def once() -> Run | None:
            mine = update(runs).where(runs.c.id == run_id, runs.c.status == RunStatus.PENDING.value, runs.c.due_at <= moment).values(status=RunStatus.RUNNING.value, worker=worker, attempts=runs.c.attempts + 1, started_at=moment, lease_until=moment + lease)
            claimed = None

            try:
                if (await session.execute(mine)).rowcount == 1:
                    claimed = to_run((await session.execute(select(runs).where(runs.c.id == run_id))).one())

                await session.commit()
            except DBAPIError:
                # the rollback is what leaves this session usable for the candidate after it
                await session.rollback()

                raise

            return claimed

        return await under_contention(once)

    def holding(self, run_id, worker: str, attempt: int):
        """the attempt is what names the holder, and the worker alone never could: a claim mints a number nobody else is on, so a worker that met its own expired lease and took the run back is asking under the attempt it is now running and not the one it lost"""
        return and_(runs.c.id == run_id, runs.c.worker == worker, runs.c.attempts == attempt, runs.c.status == RunStatus.RUNNING.value)

    async def write(self, condition, values: dict) -> bool:
        async def once() -> bool:
            async with self.sessions() as session:
                changed = (await session.execute(update(runs).where(condition).values(**values))).rowcount == 1
                await session.commit()

                return changed

        return await under_contention(once)

    async def heartbeat(self, run_id, worker: str, attempt: int, lease: timedelta, moment: datetime) -> bool:
        return await self.write(self.holding(run_id, worker, attempt), {"lease_until": moment + lease})

    async def complete(self, run_id, worker: str, attempt: int, moment: datetime, result: dict | None) -> bool:
        return await self.write(self.holding(run_id, worker, attempt), {"status": RunStatus.DONE.value, "finished_at": moment, "result": result, "error": None, "error_type": None, "worker": None, "lease_until": None})

    async def fail(self, run_id, worker: str, attempt: int, moment: datetime, error: str, error_type: str) -> bool:
        return await self.write(self.holding(run_id, worker, attempt), {"status": RunStatus.FAILED.value, "finished_at": moment, "error": error, "error_type": error_type, "worker": None, "lease_until": None})

    def requeued(self, due_at: datetime, error: str, error_type: str) -> dict:
        return {"status": RunStatus.PENDING.value, "due_at": due_at, "error": error, "error_type": error_type, "worker": None, "lease_until": None, "started_at": None}

    async def retry_later(self, run_id, worker: str, attempt: int, due_at: datetime, error: str, error_type: str) -> bool:
        return await self.write(self.holding(run_id, worker, attempt), self.requeued(due_at, error, error_type))

    async def release(self, run_id, worker: str, attempt: int, due_at: datetime, error: str, error_type: str) -> bool:
        return await self.write(self.holding(run_id, worker, attempt), self.requeued(due_at, error, error_type) | {"attempts": runs.c.attempts - 1})

    async def reclaim(self, moment: datetime) -> int:
        """what a dead worker left behind: the run goes back to the queue, unless its attempts are spent and there is nothing left to try"""
        return await under_contention(lambda: self.sweep(moment))

    async def sweep(self, moment: datetime) -> int:
        expired = and_(runs.c.status == RunStatus.RUNNING.value, runs.c.lease_until.is_not(None), runs.c.lease_until < moment)
        released = {"worker": None, "lease_until": None, "started_at": None}

        async with self.sessions() as session:
            # the two statements share the batch, because what the constant bounds is how long one pass holds the store and not how much either half of it writes
            gave_up = await self.take_back(session, and_(expired, runs.c.attempts >= runs.c.max_attempts), RECLAIM_BATCH, {"status": RunStatus.FAILED.value, "finished_at": moment, "error": "the worker holding this run stopped answering", "error_type": "LeaseExpired", **released})
            requeued = await self.take_back(session, and_(expired, runs.c.attempts < runs.c.max_attempts), RECLAIM_BATCH - gave_up, {"status": RunStatus.PENDING.value, "due_at": moment, **released})

            await session.commit()

            return gave_up + requeued

    async def take_back(self, session, expired, size: int, values: dict) -> int:
        """the batch is read first and then written, so the state those rows were named for is asserted again by the update itself. without it, a row this statement waited on a lock for is written whatever it has since become — and a lease another worker took over in that moment is taken straight back out from under it"""
        taken = await self.batch(session, expired, size)

        if not taken:
            return 0

        return (await session.execute(update(runs).where(runs.c.id.in_(taken), expired).values(**values))).rowcount

    async def batch(self, session, condition, size: int) -> list:
        """the rows one statement is allowed to touch, read without a lock and ordered by id so the write that follows names them by primary key. naming them inside the write let the database drive it off whichever index the condition reads by, locking a secondary entry and then reaching for the row — while every close of a run locks the row and then reaches for that same entry. two orders around one row is a deadlock, and the one innodb rolls back is the close"""
        return list((await session.execute(select(runs.c.id).where(condition).order_by(runs.c.id).limit(size))).scalars())

    async def purge(self, before: datetime, limit: int) -> int:
        """a settled run is still being indexed, counted and backed up, and nothing ever reads it again"""

        async def once() -> int:
            async with self.sessions() as session:
                over = and_(runs.c.status.in_([status.value for status in SETTLED]), runs.c.finished_at.is_not(None), runs.c.finished_at < before)
                taken = await self.batch(session, over, limit)

                if not taken:
                    return 0

                gone = (await session.execute(delete(runs).where(runs.c.id.in_(taken), over))).rowcount
                await session.commit()

                return gone

        return await under_contention(once)

    async def cancel(self, run_id, moment: datetime) -> bool:
        return await self.write(and_(runs.c.id == run_id, runs.c.status == RunStatus.PENDING.value), {"status": RunStatus.CANCELED.value, "finished_at": moment})

    async def one(self, condition) -> Run | None:
        async with self.sessions() as session:
            row = (await session.execute(select(runs).where(condition))).one_or_none()

            return to_run(row) if row is not None else None

    async def get(self, run_id) -> Run | None:
        return await self.one(runs.c.id == run_id)

    async def find(self, key: str) -> Run | None:
        return await self.one(runs.c.key == key)

    async def count(self, status: RunStatus | None = None, queue: str | None = None) -> int:
        wanted = []

        if status is not None:
            wanted.append(runs.c.status == status.value)

        if queue is not None:
            wanted.append(runs.c.queue == queue)

        async with self.sessions() as session:
            return await session.scalar(select(func.count()).select_from(runs).where(*wanted))
