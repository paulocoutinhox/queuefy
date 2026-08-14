"""what a line by line reading found, each one pinned by the test that would have caught it"""

import asyncio
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from datetime import date, datetime, timedelta, timezone

import pytest
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.exc import TimeoutError as PoolTimeout
from sqlalchemy.ext.asyncio import create_async_engine

from queuefy.app import REWRITES, occurrence_key
from queuefy.clock import EPOCH, WIDEST_SECONDS, WIDEST_SLOT, now
from queuefy.errors import CronError, PermanentError, QueueError
from queuefy.retry import RetryPolicy, delay_for
from queuefy.run import Run, RunStatus
from queuefy.store.base import ATTEMPT_LIMIT, ERROR_LIMIT, ERROR_TYPE_LIMIT, KEY_LIMIT, PRIORITY_LIMIT, QUEUE_LIMIT, RECLAIM_BATCH, TASK_NAME_LIMIT, WORKER_NAME_LIMIT
from queuefy.task import Task
from queuefy.trigger import Cron, Interval
from queuefy.worker import PURGE_LIMIT, Worker, described, worker_name

from .conftest import REDIS_URL, wait_until


class Greeter:
    """a callable object with an async call, which `iscoroutinefunction` says nothing about"""

    def __init__(self) -> None:
        self.seen = []

    async def __call__(self, who: str) -> None:
        self.seen.append(who)


def wrapped(handler):
    """the shape every decorator that forgets `async` has: a plain function answering a coroutine"""

    def wrapper(**payload):
        return handler(**payload)

    return wrapper


class Unspeakable(RuntimeError):
    """a failure that cannot say what it was, which is any exception whose message is built out of an object with a broken `__str__`"""

    def __str__(self) -> str:
        raise RuntimeError("this message cannot be read")


async def test_a_handler_that_only_looks_synchronous_is_still_run(app):
    """awaiting the thread alone would close the run as done with the work never started, and nothing anywhere would say so"""
    greeter = Greeter()
    app.register(Task(name="greet", handler=greeter))

    written = await app.enqueue("greet", who="paulo")

    worker = Worker(app)
    await worker.run_once()
    await worker.drain()

    assert greeter.seen == ["paulo"], "the work actually happened"
    assert (await app.get(written.id)).status == RunStatus.DONE


async def test_a_decorated_handler_whose_wrapper_is_plain_is_still_run(app):
    seen = []

    async def greet(who):
        seen.append(who)

    app.register(Task(name="greet", handler=wrapped(greet)))

    await app.enqueue("greet", who="paulo")

    worker = Worker(app)
    await worker.run_once()
    await worker.drain()

    assert seen == ["paulo"]


async def test_a_store_that_blinked_never_costs_a_recurring_task_its_slot(app):
    """the slot was remembered before it was written, so one bad moment dropped that occurrence for good — for a daily task, a whole day"""
    moment = datetime(2026, 1, 1, 10, 0, 5, tzinfo=timezone.utc)
    original = type(app.store).add
    refused = []

    async def refusing(self, run):
        if not refused:
            refused.append(1)

            raise ConnectionError("the store went away")

        return await original(self, run)

    type(app.store).add = refusing

    try:

        @app.task("nightly", cron="0 4 * * *")
        async def nightly():
            return None

        with pytest.raises(ConnectionError):
            await app.materialize(moment)

        assert await app.count() == 0

        written = await app.materialize(moment)
    finally:
        type(app.store).add = original

    assert [run.due_at for run in written] == [datetime(2026, 1, 2, 4, 0, tzinfo=timezone.utc)], "the pass after it wrote the slot the failed one owed"


async def test_a_key_reserved_by_a_write_that_never_finished_is_not_lost_for_ever(app):
    """redis reserved the key and wrote the run in separate steps, so a process dying between them poisoned that key permanently"""
    from queuefy.store.redis import RedisStore

    if not isinstance(app.store, RedisStore):
        pytest.skip("the reservation and the write are one statement in every other store")

    @app.task("welcome")
    async def welcome(account):
        return None

    written = await app.enqueue("welcome", key="welcome:7", account=7)

    assert written.id is not None
    assert (await app.find("welcome:7")).id == written.id, "the key points at a run that exists"

    again = await app.enqueue("welcome", key="welcome:7", account=7)

    assert again.id == written.id


@pytest.mark.parametrize("options,reason", [({"poll": 0}, "does not wait"), ({"poll": -1}, "does not wait"), ({"concurrency": 0}, "never takes anything"), ({"grace": -1}, "not a span"), ({"lease": timedelta(seconds=0)}, "already run out")])
def test_a_worker_that_could_never_work_is_refused_where_it_is_written(app, options, reason):
    with pytest.raises(QueueError, match=reason):
        Worker(app, **options)


@pytest.mark.parametrize("options,reason", [({"max_attempts": 0}, "at least once"), ({"timeout": 0}, "before it starts"), ({"retry_delay": -1}, "never negative"), ({"max_retry_delay": 0}, "hammers instead of backing off"), ({"max_retry_delay": -5}, "hammers instead of backing off")])
def test_a_task_that_could_never_run_is_refused_where_it_is_declared(app, options, reason):
    with pytest.raises(QueueError, match=reason):
        app.task("broken", **options)(lambda: None)


async def test_a_worker_polling_is_not_a_worker_spinning(app):
    """the poll is a wait, and a worker that does not wait spends a core asking the store nothing"""
    asked = []
    original = type(app.store).reclaim

    async def counting(self, moment):
        asked.append(1)

        return await original(self, moment)

    type(app.store).reclaim = counting

    try:
        worker = Worker(app, poll=0.05)
        polling = asyncio.create_task(worker.run())

        await asyncio.sleep(0.25)
        worker.stop()
        await polling
    finally:
        type(app.store).reclaim = original

    assert len(asked) < 20, f"a quarter of a second asked the store {len(asked)} times, which is a spin and not a poll"


async def test_a_recurring_task_is_not_worked_out_again_on_every_poll(app):
    """a yearly expression is walked minute by minute, and asking the trigger before the cache spent a third of a core answering the same thing for ever"""
    asked = []

    @app.task("yearly", cron="0 0 1 1 *")
    async def yearly():
        return None

    original = type(app.tasks["yearly"].trigger).next_after

    def counting(self, moment):
        asked.append(1)

        return original(self, moment)

    type(app.tasks["yearly"].trigger).next_after = counting

    try:
        for _ in range(50):
            await app.materialize()
    finally:
        type(app.tasks["yearly"].trigger).next_after = original

    assert asked == [1], "worked out once for the slot it wrote, and never again while that slot is ahead"


async def test_a_reclaim_takes_a_batch_and_never_the_whole_backlog(app):
    """redis runs a script to the end before it answers anybody, so a backlog of expired leases would freeze the server"""
    from queuefy.store.redis import RECLAIM_BATCH, RedisStore

    if not isinstance(app.store, RedisStore):
        pytest.skip("only redis holds the whole server while a script runs")

    @app.task("work", max_attempts=3)
    async def work():
        return None

    for _ in range(RECLAIM_BATCH + 5):
        await app.enqueue("work")

    await app.store.claim("a-worker-that-died", ("default",), RECLAIM_BATCH + 5, timedelta(seconds=-1), datetime.now(timezone.utc))

    assert await app.store.reclaim(datetime.now(timezone.utc)) == RECLAIM_BATCH, "one pass took a batch"
    assert await app.store.reclaim(datetime.now(timezone.utc)) == 5, "and the pass after it took the rest"


async def test_a_run_somebody_deleted_by_hand_does_not_end_every_reclaim_for_ever(app):
    """one id left behind in the leases would make the script read a field that is gone, and every worker's pass would break on it"""
    from queuefy.store.redis import RedisStore

    if not isinstance(app.store, RedisStore):
        pytest.skip("only redis keeps the leases in a set of its own")

    @app.task("work", max_attempts=3)
    async def work():
        return None

    written = await app.enqueue("work")
    await app.store.claim("a-worker-that-died", ("default",), 1, timedelta(seconds=-1), datetime.now(timezone.utc))

    await app.store.client.delete(app.store.run_key(written.id))

    assert await app.store.reclaim(datetime.now(timezone.utc)) == 1, "it stepped over what was not there instead of breaking"
    assert await app.store.reclaim(datetime.now(timezone.utc)) == 0, "and the leases no longer name it"


async def test_a_run_names_the_same_instant_whichever_store_wrote_it(app):
    """`fromtimestamp` with no zone answers the wall clock of the machine, and calling that utc moved every instant the redis store read by the offset of wherever it ran"""
    when = datetime(2026, 8, 10, 12, 0, tzinfo=timezone.utc)

    @app.task("work")
    async def work():
        return None

    written = await app.enqueue_at("work", when)
    read = await app.get(written.id)

    assert read.due_at == when, "read back as the instant it was written, and not three hours off it"


async def test_a_datetime_with_no_zone_is_read_as_utc_by_every_store(app):
    """`timestamp()` on a naive value reads it as local time, and the same value would be one instant in one store and another instant in the other"""

    @app.task("work")
    async def work():
        return None

    written = await app.enqueue_at("work", datetime(2026, 8, 10, 12, 0))
    read = await app.get(written.id)

    assert read.due_at == datetime(2026, 8, 10, 12, 0, tzinfo=timezone.utc), "what a column without an offset means is what this means"


async def test_an_expression_that_asks_for_a_day_no_month_has_is_refused_where_it_is_written(app):
    """nothing but the search can tell february has no thirtieth, and the search walks a year of minutes to find that out — on every pass, for as long as the process lives"""
    with pytest.raises(CronError):
        Cron("0 0 30 2 *")

    assert Cron("0 0 30 2 5"), "with the weekday field restricted posix joins the two with an or, so a friday in february still matches"
    assert Cron("0 0 31 * *"), "and a thirty first is asked for by every month that has one"


async def test_a_day_field_is_read_by_how_it_was_written_and_not_by_what_it_adds_up_to():
    """posix joins the two day fields by whether each is a star, and this decided it by whether the parsed set covered the range — so '1-31' named every day there is and was read as a star, and '*/2' named half of them and was read as a restriction. both are days no other cron would have fired on, and a schedule that is wrong is the one failure nothing anywhere records"""
    # 2026-01-01 is a thursday, so the day after it is the second and the monday after it is the fifth
    thursday = datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc)

    assert Cron("0 0 1-31 * 1").next_after(thursday) == datetime(2026, 1, 2, tzinfo=timezone.utc), "a day field naming every day there is is still a restriction, so either of the two matches"
    assert Cron("0 0 */2 * 1").next_after(thursday) == datetime(2026, 1, 5, tzinfo=timezone.utc), "and a stepped star is still a star, so both of them have to match"

    # the refusal reads the two the same way, or an expression is accepted under one rule and searched under the other
    assert Cron("0 0 30 2 0-6"), "with the weekday field restricted the or is what makes every day of february match"

    with pytest.raises(CronError):
        Cron("0 0 30 2 */2")


async def test_a_run_the_store_lost_is_not_claimed_as_a_run_with_nothing_in_it(app):
    """a redis holding this under an eviction policy drops the hash and keeps the lane, and writing the fields of a claim over nothing builds a hash that holds those fields and no run"""
    from queuefy.store.redis import RedisStore

    if not isinstance(app.store, RedisStore):
        pytest.skip("only redis keeps the run and the lane it is queued in as two separate things")

    @app.task("work")
    async def work():
        return None

    written = await app.enqueue("work")
    await app.store.client.delete(app.store.run_key(written.id))

    assert await app.store.claim("a-worker", ("default",), 10, timedelta(seconds=60), datetime.now(timezone.utc)) == [], "nothing was handed out"
    assert await app.store.claim("a-worker", ("default",), 10, timedelta(seconds=60), datetime.now(timezone.utc)) == [], "and the lane no longer names it"


async def test_writing_a_run_is_tried_again_when_the_database_asks_for_it(app):
    """every worker writes the same occurrence key at the same instant, and innodb answers a duplicate two transactions race for with a deadlock as often as with a duplicate-key error"""
    from queuefy.store.sqlalchemy import SqlAlchemyStore

    if not isinstance(app.store, SqlAlchemyStore):
        pytest.skip("contention is what the database answers, and only this store speaks to one")

    @app.task("work")
    async def work():
        return None

    original = SqlAlchemyStore.insert
    refused = []

    async def deadlocking(self, run):
        if not refused:
            refused.append(run)

            raise DBAPIError("insert", {}, Exception(1213, "Deadlock found when trying to get lock"))

        return await original(self, run)

    SqlAlchemyStore.insert = deadlocking

    try:
        written = await app.enqueue("work")
    finally:
        SqlAlchemyStore.insert = original

    assert refused, "the database asked once"
    assert await app.get(written.id) is not None, "and the run was written on the try after it"


async def test_a_herd_that_failed_at_the_same_instant_does_not_come_back_as_a_herd(app):
    """a multiplier every one of them shares works out the same delay from the same numbers, and the herd is handed back whole"""
    drawn = {delay_for(RetryPolicy.EXPONENTIAL_JITTER, 10, 2, jitter=0.5) for _ in range(50)}

    assert len(drawn) > 1, "they were spread"
    assert min(drawn) >= 20, "and never sooner than the policy without jitter says"
    assert max(drawn) <= 30, "and never further out than the jitter allows"


async def test_a_run_that_is_over_is_dropped_once_it_is_older_than_the_worker_keeps(app):
    """nothing pruned this before, so the table and the keys grew for as long as the deployment lived"""

    @app.task("work")
    async def work():
        return None

    kept = await app.enqueue("work", key="only-once")
    worker = Worker(app, poll=0.05, keep=timedelta(days=7))

    await worker.run_once()
    await wait_until(lambda: worker.free == worker.concurrency)
    await worker.run_once()

    assert await app.get(kept.id) is not None, "what is over and recent stays, and a worker pass does not touch it"

    worker.keep = timedelta(0)
    worker.purged = None

    assert await worker.tidy(now()) == 1, "and the worker is what prunes it, on its own pass"
    assert await app.get(kept.id) is None
    assert await app.find("only-once") is None, "the reservation goes with the run it named, or what is left is a key nothing can ever write again"


async def test_pruning_is_housekeeping_and_never_the_work_of_a_pass(app):
    """a delete on every poll is a write per second per worker to drop what one an hour drops just as well"""

    @app.task("work")
    async def work():
        return None

    worker = Worker(app, poll=0.05, keep=timedelta(0))
    worker.purged = None
    asked = []

    async def counting(before, limit):
        asked.append(before)

        return 0

    worker.app.store.purge = counting

    await worker.tidy(now())
    await worker.tidy(now())
    await worker.tidy(now())

    assert len(asked) == 1, "the pass after it is inside the hour, and asks nothing"


async def test_a_worker_told_to_keep_everything_prunes_nothing(app):
    """dropping what somebody wanted kept is the one thing this must never do on its own"""
    worker = Worker(app, poll=0.05, keep=None)

    assert await worker.tidy(now()) == 0


async def test_every_way_a_run_ends_is_a_way_it_can_be_pruned(app):
    """three paths close a run — the handler, the policy running out and a lease nobody renewed — and one of them writing the state without saying it is over leaves that run behind for ever"""

    @app.task("breaks", max_attempts=1)
    async def breaks():
        raise RuntimeError("no")

    @app.task("works")
    async def works():
        return None

    await app.enqueue("works")
    await app.enqueue("breaks")

    worker = Worker(app, poll=0.05)
    await worker.run_once()
    await wait_until(lambda: worker.free == worker.concurrency)

    # written after the pass, so this one is abandoned by a lease nobody renewed instead of being worked
    abandoned = await app.enqueue("works")
    await app.store.claim("a-worker-that-died", ("default",), 10, timedelta(seconds=-1), now())

    assert await app.store.reclaim(now()) == 1
    assert (await app.get(abandoned.id)).status == RunStatus.FAILED

    await app.cancel((await app.enqueue("works")).id)
    assert await app.store.purge(now() + timedelta(days=1), 1000) == 4, "done, failed, given up on and called off"
    assert await app.count() == 0


async def test_an_outcome_the_store_would_not_take_is_not_announced_as_one_it_took(app):
    """the lease ran out and somebody else took the run over, and telling the listeners it finished puts one run in an audit trail twice"""
    told = []

    @app.task("work")
    async def work():
        return None

    worker = Worker(app, poll=0.05)

    async def finished(run, answer, seconds):
        told.append(run.id)

    worker.on_finish(finished)

    written = await app.enqueue("work")

    async def refused(run_id, name, attempt, moment, result):
        return False

    worker.app.store.complete = refused

    await worker.run_once()
    await wait_until(lambda: worker.free == worker.concurrency)

    assert told == [], f"the store refused the outcome of {written.id}, so whoever holds it now is the one that answers for it"


@pytest.mark.parametrize("outcome", ["boom", "permanent", "unknown"])
async def test_no_ending_the_store_would_not_take_is_announced_as_one_it_took(app, outcome):
    """the guard covered the run that finished and none of the three ways one ends badly, so a lost lease wrote one failure per worker into the audit trail"""
    told = []

    @app.task("work", max_attempts=3)
    async def work():
        raise PermanentError("no") if outcome == "permanent" else RuntimeError("boom")

    worker = Worker(app, poll=0.05)

    async def failed(run, error, seconds, retrying):
        told.append(run.id)

    worker.on_error(failed)

    written = await app.enqueue("work") if outcome != "unknown" else await app.store.add(Run(name="from_the_future"))

    async def refused(*arguments):
        return False

    worker.app.store.fail = refused
    worker.app.store.retry_later = refused
    worker.app.store.release = refused

    await worker.run_once()
    await wait_until(lambda: worker.free == worker.concurrency)

    assert told == [], f"the store refused how {written.id} ended, so whoever holds it now is the one that answers for it"


async def test_a_listener_that_is_a_plain_function_is_a_listener(app, caplog):
    """the same ambiguity a handler already answers for. it is not that the listener never ran — it ran, and then awaiting what it returned raised, which is a `TypeError` in a log nobody reads for a hook that looked like it worked"""
    told = []

    @app.task("work")
    async def work():
        return None

    worker = Worker(app, poll=0.05)
    worker.on_start(lambda run: told.append(run.name))

    await app.enqueue("work")
    await worker.run_once()
    await wait_until(lambda: worker.free == worker.concurrency)

    assert told == ["work"]
    assert "a listener of" not in caplog.text, "and nothing was logged as a listener that broke"


async def test_replicas_coming_up_together_all_come_up(app):
    """`create_all` asks whether the table is there and then creates it, and ten replicas booting together left eight of them dead on the question"""
    from queuefy.store.sqlalchemy import SqlAlchemyStore, metadata

    if not isinstance(app.store, SqlAlchemyStore):
        pytest.skip("only a database is asked to build anything")

    async with app.store.engine.begin() as connection:
        await connection.run_sync(metadata.drop_all)

    outcome = await asyncio.gather(*[SqlAlchemyStore(app.store.engine).setup() for _ in range(10)], return_exceptions=True)
    broke = [error for error in outcome if isinstance(error, BaseException)]

    assert broke == [], "every one of them came up"


async def test_a_database_that_refuses_to_build_the_table_still_says_so(app):
    """asking again is what tells a race apart from a real refusal, and swallowing both would be a worker that polls for ever against nothing"""
    from queuefy.store.sqlalchemy import SqlAlchemyStore

    if not isinstance(app.store, SqlAlchemyStore):
        pytest.skip("only a database is asked to build anything")

    broken = SqlAlchemyStore(create_async_engine("sqlite+aiosqlite:////this/is/not/a/place/runs.sqlite"))

    with pytest.raises(DBAPIError):
        await broken.setup()

    await broken.engine.dispose()


async def test_a_cluster_that_died_holding_everything_is_taken_back_a_batch_at_a_time(app):
    """one statement over every lease a dead cluster left is a transaction that holds the store while every other worker waits on it"""

    @app.task("work", max_attempts=5)
    async def work():
        return None

    for _ in range(RECLAIM_BATCH + 5):
        await app.enqueue("work")

    await app.store.claim("a-worker-that-died", ("default",), RECLAIM_BATCH + 5, timedelta(seconds=-1), now())

    assert await app.store.reclaim(now()) == RECLAIM_BATCH, "one pass took a batch"
    assert await app.store.reclaim(now()) == 5, "and the pass after it took the rest"


async def test_a_pruning_the_store_refused_does_not_cost_the_pass_its_claim(app):
    """housekeeping that ends the pass is a worker that stops claiming once an hour, for a reason nothing in the queue explains"""

    @app.task("work")
    async def work():
        return None

    worker = Worker(app, poll=0.05, keep=timedelta(0))
    worker.purged = None

    async def refuse(before, limit):
        raise RuntimeError("the store said no")

    worker.app.store.purge = refuse
    await app.enqueue("work")

    assert await worker.run_once(), "the pass went on and claimed what was due"
    assert worker.purged is not None, "and the hour started, so a store refusing it is not asked again on every poll"


async def test_a_backlog_is_drained_pass_by_pass_and_not_hour_by_hour(app):
    """a thousand an hour is a year of hours to catch up on a year that was never pruned"""
    worker = Worker(app, poll=0.05, keep=timedelta(0))
    worker.purged = None
    passes = []

    async def full(before, limit):
        passes.append(limit)

        return limit if len(passes) < 3 else 0

    worker.app.store.purge = full

    assert await worker.tidy(now()) == PURGE_LIMIT
    assert await worker.tidy(now()) == PURGE_LIMIT, "a full batch means there is more of it, and the pass after it takes the next one"
    assert await worker.tidy(now()) == 0
    assert await worker.tidy(now()) == 0, "and once it comes back short the hour starts"
    assert len(passes) == 3


async def test_ten_workers_do_not_all_prune_in_the_same_instant(app):
    """the same herd a drawn retry delay exists to spread, one deploy later"""
    drawn = {Worker(app).purged for _ in range(20)}

    assert len(drawn) > 1


async def test_keeping_a_run_for_less_than_no_time_is_refused(app):
    """it asks for what is over to be dropped before it is over, which is every one of them"""
    with pytest.raises(QueueError):
        Worker(app, keep=timedelta(seconds=-1))

    assert Worker(app, keep=timedelta(0)), "and dropping a run the moment it is over is a thing somebody may want"


async def test_a_name_the_worker_beside_this_one_knows_is_not_destroyed_by_this_one(app):
    """a rolling deploy runs two versions at once, and the older replica claiming what the newer one enqueued must not be the end of it — `max_attempts` is one by default, so failing it here is failing it for good"""
    written = await app.store.add(Run(name="from_the_future"))

    assert written.max_attempts == 1

    worker = Worker(app, poll=0.05)
    await worker.run_once()
    await wait_until(lambda: worker.free == worker.concurrency)

    settled = await app.get(written.id)

    assert settled.status == RunStatus.PENDING, "it waits for the replica that knows the name"
    assert settled.error_type == "UnknownTask", "and it says what it is waiting for"


async def test_a_name_nobody_knows_backs_off_instead_of_being_asked_for_ever(app):
    """what is never destroyed has to stop costing something, and the backoff is what does that. it is the base delay of the run and never a growing one: the attempt is given back, so the wait is worked out from the same attempt every time — which is the price of never spending a run's budget on a deploy"""
    written = await app.store.add(Run(name="nobody_knows", retry_policy=RetryPolicy.FIXED, retry_delay=30))

    worker = Worker(app, poll=0.05)
    await worker.run_once()
    await wait_until(lambda: worker.free == worker.concurrency)

    settled = await app.get(written.id)

    assert settled.due_at > now() + timedelta(seconds=20), "it is not asked for again on the next poll"


async def test_a_jitter_below_zero_is_refused(app):
    """it draws a delay shorter than the policy asked for, and one far enough below it is a retry due in the past — which is hammering, not backing off"""
    with pytest.raises(QueueError):
        Worker(app, jitter=-0.5)

    assert Worker(app, jitter=0), "and asking for no spread at all is a thing somebody may want"


async def test_a_timeout_stops_the_waiting_and_only_a_coroutine_stops_the_work(app):
    """python cannot end a thread from outside, so a plain handler carries on to its own end while the worker has already given up on it — and the attempt after it overlaps the one before"""
    ran = []

    @app.task("slow", timeout=0.1, max_attempts=2, retry_delay=0)
    def slow():
        ran.append("started")
        time.sleep(0.4)
        ran.append("finished anyway")

    worker = Worker(app, poll=0.05, keep=None)
    await app.enqueue("slow")

    for _ in range(8):
        await worker.run_once()
        await asyncio.sleep(0.08)

    await asyncio.sleep(0.6)

    assert ran.count("started") == 2, "the worker gave up on the first and started the attempt after it"
    assert ran.count("finished anyway") == 2, "and neither copy was stopped, which is what a timeout on a plain handler means"


async def test_a_worker_on_a_machine_with_a_long_name_can_still_claim_anything(app):
    """a pod is named after its deployment, its namespace and its cluster, and a name past what the column holds is a worker whose every claim the database refuses — it polls forever and takes nothing"""
    assert len(worker_name()) <= WORKER_NAME_LIMIT, "the name a worker gives itself always fits"

    @app.task("work")
    async def work():
        return None

    await app.enqueue("work")

    longest = Worker(app, name="x" * WORKER_NAME_LIMIT)

    assert len(await longest.app.store.claim(longest.name, ("default",), 1, timedelta(seconds=60), now())) == 1, "the store took the longest name a worker may have"

    with pytest.raises(QueueError, match="does not fit"):
        Worker(app, name="x" * (WORKER_NAME_LIMIT + 1))


async def test_a_name_this_worker_never_heard_of_costs_the_run_no_attempt(app):
    """`max_attempts` is one by default, so a rolling deploy that spent the attempt left the run to be destroyed by the first reclaim that met it"""
    written = await app.store.add(Run(name="from_the_future", max_attempts=1))

    worker = Worker(app, poll=0.05)
    await worker.run_once()
    await wait_until(lambda: worker.free == worker.concurrency)

    settled = await app.get(written.id)

    assert settled.status == RunStatus.PENDING
    assert settled.attempts == 0, "the attempt it never had is given back"
    assert await app.store.reclaim(now() + timedelta(hours=1)) == 0, "so nothing is left for a reclaim to give up on"
    assert (await app.get(written.id)).status == RunStatus.PENDING, "and the run is still there for the replica that knows the name"


async def test_a_claim_that_took_a_run_never_answers_without_it(app, monkeypatch):
    """the runs were read in a round trip of their own after the script had already taken them, so a connection that went away between the two left every one of them claimed, its attempt spent and its lease running, with the worker that won them holding nothing — and `max_attempts` is one by default, so each was failed under `LeaseExpired` with its handler never called"""
    from queuefy.store.redis import RedisStore

    if not isinstance(app.store, RedisStore):
        pytest.skip("only redis took its whole batch in one step and then read it in another")

    @app.task("work")
    async def work():
        return None

    for _ in range(3):
        await app.enqueue("work")

    def gone(*arguments, **keywords):
        raise ConnectionError("the connection went away once the runs had been taken")

    # everything the claim could ask the store for after its script, which is what it no longer asks anything of
    monkeypatch.setattr(app.store.client, "pipeline", gone)
    monkeypatch.setattr(app.store.client, "hgetall", gone)

    taken = await app.store.claim("a-worker", ("default",), 10, timedelta(seconds=60), now())

    assert len(taken) == 3, "the step that took the runs is the step that answered with them"
    assert await app.store.count(status=RunStatus.RUNNING) == 3, "and nothing is claimed that nobody came away holding"


async def test_a_lane_names_the_same_instant_a_run_was_written_for(app):
    """`timestamp()` on a value with no zone reads it as the wall clock of whoever wrote it, so a run queued in one zone came due in another"""
    from queuefy.store.redis import RedisStore

    if not isinstance(app.store, RedisStore):
        pytest.skip("only redis scores a queue by when a run is due")

    overdue = now().replace(tzinfo=None) - timedelta(hours=1)
    written = await app.store.add(Run(name="work", due_at=overdue))

    assert await app.store.client.zscore(f"{app.store.prefix}:queue:default:0", written.id) == (overdue.replace(tzinfo=timezone.utc) - EPOCH) // timedelta(microseconds=1)
    assert len(await app.store.claim("a-worker", ("default",), 1, timedelta(seconds=60), now())) == 1, "an hour overdue is due"


async def test_a_run_a_pruning_already_dropped_is_not_counted_as_depth(app):
    """a scan names what a pruning beside it has already deleted, and reading fields a hash no longer has counted a run that is gone"""
    from queuefy.store.redis import RedisStore

    if not isinstance(app.store, RedisStore):
        pytest.skip("only redis counts by walking what it owns")

    @app.task("work")
    async def work():
        return None

    await app.enqueue("work")

    naming = app.store.client.scan_iter

    async def naming_a_ghost(**keywords):
        async for name in naming(**keywords):
            yield name

        yield f"{app.store.prefix}:run:gone".encode()

    app.store.client.scan_iter = naming_a_ghost

    assert await app.count() == 1, "the one run that is there, and not the one that was dropped mid scan"


def test_a_run_allowed_a_thousand_attempts_still_works_out_a_delay():
    """doubling is an integer that outgrows a float long before the ceiling has anything to say, and the retry raised instead of waiting"""
    assert delay_for(RetryPolicy.EXPONENTIAL, 5.0, 1100, 0.0, 3600.0) == 3600.0
    assert delay_for(RetryPolicy.EXPONENTIAL_JITTER, 5.0, 4000, 0.5, 60.0) <= 60.0
    assert delay_for(RetryPolicy.EXPONENTIAL, 5.0, 3, 0.0, 3600.0) == 20.0, "and the growth below the ceiling is untouched"


async def test_a_write_the_database_refused_for_its_own_reasons_is_not_a_key_that_was_taken(app):
    """a run with no key never raced anybody for one, and reading every refusal as a duplicate handed the caller a run belonging to somebody else"""
    from queuefy.store.sqlalchemy import SqlAlchemyStore

    if not isinstance(app.store, SqlAlchemyStore):
        pytest.skip("only a database refuses a write for reasons of its own")

    somebody_else = await app.store.add(Run(name="work", payload={"who": "somebody else entirely"}))

    with pytest.raises(IntegrityError):
        await app.store_once(Run(name=None, payload={"who": "mine"}))

    assert (await app.get(somebody_else.id)).payload == {"who": "somebody else entirely"}, "and the run of somebody else was never handed out as this one"


async def test_a_caller_that_changes_the_run_it_enqueued_does_not_change_the_row(app):
    """every other store writes the run out and keeps nothing of the object, and one that kept it let a caller edit a queued run from the outside"""
    mine = Run(name="work", payload={"who": "paulo"})
    written = await app.store.add(mine)

    mine.payload["who"] = "changed after the store took it"
    mine.status = RunStatus.DONE

    stored = await app.get(written.id)

    assert stored.payload == {"who": "paulo"}
    assert stored.status == RunStatus.PENDING


async def test_a_reclaim_leaves_alone_a_run_somebody_claimed_while_it_was_waiting(app, monkeypatch):
    """the batch of expired leases is read in one statement and written in another, and the update asserted nothing about those rows — so a row the statement waited on a lock for was written whatever it had since become, and a worker still working lost the run out from under it"""
    from queuefy.store import sqlalchemy as backend

    if not isinstance(app.store, backend.SqlAlchemyStore):
        pytest.skip("only a database names a batch in one statement and writes it in another")

    written = await app.store.add(Run(name="work", max_attempts=5))
    working = await app.store.claim("worker-still-alive", ("default",), 1, timedelta(seconds=600), now())

    assert len(working) == 1, "a live worker holds it, on a lease with ten minutes left"

    # the batch a sweep already read still names this run, which is what it was before the live worker took it over
    async def already_named(self, session, condition, size):
        return [written.id]

    monkeypatch.setattr(backend.SqlAlchemyStore, "batch", already_named)

    assert await app.store.reclaim(now()) == 0, "no lease has run out, so there is nothing to take back"

    held = await app.get(written.id)

    assert held.status == RunStatus.RUNNING
    assert held.worker == "worker-still-alive", "the run stayed with the worker that is still working on it"


async def test_a_handler_reaching_for_a_name_that_is_not_there_is_a_handler_that_failed(app):
    """`UnknownTask` means this worker does not declare the name, and only the lookup can say that. read off the handler instead, a task fanning out to a name nobody registered was handed back with its attempt given back — for ever, at every poll, repeating everything it did before it raised"""
    ran = []

    @app.task("fan_out", max_attempts=1, retry_delay=0)
    async def fan_out():
        ran.append(1)

        await app.enqueue("a_name_nobody_declared")

    written = await app.enqueue("fan_out")
    worker = Worker(app, poll=0.05)

    for _ in range(4):
        await worker.run_once()
        await worker.drain()

    settled = await app.get(written.id)

    assert len(ran) == 1, "the attempt was spent, instead of the side effect repeating for ever"
    assert settled.status == RunStatus.FAILED
    assert settled.error_type == "UnknownTask"
    assert settled.attempts == 1


def test_a_herd_that_doubled_its_way_past_the_ceiling_is_still_spread():
    """the ceiling was applied to the drawn delay instead of to the growth under it, so every run that saturated it worked the very same wait out from the very same numbers — which is the herd this policy exists to break up, handed back whole"""
    drawn = {delay_for(RetryPolicy.EXPONENTIAL_JITTER, 5.0, 40, 0.5, 60.0) for _ in range(50)}

    assert len(drawn) > 1, "they were spread, and every one of them is far past the ceiling"
    assert max(drawn) <= 60.0, "and no wait is ever longer than the ceiling"
    assert min(drawn) >= 40.0, "and none is drawn further under it than the jitter asked for"


async def test_the_lease_a_claim_hands_out_runs_from_the_instant_the_run_was_taken(app):
    """the pass read the instant once and claimed with it after reclaiming, pruning and materializing — three round trips — so a slow pass handed out a lease it had already spent most of, and the next reclaim took the run back while it was still being worked"""
    waiting = 0.3

    @app.task("work")
    async def work():
        return None

    original = type(app.store).reclaim

    async def slowly(self, moment):
        await asyncio.sleep(waiting)

        return await original(self, moment)

    type(app.store).reclaim = slowly

    try:
        await app.enqueue("work")

        worker = Worker(app, lease=timedelta(seconds=60))
        began = now()
        claimed = (await worker.run_once())[0]

        await worker.drain()
    finally:
        type(app.store).reclaim = original

    assert claimed.started_at >= began + timedelta(seconds=waiting - 0.05), "the claim was made with the instant it happened, and not with the one the pass opened with"


@pytest.mark.parametrize("options,reason", [({"name": "x" * (TASK_NAME_LIMIT + 1)}, "the name of"), ({"name": "work", "queue": "q" * (QUEUE_LIMIT + 1)}, "the queue"), ({"name": "x" * TASK_NAME_LIMIT, "every": 60}, "the key each slot")])
def test_a_name_a_store_could_never_hold_is_refused_where_it_is_declared(app, options, reason):
    """the worker name has been sized against the column it goes in since the beginning, and the task name, the queue and the key never were. a name past the column is a write the database refuses, and on one that is not strict a name quietly cut short — where two slots of two different minutes become one run"""
    with pytest.raises(QueueError, match=reason):
        app.task(**options)(lambda: None)


def test_a_task_that_fits_every_column_it_touches_is_declared(app):
    """the limits are the columns and not a guess at them, so the longest name a store can hold is a name that works"""
    assert app.task("x" * TASK_NAME_LIMIT, queue="q" * QUEUE_LIMIT)(lambda: None)

    # the slot key is the name and the widest instant one is ever named for, so a recurring task has less of the column to itself than a plain one
    assert app.task("y" * (KEY_LIMIT - len(occurrence_key("", WIDEST_SLOT))), every=60)(lambda: None)


async def test_a_key_a_store_could_never_hold_is_refused_at_the_call(app):
    """a caller keying a run by something long — an url, a rendered template — had it cut short by the database, and the run that came back was somebody else's"""

    @app.task("work")
    async def work():
        return None

    with pytest.raises(QueueError, match="the key"):
        await app.enqueue("work", key="k" * (KEY_LIMIT + 1))

    assert await app.enqueue("work", key="k" * KEY_LIMIT), "and the longest key a store can hold is a key that works"


async def test_the_columns_a_store_builds_are_the_limits_the_app_refuses_by(app):
    """two numbers written down twice drift, and the day they do the app refuses what the store would have taken or takes what it will not hold"""
    from queuefy.store.sqlalchemy import SqlAlchemyStore, runs

    if not isinstance(app.store, SqlAlchemyStore):
        pytest.skip("only a database sizes a column")

    assert (runs.c.name.type.length, runs.c.queue.type.length, runs.c.key.type.length) == (TASK_NAME_LIMIT, QUEUE_LIMIT, KEY_LIMIT)
    assert runs.c.worker.type.length == WORKER_NAME_LIMIT
    assert (runs.c.error.type.length, runs.c.error_type.type.length) == (ERROR_LIMIT, ERROR_TYPE_LIMIT)


async def test_a_reclaim_pass_takes_one_batch_however_many_ways_a_lease_can_end(app, monkeypatch):
    """the two halves of a sweep each took a batch of their own, so a pass over a cluster that died holding a mix of both wrote twice what the constant says one pass writes — and the constant is there to bound how long the store is held"""
    from queuefy.store import sqlalchemy as backend

    if not isinstance(app.store, backend.SqlAlchemyStore):
        pytest.skip("only this store takes back what is spent and what is not in two statements")

    monkeypatch.setattr(backend, "RECLAIM_BATCH", 10)

    @app.task("spent", max_attempts=1)
    async def spent():
        return None

    @app.task("work", max_attempts=5)
    async def work():
        return None

    for _ in range(10):
        await app.enqueue("spent")

    for _ in range(5):
        await app.enqueue("work")

    await app.store.claim("a-cluster-that-died", ("default",), 15, timedelta(seconds=-1), now())

    assert await app.store.reclaim(now()) == 10, "one pass took one batch, and not one of each kind"
    assert await app.store.reclaim(now()) == 5, "and the pass after it took the rest"


async def test_a_herd_the_database_rolled_back_does_not_come_back_in_lockstep(monkeypatch):
    """the wait between tries was a fixed schedule every contender worked out from the same numbers, so the transactions innodb rolled back came back together, deadlocked on each other again and spent the whole budget doing it. measured against a hot queue with forty claims and a reclaim on the same rows, that left outcomes the store refused on every single run — and a refused outcome is work silently done again a lease later"""
    from queuefy.store import sqlalchemy as backend

    waits = []

    async def instead(seconds):
        waits.append(seconds)

    monkeypatch.setattr(backend.asyncio, "sleep", instead)

    async def deadlocking():
        raise DBAPIError("UPDATE", {}, Exception(1213, "Deadlock found when trying to get lock"))

    for _ in range(2):
        with pytest.raises(DBAPIError):
            await backend.under_contention(deadlocking)

    mine, theirs = waits[: backend.TRIES - 1], waits[backend.TRIES - 1 :]

    assert len(mine) == len(theirs) == backend.TRIES - 1
    assert mine != theirs, "two transactions rolled back by the same deadlock do not come back at the same instants"
    assert all(later > earlier for earlier, later in zip(mine, mine[1:])), "and each wait is longer than the one before it"
    assert sum(mine) >= 0.6, "the budget outlasts a burst of contention instead of adding a few milliseconds and giving up on it"


async def test_priority_orders_a_claim_across_every_queue_the_worker_serves(app):
    """the lanes were walked one queue at a time, so a worker serving two of them drained the first to its end — and handed out an ordinary run while an urgent one sat waiting next door"""
    await app.store.add(Run(name="ordinary", queue="reports", priority=0, due_at=now() - timedelta(minutes=5)))
    await app.store.add(Run(name="urgent", queue="email", priority=10, due_at=now()))

    taken = await app.store.claim("a-worker", ("reports", "email"), 1, timedelta(seconds=60), now())

    assert [run.name for run in taken] == ["urgent"], "a queue is where a run waits and never what orders it"


async def test_the_oldest_of_a_priority_goes_first_whichever_queue_it_waits_in(app):
    """one lane per queue answers which run of that queue is oldest, and nothing was merging the answers — so the order inside a priority was the order the queues were named in"""
    await app.store.add(Run(name="second", queue="email", priority=5, due_at=now() - timedelta(minutes=1)))
    await app.store.add(Run(name="first", queue="reports", priority=5, due_at=now() - timedelta(minutes=9)))

    taken = await app.store.claim("a-worker", ("email", "reports"), 2, timedelta(seconds=60), now())

    assert [run.name for run in taken] == ["first", "second"]


async def test_a_recurring_task_is_measured_by_the_longest_key_it_will_ever_write(app, monkeypatch):
    """the guard read the one slot the task wanted next, and an interval landing on microseconds every other slot goes on to write a key seven characters longer than that one — accepted where it was declared, and then refused by `build` on every pass of every worker, before anything was ever claimed"""
    from queuefy import app as application

    fits = "n" * (KEY_LIMIT - len(occurrence_key("", WIDEST_SLOT)))

    # the guard read the clock, so which of the two keys it measured was a coin toss, and this is the toss that let the longer one through. only what is declared under it is pinned, because materializing wants a clock that moves
    with monkeypatch.context() as clock:
        clock.setattr(application, "now", lambda: EPOCH + timedelta(seconds=0.5))

        with pytest.raises(QueueError, match="the key each slot"):
            app.task(fits + "n", every=0.5)(lambda: None)

        assert app.task(fits, every=0.5)(lambda: None), "and the longest name that fits every slot it writes is a name that works"

    # both widths the interval alternates between, every one of them a key the app writes instead of raising on
    for _ in range(4):
        await app.materialize()
        app.written.clear()

        await asyncio.sleep(0.3)

    assert await app.count() >= 2, "the slots really were written, on a name at the very edge of the column"


@pytest.mark.parametrize("queues,reason", [((), "serving no queues"), (("q" * (QUEUE_LIMIT + 1),), "no task could ever be declared")])
def test_a_worker_that_could_never_be_served_is_refused_where_it_is_written(app, queues, reason):
    """a lease that has run out and a concurrency of zero were both refused, and a worker with nothing to serve was not — it came up, polled, claimed nothing and never said a word about why"""
    with pytest.raises(QueueError, match=reason):
        Worker(app, queues=queues)


@pytest.mark.parametrize("seconds", [0.001, 0.01, 0.1, 0.2, 0.3, 0.7, 1.0, 2.5])
def test_an_interval_always_names_a_slot_that_is_still_to_come(seconds):
    """the count of whole slots was carried back through a multiplication a float cannot undo, so the slot that came out was the instant it was asked after — the occurrence already written, and never the one after it"""
    moment = now()

    for _ in range(2000):
        due_at = Interval(seconds).next_after(moment)

        assert due_at > moment, f"an interval of {seconds}s answered the instant it was asked after"

        moment = due_at


def test_an_interval_no_store_could_tell_apart_is_refused_where_it_is_written(app):
    """every store keeps an instant to the microsecond, so a slot under one is the slot before it under another name — and counting in whole slots divides by a span that rounded to nothing"""
    with pytest.raises(QueueError, match="finer than the microsecond"):
        app.task("often", every=0.0000004)(lambda: None)

    assert Interval(0.000001), "and the finest span a store can still tell apart is one that works"


async def test_an_id_a_pruning_freed_is_never_handed_to_the_run_after_it(app):
    """sqlite hands out the highest id it can see plus one, so pruning the newest settled run gave its id to the next run written — and a caller holding an id from before the pruning read, and called off, somebody else's run"""
    from queuefy.store.sqlalchemy import SqlAlchemyStore

    if not isinstance(app.store, SqlAlchemyStore):
        pytest.skip("only a database counts its own ids, and only sqlite ever counts one back down")

    @app.task("work")
    async def work():
        return None

    dropped = await app.enqueue("work", payload={"which": "the one that was pruned"})
    worker = Worker(app, keep=timedelta(seconds=0))

    await worker.run_once()
    await worker.drain()
    await wait_until(lambda: worker.free == worker.concurrency)

    assert await app.store.purge(now() + timedelta(seconds=1), PURGE_LIMIT) == 1

    written = await app.enqueue("work", payload={"which": "the one written after it"})

    assert written.id != dropped.id, "the id of a run that was pruned is spent, and never handed out again"
    assert await app.get(dropped.id) is None, "so what a caller holding it reads is nothing, and never a run belonging to somebody else"


async def test_a_lease_running_out_this_very_instant_is_one_the_worker_still_holds(app):
    """the range redis read the expired leases with took in the instant itself, where every other store stops short of it — so on redis alone a run was handed to a second worker in the moment the first was still working it"""
    written = await app.store.add(Run(name="work", max_attempts=3))
    moment = now()

    taken = await app.store.claim("worker-1", ("default",), 1, timedelta(seconds=60), moment)

    assert await app.store.reclaim(taken[0].lease_until) == 0, "a lease is held up to the instant it runs out, and not up to the one before it"
    assert (await app.get(written.id)).status == RunStatus.RUNNING


async def test_a_run_that_is_over_is_kept_until_it_is_older_than_the_instant_asked_for(app):
    """the same range, one method along: what a pruning drops is a run that finished strictly before the instant, and redis dropped the ones that finished exactly on it"""
    written = await app.store.add(Run(name="work"))
    taken = await app.store.claim("worker-1", ("default",), 1, timedelta(seconds=60), now())
    finished = now()

    await app.store.complete(taken[0].id, "worker-1", 1, finished, None)

    assert await app.store.purge(finished, PURGE_LIMIT) == 0
    assert await app.get(written.id) is not None
    assert await app.store.purge(finished + timedelta(microseconds=1), PURGE_LIMIT) == 1


async def test_a_key_of_no_characters_is_refused_where_it_is_written(app):
    """a column takes an empty key as a name that can be held once and redis reads it as no key at all, so the same call folded every enqueue into one row on a database and wrote a row every time on redis — and nothing anywhere said which of the two an application was getting"""

    @app.task("work")
    async def work():
        return None

    with pytest.raises(QueueError, match="names nothing"):
        await app.enqueue("work", key="")

    assert await app.count() == 0, "and nothing was written on the way to finding that out"
    assert await app.enqueue("work", key="k"), "the shortest key a store can tell runs apart by is one that works"


async def test_an_interval_whose_slot_no_datetime_could_hold_is_refused_where_it_is_written(app):
    """the guard measured how fine an interval is and never how wide. one whose first slot lands past the last instant a datetime holds was taken where it was declared, and then raised inside `materialize` on every pass of every worker — before a single run was ever claimed, against a store with nothing wrong with it"""

    @app.task("work")
    async def work():
        return None

    with pytest.raises(QueueError, match="past the last instant"):
        app.task("millennial", every=WIDEST_SECONDS + 1)(lambda: None)

    await app.enqueue("work")

    worker = Worker(app)
    claimed = await worker.run_once()

    await worker.drain()

    assert len(claimed) == 1, "so the pass of every worker goes on claiming what is due"
    assert Interval(WIDEST_SECONDS).next_after(now()) <= WIDEST_SLOT, "and the widest interval whose first slot still fits is one that works"


async def test_a_failure_no_column_could_hold_is_still_an_ending_the_store_takes(app):
    """a message and the class that raised it are whatever the code being run put in them, so unlike a name or a key there is nowhere to refuse one where it is written. past the column mysql refused the message and postgres refused the class, and the ending never reached the store at all — the run stayed claimed, and every lease after that ran the very same handler again"""
    wordy = type("V" * (ERROR_TYPE_LIMIT * 2), (RuntimeError,), {})

    @app.task("wordy", max_attempts=1)
    async def raises():
        raise wordy("x" * (ERROR_LIMIT * 3))

    written = await app.enqueue("wordy")
    worker = Worker(app, poll=0.05)

    await worker.run_once()
    await wait_until(lambda: worker.free == worker.concurrency)

    settled = await app.get(written.id)

    assert settled.status == RunStatus.FAILED, "it ended where it failed, instead of waiting on a lease to close it as a run nobody answered for"
    assert (len(settled.error), len(settled.error_type)) == (ERROR_LIMIT, ERROR_TYPE_LIMIT)
    assert settled.error.endswith("...") and settled.error_type.endswith("..."), "and what was cut says so, instead of reading as the whole of it"


async def test_a_failure_no_column_could_encode_is_still_an_ending_the_store_takes(app):
    """the length was cut on the way in and the characters were not. a path is bytes, and read back off a posix system it carries whatever bytes it had as lone surrogates — so `open` on a file somebody uploaded from another machine raises with one in the message. sqlite, mysql, postgres and redis each refused it, the ending never reached the store, the run stayed claimed, and every lease after that ran the very same handler again until the attempts were spent and a reclaim closed it with nothing anywhere saying what broke"""

    @app.task("unwritable", max_attempts=1)
    async def raises():
        raise FileNotFoundError("no such file: '/data/\udcc3\udca9.csv', and \x00 came with it")

    written = await app.enqueue("unwritable")
    worker = Worker(app, poll=0.05)

    await worker.run_once()
    await wait_until(lambda: worker.free == worker.concurrency)

    settled = await app.get(written.id)

    assert settled.status == RunStatus.FAILED, "it ended where it failed, instead of running again on every lease until the attempts were spent"
    assert "\\udcc3" in settled.error and "\\x00" in settled.error, "and what no store could hold says what it was, instead of being dropped"
    assert settled.error_type == "FileNotFoundError"


def test_a_failure_that_had_to_be_escaped_is_still_cut_to_what_the_column_holds():
    """escaping is what makes a message longer, so a message cut first and escaped after was one that fit the column before the write and not during it — and the ending never reached the store for the very reason the cutting exists"""
    message, kind = described(RuntimeError("\udcff" * ERROR_LIMIT))

    assert len(message) == ERROR_LIMIT, "it was made storable and then cut, and not the other way round"
    assert len(kind) <= ERROR_TYPE_LIMIT
    assert message.encode("utf-8") and kind.encode("utf-8"), "and what is left is text every store can write"


async def test_a_listener_that_asks_the_process_to_stop_does_not_take_the_run_with_it(app):
    """`sys.exit()` deep inside a library is not an `Exception`, and the guard a handler has had never covered the listeners beside it. announced before the handler runs, it ended the attempt where it stood: the work never started, no outcome was ever written, and the run sat claimed until a lease closed it as one nobody answered for"""
    ran = []

    @app.task("work", max_attempts=1)
    async def work():
        ran.append(1)

    def leaving(run):
        raise SystemExit(1)

    worker = Worker(app, poll=0.05)
    worker.on_start(leaving)

    written = await app.enqueue("work")

    await worker.run_once()
    await wait_until(lambda: worker.free == worker.concurrency)

    assert ran == [1], "the listener that broke broke alone, and the work happened"
    assert (await app.get(written.id)).status == RunStatus.DONE


async def test_a_listener_meeting_a_cancellation_still_passes_it_on(app):
    """the guard around a listener had to widen to hold what a handler is already held against, and a cancellation is the one thing it must go on letting through — logged and stepped over there, it is a worker nobody can stop"""

    @app.task("work")
    async def work():
        return None

    async def cancelling(run):
        raise asyncio.CancelledError

    worker = Worker(app)
    worker.on_start(cancelling)

    await app.enqueue("work")
    claimed = await worker.run_once()
    running = next(iter(worker.running))

    with pytest.raises(asyncio.CancelledError):
        await running

    assert len(claimed) == 1, "the run was taken, and the cancellation ended the attempt instead of being written off as a listener that broke"


async def test_a_client_that_answers_strings_is_refused_where_the_store_is_built(app):
    """this store reads what redis answers as bytes, and an application sharing its client very often has the setting that changes that turned on. nothing below could tell: enqueueing went on working, so the queue took everything it was given, and every claim of every worker raised — a process that came up, polled, took nothing and said the same thing once a second while the backlog grew behind it"""
    from redis.asyncio import Redis

    from queuefy.store.redis import RedisStore

    if not isinstance(app.store, RedisStore):
        pytest.skip("only redis hands back what it holds as bytes")

    talkative = Redis.from_url(REDIS_URL, decode_responses=True)

    try:
        with pytest.raises(QueueError, match="decode_responses"):
            RedisStore(talkative)
    finally:
        await talkative.aclose()

    assert RedisStore(app.store.client), "and the client this store is meant to be given is one that works"


async def test_a_priority_drawn_from_something_that_keeps_changing_is_refused(app):
    """a priority names a lane and never a score: redis keeps one sorted set per priority a queue has ever seen, and a claim walks every one of them inside a script that holds the server while it runs. drawn from a clock or an id it is a new lane every second, for ever — and nothing anywhere said so, while the queue went on working and got slower every hour it ran"""
    with pytest.raises(QueueError, match="a priority runs from"):
        app.task("hot", priority=int(now().timestamp()))(lambda: None)

    @app.task("work", priority=PRIORITY_LIMIT)
    async def work():
        return None

    with pytest.raises(QueueError, match="a priority runs from"):
        await app.enqueue("work", priority=-PRIORITY_LIMIT - 1)

    assert await app.count() == 0, "and nothing was written on the way to finding that out"
    assert await app.enqueue("work", priority=-PRIORITY_LIMIT), "and the outermost lane either way is one that works"


@pytest.mark.parametrize("count", [5.0, True])
async def test_a_count_no_store_could_read_back_is_refused_where_it_is_written(app, count):
    """a priority and an attempt count are written down as text and read back with `int`, and neither a float nor a boolean is text that reads back. taken at the call, a priority of `5.0` sat in redis as a lane of that name and every claim of every worker raised on it from then on — not the one run, but the whole pass, so a fleet that had been working stopped taking anything at all over a single enqueue"""

    @app.task("work")
    async def work():
        return None

    with pytest.raises(QueueError, match="whole number"):
        app.task("urgent", priority=count)(lambda: None)

    with pytest.raises(QueueError, match="whole number"):
        app.task("stubborn", max_attempts=count)(lambda: None)

    with pytest.raises(QueueError, match="whole number"):
        await app.enqueue("work", priority=count)

    assert await app.count() == 0, "and nothing was written on the way to finding that out"
    assert await app.enqueue("work", priority=5), "and the lane a store reads back the way it wrote it is one that works"


async def test_a_failure_that_cannot_say_what_it_was_is_still_an_ending_the_store_takes(app):
    """the message is escaped and then cut so that nothing about it can stop the ending being written, and asking the failure what it says was the one step before all of that with nothing holding it. an exception carrying an object whose own `__str__` raises took the ending down with it: nothing was ever written, the run stayed claimed, and every lease after that ran the very same handler again until the attempts were spent and a reclaim closed it as a run nobody answered for"""

    @app.task("unspeakable", max_attempts=1)
    async def raises():
        raise Unspeakable("the object this message is built from cannot be read")

    written = await app.enqueue("unspeakable")
    worker = Worker(app, poll=0.05)

    await worker.run_once()
    await wait_until(lambda: worker.free == worker.concurrency)

    settled = await app.get(written.id)

    assert settled.status == RunStatus.FAILED, "it ended where it failed, instead of waiting on a lease to close it as a run nobody answered for"
    assert (settled.error, settled.error_type) == ("Unspeakable", "Unspeakable"), "and what raised is on the record, because it was the only thing left that could be read"


async def test_a_worker_given_its_one_queue_as_a_name_is_refused_where_it_is_written(app):
    """a string is a sequence of its letters, so a worker given `emails` served `e`, `m`, `a`, `i`, `l` and `s` and never the queue it was named for. memory found the name inside the string and worked, sqlalchemy refused the statement on every pass, and redis quietly claimed nothing at all — one mistake, three answers, and the two that mattered were silent"""

    @app.task("work", queue="emails")
    async def work():
        return None

    with pytest.raises(QueueError, match="one letter at a time"):
        Worker(app, queues="emails")

    await app.enqueue("work")

    worker = Worker(app, queues=("emails",))
    claimed = await worker.run_once()

    await worker.drain()

    assert len(claimed) == 1, "and the tuple that says the same thing is one that works"


@pytest.mark.parametrize("housekeeping", ["reclaim", "purge"])
async def test_the_housekeeping_names_the_rows_it_writes_by_id_and_never_by_the_index_it_found_them_with(app, housekeeping):
    """innodb named the cycle: naming the rows inside the write let the database drive the statement off the index the condition reads by, so it locked a secondary entry and then reached for the row — while every close of a run locks the row and then reaches for that same entry. two orders around one row is a deadlock, and the one innodb rolled back was always the close, so a run that had already finished was left claimed and its work done all over again a lease later"""
    from sqlalchemy import event

    from queuefy.store.sqlalchemy import SqlAlchemyStore

    if not isinstance(app.store, SqlAlchemyStore):
        pytest.skip("only a database chooses an index to drive a statement with")

    written = await app.store.add(Run(name="work", max_attempts=1))
    await app.store.claim("a-worker-that-died", ("default",), 1, timedelta(seconds=-1), now())
    await app.store.reclaim(now())

    if housekeeping == "purge":
        assert (await app.get(written.id)).status == RunStatus.FAILED, "a lease nobody renewed left it over and ready to be pruned"

    said = []

    def overheard(connection, cursor, statement, parameters, context, executemany):
        said.append(" ".join(statement.split()))

    event.listen(app.store.engine.sync_engine, "before_cursor_execute", overheard)

    try:
        asked = await app.store.purge(now() + timedelta(days=1), PURGE_LIMIT) if housekeeping == "purge" else await app.store.reclaim(now())
    finally:
        event.remove(app.store.engine.sync_engine, "before_cursor_execute", overheard)

    changed = [statement for statement in said if statement.startswith(("UPDATE", "DELETE"))]

    assert asked == (1 if housekeeping == "purge" else 0)
    assert changed if housekeeping == "purge" else not changed, "a pruning wrote, and a reclaim with nothing expired wrote nothing at all"

    for statement in changed:
        assert "SELECT" not in statement, f"the rows are read out first and never named inside the write: {statement}"
        assert "id IN" in statement, f"and the write names them by the key every close of a run locks first: {statement}"


@pytest.mark.parametrize("concurrency", [4.0, True])
def test_a_worker_holding_a_concurrency_no_claim_could_be_made_with_is_refused(app, concurrency):
    """a count of cores halved is a float, and the number goes on to be the limit of every claim: memory sliced a list by it and raised, and the servers took a limit nothing means. it came up, polled, raised once a second and never took a thing — the failure a whole number is asked for everywhere else to prevent"""
    with pytest.raises(QueueError, match="not a whole number of runs"):
        Worker(app, concurrency=concurrency)

    assert Worker(app, concurrency=4).free == 4, "and the whole number that says the same thing is one that claims"


@pytest.mark.parametrize("payload,reason", [({"when": object()}, "not something a store can write"), ({"file": "photo-\udcff.jpg"}, "not something a store can write")])
async def test_a_payload_no_store_could_write_is_refused_where_it_is_written(app, payload, reason):
    """the arguments of a run are json wherever they live, and nothing was asking for that at the call. memory took an object no store could serialise and every real one refused it deep inside a driver, so an application built against memory enqueued it and met it in production — and a lone surrogate, which is what a posix path carries when its bytes were never utf-8, was taken by four stores and refused by mysql"""

    @app.task("work")
    async def work(**arguments):
        return None

    with pytest.raises(QueueError, match=reason):
        await app.enqueue("work", payload=payload)

    assert await app.count() == 0, "and nothing was written on the way to finding that out"
    assert await app.enqueue("work", payload={"file": "photo.jpg"}), "while the arguments every store holds are arguments that work"


async def test_an_answer_no_column_could_encode_is_still_an_ending_the_store_takes(app):
    """a message has been escaped on the way in since the day mysql refused one, and what the handler answered never was. a run returning a path read back off a filesystem — bytes that were never utf-8, carried as lone surrogates — had its ending refused, so the run stayed claimed, the lease handed it back, and the very same handler ran again on every one of them until the attempts were spent and a reclaim closed it as a run nobody answered for"""

    @app.task("listing", max_attempts=3)
    async def listing():
        return {"file": "photo-\udcff.jpg", "beside": ["a\udcff"], "under": ("b\udcff",), "count": 2, "nested": {"deep": "c\udcff"}}

    written = await app.enqueue("listing")
    worker = Worker(app, poll=0.05)

    await worker.run_once()
    await wait_until(lambda: worker.free == worker.concurrency)

    settled = await app.get(written.id)

    assert settled.status == RunStatus.DONE, "the ending reached the store, instead of a run left claimed for a lease to hand back"
    assert settled.attempts == 1, "and it was run once, and not once for every lease that ran out under it"
    assert settled.result == {"file": "photo-\\udcff.jpg", "beside": ["a\\udcff"], "under": ["b\\udcff"], "count": 2, "nested": {"deep": "c\\udcff"}}, "and what the handler answered is on the record as the text that says what it was"


async def test_a_run_written_with_a_field_already_filled_is_read_back_with_it(app):
    """what a store is handed it writes down, and this one wrote fourteen of the twenty-two columns and left the rest to their defaults. memory and redis read back what they were given and a database read back nothing, so a run written out of one this store had answered came back with its ending, its lease and its result all quietly gone"""
    moment = now()
    given = Run(name="work", key="filled", due_at=moment, attempts=2, max_attempts=5, worker="a-machine", lease_until=moment, started_at=moment, finished_at=moment, result={"ok": True}, error="boom", error_type="Error")
    written = await app.store.add(given)
    read = await app.store.get(written.id)

    assert (read.worker, read.error, read.error_type, read.result) == ("a-machine", "boom", "Error", {"ok": True})
    assert (read.lease_until, read.started_at, read.finished_at) == (moment, moment, moment), "the instants included, because a lease a store forgot is a run nothing ever takes back"


@pytest.mark.parametrize("payload", [{"ratio": float("nan")}, {"ratio": float("inf")}, {"ratio": float("-inf")}])
async def test_a_number_no_other_reader_of_json_takes_is_refused_where_it_is_written(app, payload):
    """the guard asked python's own serialiser whether the arguments were json and it said yes, because python writes `nan` and `infinity` as words — which no other reader of json has. memory, sqlite and redis held one where mysql and postgres refused it deep inside a driver, so an enqueue that worked all the way through development raised in production over an average of nothing at all"""

    @app.task("work")
    async def work(**arguments):
        return None

    with pytest.raises(QueueError, match="not something a store can write"):
        await app.enqueue("work", payload=payload)

    assert await app.count() == 0, "and nothing was written on the way to finding that out"


async def test_arguments_a_store_reads_back_as_something_else_are_settled_where_they_are_written(app):
    """a tuple comes back as a list and a key that is not a string comes back as one, in every store but the one that keeps the object it was handed. so the same call gave a handler a tuple in memory and a list on every real store, and an application built against memory met the difference in production — the divergence the arguments are already refused for, one step along"""

    @app.task("work")
    async def work(**arguments):
        return None

    written = await app.enqueue("work", payload={"pair": (1, 2), 3: "keyed by a number", "nested": {"inner": ("a",)}})

    assert written.payload == {"pair": [1, 2], "3": "keyed by a number", "nested": {"inner": ["a"]}}, "the arguments were settled once, instead of meaning one thing in memory and another everywhere else"
    assert (await app.get(written.id)).payload == written.payload, "and what the store reads back is what the call was answered with"


@pytest.mark.parametrize("answer", [{"ratio": float("nan")}, {"ratio": float("inf")}, {"when": object()}, {"tags": {"a"}}, {(1, 2): "a"}])
async def test_an_answer_no_store_could_write_ends_the_run_where_it_happened(app, answer):
    """the escaping the answer got covered a lone surrogate and nothing else. `nan` and `infinity` are words only python's own reader of json has, so memory, sqlite and redis held one where mysql and postgres refused it — and an object no serialiser takes was one memory kept while every other store threw the ending away over it. a key that is not hashable was worse still: the escaping raised on it before any store was asked, in every store there is. each of them left the run claimed, and the lease ran the very same handler again on every one of them until the attempts were spent, under a message about a worker that stopped answering"""
    ran = []

    @app.task("odd", max_attempts=3)
    async def odd():
        ran.append(1)

        return answer

    written = await app.enqueue("odd")
    worker = Worker(app, poll=0.05)

    await worker.run_once()
    await wait_until(lambda: worker.free == worker.concurrency)

    settled = await app.get(written.id)

    assert settled.status == RunStatus.FAILED, "it ended where it happened, instead of waiting on a lease to close it as a run nobody answered for"
    assert settled.error_type == "UnwritableAnswer", "and it says what broke, instead of naming a worker that never stopped answering"
    assert (ran, settled.attempts) == ([1], 1), "and the side effect happened once, because no attempt makes a handler answer something else"


async def test_an_answer_a_store_reads_back_as_something_else_is_settled_where_it_is_written(app):
    """a key that is not a string comes back as one in every store but the one that keeps the object it was handed, so the same handler left a numeric key in memory and a string on every real store — and the listener beside it was told one thing while the store held another. the divergence the arguments of a run are already settled against, one field along"""
    told = []

    @app.task("listing")
    async def listing():
        return {1: "a", "pair": (2, 3)}

    written = await app.enqueue("listing")
    worker = Worker(app, poll=0.05)
    worker.on_finish(lambda run, answer, seconds: told.append(answer))

    await worker.run_once()
    await wait_until(lambda: worker.free == worker.concurrency)

    assert (await app.get(written.id)).result == {"1": "a", "pair": [2, 3]}, "the answer was settled once, instead of meaning one thing in memory and another everywhere else"
    assert told == [{"1": "a", "pair": [2, 3]}], "and what the listener was told is what the store holds"


@pytest.mark.parametrize("options", [{"every": float("nan")}, {"timeout": float("nan")}, {"timeout": float("inf")}, {"retry_delay": float("nan")}, {"max_retry_delay": float("nan")}, {"max_retry_delay": float("inf")}])
def test_a_span_a_task_could_never_be_worked_out_from_is_refused_where_it_is_declared(app, options):
    """every guard here is a comparison, and `nan` is false against all of them while `infinity` is past every one — so both went through each one and reached the arithmetic instead. a `retry_delay` of `nan` raised inside `timedelta` exactly where the ending of a run was being written: the ending never reached the store, the run stayed claimed, and the lease ran the very same handler again on every one of them until the attempts were spent. an interval of `nan` raised a `ValueError` out of the standard library rather than saying what was asked for and why it cannot be"""
    with pytest.raises(QueueError, match="real number"):
        app.task("odd", **options)(lambda: None)


@pytest.mark.parametrize("options", [{"poll": float("nan")}, {"poll": float("inf")}, {"jitter": float("nan")}, {"grace": float("nan")}, {"grace": float("inf")}])
def test_a_span_a_worker_could_never_be_worked_out_from_is_refused_where_it_is_built(app, options):
    """the same hole one object along: a jitter of `nan` draws a delay of `nan`, which raises where the ending of a run is being written, and a grace of `infinity` is a shutdown that never comes back while anything is in flight"""
    with pytest.raises(QueueError, match="real number"):
        Worker(app, **options)


@pytest.mark.parametrize("options", [{"retry_delay": float(WIDEST_SECONDS)}, {"max_retry_delay": float(WIDEST_SECONDS)}])
async def test_a_wait_no_instant_could_be_worked_out_from_is_refused_where_it_is_declared(app, options):
    """a wait is added to `now()` and an interval is counted from the epoch, and both were measured against the whole span between the two — so every wait between what is left of the range and the whole of it was accepted where it was declared and raised where it was taken. that is the very failure this guard exists for: the ending never reached the store, the run stayed claimed, and the lease ran the very same handler again on every one of them until the attempts were spent"""
    ran = []

    @app.task("broken", max_attempts=1, retry_delay=0)
    async def broken():
        ran.append(1)

        raise RuntimeError("it never works")

    # what the wait below is added to, and what the bound it was measured against never accounted for
    with pytest.raises(OverflowError):
        now() + timedelta(seconds=WIDEST_SECONDS)

    with pytest.raises(QueueError, match="the last instant a datetime holds"):
        app.task("millennial", **options)(lambda: None)

    written = await app.enqueue("broken")
    worker = Worker(app, poll=0.05)

    await worker.run_once()
    await wait_until(lambda: worker.free == worker.concurrency)

    settled = await app.get(written.id)

    assert (settled.status, settled.error_type) == (RunStatus.FAILED, "RuntimeError"), "while a wait an instant can be worked out from is one that ends the run where it failed"
    assert ran == [1]


async def test_a_handler_that_changes_the_answer_it_gave_does_not_change_the_row(app):
    """every store writes the answer out and keeps nothing of the object, and the one that kept it let a handler edit a finished run from the outside — the very thing the run itself has been copied against since the beginning, on the one field that reaches a store after the run was written"""

    @app.task("work")
    async def work():
        return None

    written = await app.enqueue("work")
    taken = await app.store.claim("a-machine", ("default",), 1, timedelta(seconds=60), now())
    answered = {"files": ["one"]}

    await app.store.complete(taken[0].id, "a-machine", 1, now(), answered)

    answered["files"].append("two")

    assert (await app.get(written.id)).result == {"files": ["one"]}


async def test_a_lease_no_instant_could_be_worked_out_from_is_refused_where_the_worker_is_built(app):
    """a lease is added to the instant every claim and every beat is made at, and only the near end of it was ever answered for. one past what is left of the range raised inside the claim itself, where the pass logs it and waits and asks again — a worker that comes up, says so once a second and never takes anything at all"""

    @app.task("work")
    async def work():
        return None

    with pytest.raises(QueueError, match="the last instant a datetime holds"):
        Worker(app, lease=timedelta(days=999999999))

    await app.enqueue("work")

    worker = Worker(app, lease=timedelta(seconds=60))

    assert len(await worker.run_once()) == 1, "while a lease the instant it names exists for is one a claim is made with"

    await worker.drain()


@pytest.mark.parametrize("options", [{"max_attempts": 0}, {"max_attempts": 1.0}, {"timeout": 0.0}, {"timeout": float("nan")}, {"retry_delay": -1.0}, {"retry_delay": float("nan")}, {"max_retry_delay": 0.0}, {"max_retry_delay": float("inf")}])
async def test_a_task_written_by_hand_is_answered_for_by_everything_the_decorator_is(app, options):
    """`Task` is a frozen dataclass anybody may build, and half of what a run is tried and waited under was answered for by the decorator and by nothing under it. registered this way a wait of `nan` went through every guard there was and reached the arithmetic instead: it raised where the ending of a run was being written, so the ending never reached the store, the run stayed claimed, and the lease ran the very same handler again on every one of them until the attempts were spent"""
    with pytest.raises(QueueError):
        app.register(Task(name="broken", handler=lambda: None, **options))


async def test_an_answer_that_holds_itself_ends_the_run_where_it_happened(app):
    """the escaping walks the answer before anything writes it, and what a structure holding itself raises there is a `RecursionError` — which is neither of the two refusals the ending was caught by. it went straight past them: the run stayed claimed, and the lease ran the very same handler again on every one of them until the attempts were spent, under a message about a worker that stopped answering"""
    ran = []

    @app.task("odd", max_attempts=3)
    async def odd():
        ran.append(1)
        answer = {}
        answer["itself"] = answer

        return answer

    written = await app.enqueue("odd")
    worker = Worker(app, poll=0.05)

    await worker.run_once()
    await wait_until(lambda: worker.free == worker.concurrency)

    settled = await app.get(written.id)

    assert settled.status == RunStatus.FAILED, "it ended where it happened, instead of waiting on a lease to close it as a run nobody answered for"
    assert settled.error_type == "UnwritableAnswer", "and it says what broke, instead of naming a worker that never stopped answering"
    assert (ran, settled.attempts) == ([1], 1), "and the side effect happened once, because no attempt makes a handler answer something else"


@pytest.mark.parametrize("when", [None, "2026-08-01T10:00:00+00:00", 1785535200, date(2026, 8, 1)])
async def test_an_instant_no_store_could_write_a_run_down_at_is_refused_where_the_call_is_made(app, when):
    """the instant a run comes due was the one field of it settled nowhere. a nullable column handed straight to `enqueue_at` was taken by memory without a word and refused by every real store deep inside a driver — and what memory then cost was not the one run: every claim it met raised comparing nothing to the moment, so every worker of the fleet polled, logged the pass and never took anything again"""

    @app.task("work")
    async def work():
        return None

    with pytest.raises(QueueError, match="a run comes due at an instant"):
        await app.enqueue_at("work", when)

    assert await app.count() == 0, "and nothing of it reached the store"


async def test_a_run_due_at_an_instant_is_still_written_and_claimed(app):
    """the guard above refuses what is not an instant and must go on settling what is"""

    @app.task("work")
    async def work():
        return None

    written = await app.enqueue_at("work", datetime(2026, 8, 1, 10, tzinfo=timezone.utc))

    assert (await app.get(written.id)).due_at == datetime(2026, 8, 1, 10, tzinfo=timezone.utc)
    assert (await app.enqueue_at("work", datetime(2026, 8, 1, 10))).due_at == datetime(2026, 8, 1, 10, tzinfo=timezone.utc), "and a naive one is still the utc instant it reads as"


UNWRITABLE = ["sur\udce9rogate", "lone\ud800pair", "null\x00byte"]


@pytest.mark.parametrize("text", UNWRITABLE)
async def test_a_key_no_store_could_write_down_is_refused_where_the_call_is_made(app, text):
    """a key is measured against the column and was never asked whether a store could write it at all. a lone surrogate is what a posix path carries when the bytes behind it were never utf-8, and a key built out of one was taken by memory and refused by sqlite, mysql, postgres and redis each in their own way — a nul byte is one postgres refuses on its own. it is refused and never escaped, because a key is what makes a run single and escaping one folds two callers into a single row"""

    @app.task("work")
    async def work():
        return None

    with pytest.raises(QueueError, match="no store can write down|holds a nul byte"):
        await app.enqueue("work", key=f"invoice-{text}")

    assert await app.count() == 0


@pytest.mark.parametrize("text", UNWRITABLE)
def test_a_name_and_a_queue_no_store_could_write_down_are_refused_where_the_task_is_declared(app, text):
    """the same divergence one field along: the name travels in every store and the queue is in every claim, and both were only ever measured for length"""
    with pytest.raises(QueueError, match="no store can write down|holds a nul byte"):
        app.task(f"work-{text}")(lambda: None)

    with pytest.raises(QueueError, match="no store can write down|holds a nul byte"):
        app.task("work", queue=f"q-{text}")(lambda: None)


@pytest.mark.parametrize("text", UNWRITABLE)
def test_a_worker_serving_a_queue_no_store_could_write_down_is_refused_where_it_is_built(app, text):
    """the name goes into every claim this worker makes, so one a driver refuses is a worker that comes up, polls, says so once a second and never takes anything at all"""
    with pytest.raises(QueueError, match="no store can write down|holds a nul byte"):
        Worker(app, queues=(f"q-{text}",))


def test_a_name_that_is_awkward_but_writable_is_still_declared(app):
    """the guard refuses what no store can hold and must go on taking what every one of them can"""
    assert app.task("grüßen 😀", queue="münchen")(lambda: None)


async def test_a_handler_that_is_a_generator_is_refused_where_it_is_declared(app):
    """calling a generator runs none of its body: the worker was handed the generator object itself, kept nothing of it because it is not a mapping, and closed the run as done — every run of it, with the work never started and nothing anywhere saying so"""
    with pytest.raises(QueueError, match="is a generator"):

        @app.task("chunks")
        def chunks():
            yield 1

    with pytest.raises(QueueError, match="is a generator"):

        @app.task("streamed")
        async def streamed():
            yield 1

    assert app.tasks == {}


async def test_a_handler_whose_call_is_a_generator_is_refused_like_every_other_generator(app):
    """a callable object is a handler this library takes, and what runs when one is called is its `__call__`. asked about the object itself, `inspect` read the code of an instance — which has none — so the gate answered no, the worker was handed the generator that call built, kept nothing of it because it is not a mapping, and closed the run as done with the body never entered. every run of it, with nothing anywhere saying so"""

    class Chunks:
        def __call__(self):
            yield 1

    class Streamed:
        async def __call__(self):
            yield 1

    with pytest.raises(QueueError, match="is a generator"):
        app.register(Task(name="chunks", handler=Chunks()))

    with pytest.raises(QueueError, match="is a generator"):
        app.register(Task(name="streamed", handler=Streamed()))

    assert app.tasks == {}

    # and the gate reads the `__call__` to answer for it, so every object whose call returns is one this library goes on taking
    answered = []

    class Answers:
        def __call__(self, marker):
            answered.append(marker)

    app.register(Task(name="answers", handler=Answers()))
    await app.enqueue("answers", marker="one")

    worker = Worker(app, poll=0.01)
    await worker.run_once()
    await worker.drain()

    assert answered == ["one"]


async def test_a_pass_makes_its_claim_however_the_housekeeping_before_it_went(app):
    """reclaiming and writing the slots that came due are housekeeping, and only pruning was ever held to it. the write that puts the next slot in the store is the one every worker of a fleet aims at the same key in the same instant, and one burst of that outlasting the retry budget ended the pass of every one of them — a fleet that claims nothing for a second, for a reason nothing in the queue explains"""
    ran = []

    @app.task("work")
    async def work():
        ran.append(1)

    @app.task("tick", every=3600)
    async def tick():
        return None

    await app.enqueue("work")

    for name in ("reclaim", "add"):
        original = getattr(type(app.store), name)

        async def refusing(self, *arguments, **options):
            raise ConnectionError("the store refused the housekeeping")

        setattr(type(app.store), name, refusing)

        try:
            worker = Worker(app, keep=None)

            assert len(await worker.run_once()) == 1, f"a pass whose {name} was refused never made the claim it was on its way to make"

            await worker.drain()
        finally:
            setattr(type(app.store), name, original)

        assert ran == [1]
        ran.clear()
        await app.enqueue("work")


async def test_a_key_freed_by_a_pruning_between_the_write_and_the_read_is_still_written(app):
    """writing and finding are two calls with the world between them: the key was refused as taken, and the pruning that dropped the run holding it landed before the read went looking. the caller was handed `None` where a run was owed — an `AttributeError` on the very next line, from a call whose whole promise is that it answers with a run"""

    @app.task("work")
    async def work():
        return None

    settled = await app.enqueue("work", key="welcome:42")

    await app.store.claim("a-machine", ("default",), 1, timedelta(seconds=60), now())
    await app.store.complete(settled.id, "a-machine", 1, now() - timedelta(days=8), None)

    original = type(app.store).find
    asked = []

    async def pruning(self, key):
        asked.append(key)

        # the poda of a worker beside this one, landing between the write that was refused and the read that follows it
        await self.purge(now(), PURGE_LIMIT)

        return await original(self, key)

    type(app.store).find = pruning

    try:
        written = await app.enqueue("work", key="welcome:42")
    finally:
        type(app.store).find = original

    assert asked == ["welcome:42"], "the write was refused once, which is what sends it looking"
    assert written is not None, "and it came back with a run instead of nothing"
    assert written.id != settled.id
    assert (await app.find("welcome:42")).id == written.id


async def test_a_store_that_says_a_key_is_both_taken_and_held_by_nobody_is_not_asked_for_ever(app):
    """the two answers cannot both be true, and a store giving them together would spin the enqueue of whoever called it until the process was killed"""

    @app.task("work")
    async def work():
        return None

    asked = []

    async def refuse(run):
        asked.append(run.key)

        return None

    async def nothing(key):
        return None

    app.store.add = refuse
    app.store.find = nothing

    with pytest.raises(QueueError, match="held by nobody"):
        await app.enqueue("work", key="welcome:42")

    assert len(asked) == REWRITES, "it stopped at the bound instead of asking for ever"


async def test_a_plain_handler_is_never_written_down_as_too_slow_before_it_has_started(app):
    """a plain handler runs in a thread, and the pool asyncio hands out by default holds min(32, cores + 4) of them for the whole process — six on a two core container, under the eight runs a worker holds by default. the runs past it waited in a queue while the timeout, which starts where the run was claimed and not where the handler did, ran out on work that had never begun: the run was written down as having taken too long, its attempt was spent, and the handler was never called at all"""
    started = []

    # every handler waits for all of them, so a worker one thread short is a run that never starts at all rather than one that starts late — which is the same thing read off a clock, and a margin a loaded runner closes
    gathered = threading.Barrier(6, timeout=20.0)

    @app.task("plain", timeout=3.0, max_attempts=1)
    def plain(marker):
        started.append(marker)
        gathered.wait()

    # the pool a small container is given, which holds fewer threads than this worker holds runs
    asyncio.get_running_loop().set_default_executor(ThreadPoolExecutor(max_workers=2))

    ids = [(await app.enqueue("plain", marker=marker)).id for marker in range(6)]

    worker = Worker(app, concurrency=6)

    assert len(await worker.run_once()) == 6

    await worker.drain()

    assert sorted(started) == list(range(6)), "every handler this worker claimed was actually called"
    assert [(await app.get(run_id)).status for run_id in ids] == [RunStatus.DONE] * 6, "and none of them was recorded as too slow while it sat in a queue"


async def nothing_left(app) -> bool:
    """every run written is over, which is what both of the threading tests below wait on rather than on a count of handlers that may never be called"""
    return await app.count(status=RunStatus.PENDING) == 0 and await app.count(status=RunStatus.RUNNING) == 0


async def broke(app, wanted: int) -> bool:
    return await app.count(status=RunStatus.FAILED) == wanted


async def test_a_thread_a_handler_ran_past_its_timeout_on_is_not_a_slot_the_worker_claims_into(app):
    """a timeout ends the waiting and only a coroutine ends the work, so a plain handler that ran past one carries on to its own end and keeps its thread — while the slot it was claimed into was handed straight back. the worker then claimed a run onto a pool with nothing left to run it on, and that run waited there for a thread while its own timeout, which starts where the work was handed over, ran out on work that never began: written down as having taken too long, its attempt spent, and its handler never entered. counting the thread instead of the slot is what makes the worker claim what it can actually run"""
    entered = []

    # four handlers that each run four times their timeout and then return, which is one stranded thread per run this worker holds
    @app.task("stalls", timeout=0.3, max_attempts=1)
    def stalls():
        entered.append("stalls")
        time.sleep(1.2)

    @app.task("quick", timeout=0.3, max_attempts=1)
    def quick(marker):
        entered.append(marker)

    worker = Worker(app, concurrency=4, poll=0.05)

    for slot in range(4):
        await app.enqueue("stalls", key=f"stalls-{slot}")

    for marker in range(12):
        await app.enqueue("quick", key=f"quick-{marker}", marker=marker)

    polling = asyncio.create_task(worker.run())

    await wait_until(lambda: nothing_left(app))

    worker.stop()
    await polling

    assert sorted(marker for marker in entered if marker != "stalls") == list(range(12)), "every run this worker claimed reached its handler"
    assert await app.count(status=RunStatus.DONE) == 12, "and none of them was recorded as too slow while it waited for a thread that was never coming"


async def test_a_worker_whose_threads_are_all_stranded_leaves_the_work_in_the_queue(app):
    """a handler that never returns keeps its thread for as long as the process lives, so a worker that met one of those for every thread it has cannot run a plain handler again. claiming anyway spent the attempt of every run it took and wrote each of them down as having taken too long, which is a worker that looks busy while it destroys the queue — and with `max_attempts` at one that is the work gone. left where it is, the run waits for a worker that can actually run it"""
    entered = []
    held = threading.Event()

    @app.task("hangs", timeout=0.3, max_attempts=1)
    def hangs():
        entered.append("hangs")
        held.wait()

    @app.task("waits", timeout=0.3, max_attempts=1)
    def waits():
        entered.append("waits")

    worker = Worker(app, concurrency=2, poll=0.05)

    for slot in range(2):
        await app.enqueue("hangs", key=f"hangs-{slot}")

    polling = asyncio.create_task(worker.run())

    try:
        # both of them past their timeout and written down, which is the moment the worker has stopped waiting for two threads it is never getting back
        await wait_until(lambda: broke(app, 2))

        left = [(await app.enqueue("waits", key=f"waits-{marker}")).id for marker in range(3)]

        await asyncio.sleep(0.5)

        worker.stop()
        await polling

        assert entered == ["hangs", "hangs"], "the worker claimed nothing it had no thread for"
        assert [(await app.get(run_id)).status for run_id in left] == [RunStatus.PENDING] * 3, "and what it could not run was left in the queue instead of written down as too slow"
    finally:
        held.set()


async def test_a_run_still_queued_for_a_thread_never_spends_the_attempt_it_never_used(app):
    """a timeout starts where the work is handed over and not where a thread picks it up, so one shorter than that handover is a run written down as having taken too long with no line of its handler ever run. the attempt was spent on nothing, and with `max_attempts` at one that is the work gone — while the message named the handler, which an operator then reads as code being slow"""
    entered = []
    told = []
    holding = threading.Event()

    @app.task("never-starts", timeout=0.05, max_attempts=1)
    def never_starts():
        entered.append("never-starts")

    run = await app.enqueue("never-starts", key="the-queued-one")
    worker = Worker(app, concurrency=1)

    worker.on_error(lambda held, error, took, coming_back: told.append((type(error).__name__, coming_back)))

    # every thread this worker has, busy with something that is not a run, so the work below can only wait in the queue for one
    for _ in range(worker.concurrency):
        worker.threads.submit(holding.wait)

    try:
        assert len(await worker.run_once()) == 1

        await worker.drain()
    finally:
        holding.set()

    assert entered == [], "no line of the handler ran, which is the whole of what went wrong"
    assert told == [("WorkNeverStarted", True)], "and it is not written down under the name of a handler that was too slow"

    held = await app.get(run.id)

    assert held.status == RunStatus.PENDING, "the run waits for a worker with a thread to spare instead of being failed outright"
    assert held.attempts == 0, "and the attempt it never used is one it still has"


async def test_a_handler_that_times_out_on_its_own_is_not_read_as_one_that_never_ran(app):
    """what a handler raises is code being run, and `TimeoutError` is what a socket, a driver or a request of its own raises. read as the wait this worker gave up on, a handler that ran the whole of its work and then said so would have its attempt handed back for ever and repeat everything it did before it raised"""
    entered = []

    @app.task("own-timeout", max_attempts=1)
    def own_timeout():
        entered.append("own-timeout")

        raise TimeoutError("the socket of this handler gave up")

    run = await app.enqueue("own-timeout", key="the-honest-one")
    worker = Worker(app, concurrency=1)

    assert len(await worker.run_once()) == 1

    await worker.drain()

    held = await app.get(run.id)

    assert entered == ["own-timeout"], "the handler ran"
    assert held.status == RunStatus.FAILED, "so what it raised is the ending of the run and never a wait nobody was serving"
    assert held.attempts == 1, "and the attempt it used is one it spent"


def test_a_worker_holds_a_thread_for_every_run_it_may_hold(app):
    """the pool is the worker's own and sized to what it claims, because the one asyncio shares out is sized for the whole process and for everything else in it"""
    assert Worker(app, concurrency=12).threads._max_workers == 12

    # and they are made only as they are needed, so a worker whose handlers are all coroutines makes none
    assert Worker(app, concurrency=12).threads._threads == set()


@pytest.mark.parametrize("payload", [[], "", 0, [1, 2], "hello"])
async def test_arguments_that_are_not_a_mapping_are_refused_where_the_call_is_made(app, payload):
    """a handler is called with keyword arguments, so a payload that is not a mapping could never be called at all — and the stores did not even agree on what it was. memory and redis read back the list, the empty string and the zero they were handed where a database read all three back as no arguments, so the same call meant two different things depending on where the runs lived, and every attempt of it was spent on a `TypeError` nobody asked for"""

    @app.task("work")
    async def work(**arguments):
        return None

    with pytest.raises(QueueError, match="the arguments of a run are a mapping"):
        await app.enqueue("work", payload=payload)

    assert await app.count() == 0, "and nothing was written on the way to finding that out"


async def test_arguments_a_store_was_handed_are_read_back_as_the_very_same_thing(app):
    """the read this store answers a run with turned anything falsy into no arguments at all, so a payload of `[]`, `''` or `0` came back `{}` where memory and redis came back with what they were given. the guard above is what stops one being written, and this is what stops the read quietly rewriting one that was"""
    for payload in ([], "", 0, {"ok": 1}):
        written = await app.store.add(Run(name="work", payload=payload))

        assert (await app.store.get(written.id)).payload == payload, "what a store was handed is what it reads back"


@pytest.mark.parametrize("value", [2**64, -(2**63) - 1, 10**40])
async def test_a_whole_number_no_store_keeps_inside_json_is_refused_where_it_is_written(app, value):
    """mysql keeps a whole number inside a json value for as long as it fits a 64 bit integer and turns everything either side of that into a double, so a payload of `10**40` was read back off it as 9.999999999999998e+39 while memory, sqlite, postgres and redis every one of them read back the number that was written. it is the one divergence of the arguments that says nothing at all when it happens: the handler is called on a value nobody enqueued, no store refuses anything and no line anywhere records it"""

    @app.task("work")
    async def work(**arguments):
        return None

    with pytest.raises(QueueError, match="past the whole numbers a store keeps inside json"):
        await app.enqueue("work", n=value)

    assert await app.count() == 0, "and nothing was written on the way to finding that out"


@pytest.mark.parametrize("value", [2**63 - 1, 2**64 - 1, -(2**63), True, 1.5])
async def test_a_whole_number_every_store_keeps_is_still_written_and_read_back(app, value):
    """the guard refuses what mysql would answer with another number for and must go on taking every number it holds, both ends of the range included — and a boolean, which is an `int` to python and `true` to every reader of json"""

    @app.task("work")
    async def work(**arguments):
        return None

    written = await app.enqueue("work", n=value)

    assert (await app.get(written.id)).payload == {"n": value}


async def test_arguments_keyed_by_a_whole_number_that_wide_are_settled_and_not_refused(app):
    """json names every key with a string, so a payload keyed by a number past the range is settled into one every store already agrees on — and refusing it would be the guard turning down what nothing was ever wrong with"""

    @app.task("work")
    async def work(**arguments):
        return None

    written = await app.enqueue("work", payload={10**40: "a"})

    assert written.payload == {"10000000000000000000000000000000000000000": "a"}
    assert (await app.get(written.id)).payload == written.payload


async def test_an_answer_holding_a_whole_number_no_store_keeps_ends_the_run_where_it_happened(app):
    """the answer reaches the very same json column the arguments do, and no attempt makes a handler answer a different number. left to the store it is not an ending that never lands but a result nobody can tell was changed, which is worse: the run is written down as done and what it is closed with is a number the handler never gave"""
    broke = []

    @app.task("work", max_attempts=3)
    async def work():
        return {"total": 10**40}

    worker = Worker(app)
    worker.on_error(lambda run, error, took, coming_back: broke.append((type(error).__name__, coming_back)))

    written = await app.enqueue("work")

    await worker.run_once()
    await worker.drain()

    held = await app.get(written.id)

    assert held.status == RunStatus.FAILED, "the run ended where it happened, instead of being tried again for an answer that never changes"
    assert held.error_type == "UnwritableAnswer"
    assert broke == [("UnwritableAnswer", False)]


@pytest.mark.parametrize("text", UNWRITABLE)
def test_a_worker_named_something_no_store_could_write_down_is_refused_where_it_is_built(app, text):
    """the name is in every claim, every beat and every ending a worker writes, exactly as the queues it serves are — and it was only ever measured for length. it is not a name anybody had to type either: `worker_name` builds it out of `socket.gethostname()`, which carries whatever bytes the host had, and python reads the ones that were never utf-8 back as lone surrogates. memory claimed under it without a word while every real store refused it deep inside a driver, so what came up was a worker that logged the pass, waited and asked again, and never took anything at all"""
    with pytest.raises(QueueError, match="no store can write down|holds a nul byte"):
        Worker(app, name=f"machine-{text}")


async def test_a_span_a_store_was_handed_is_read_back_as_the_very_same_span(app):
    """the columns holding a timeout and the two retry delays were the generic float, which is a single precision `FLOAT` on mysql where a python float is a double. a timeout of 12345.678 came back 12345.7 and one of 3599.999999 came back 3600.0, while memory, sqlite, postgres and redis every one of them read back the number that was written — and past what a single holds it was not read back changed but refused outright, which is the same call writing a run on four stores and raising on the fifth"""
    for value in (12345.678, 3599.999999, 0.30000000000000004, 1e300):
        written = await app.store.add(Run(name="work", timeout=value, retry_delay=value, max_retry_delay=value))
        read = await app.store.get(written.id)

        assert (read.timeout, read.retry_delay, read.max_retry_delay) == (value, value, value), "a span a store forgot the end of is a policy the worker silently stops honouring"


async def test_the_attempt_a_worker_lost_never_answers_for_the_one_it_is_running_now(app):
    """a pass reclaims what expired and then claims it, so a worker whose beat never landed meets its own run again under its own name — and both of the attempts it is then running answered to a condition written on that name alone. the one that lost the run put it back in the queue while the one still working it was mid handler, which is a third worker claiming it and the very same work multiplying with every lease"""
    gates = []

    @app.task("slow", max_attempts=5, retry_delay=0.01)
    async def slow():
        gate = asyncio.Event()
        gates.append(gate)

        await gate.wait()

        raise RuntimeError("boom")

    # the beat never lands, which is a store that blinked, so the lease runs out under a handler that is still working
    async def unheard(*arguments):
        return False

    written = await app.enqueue("slow")
    app.store.heartbeat = unheard

    worker = Worker(app, lease=timedelta(milliseconds=50))

    await worker.run_once()
    await wait_until(lambda: len(gates) == 1)
    await asyncio.sleep(0.1)

    # the very same worker takes its own expired lease back and claims the very same run again
    await worker.run_once()
    await wait_until(lambda: len(gates) == 2)

    gates[0].set()
    await wait_until(lambda: len(worker.running) == 1)

    still_running = await app.get(written.id)

    assert still_running.status == RunStatus.RUNNING, "the attempt that lost the run put it back in the queue while the attempt that holds it was still working"
    assert still_running.attempts == 2

    gates[1].set()
    await worker.drain()

    ended = await app.get(written.id)

    assert (ended.status, ended.attempts) == (RunStatus.PENDING, 2), "and the attempt that does hold it is the one that hands it back"


# text that reads alike to a collation that folds case or accents away, and that every one of these stores has to keep apart: a key built out of an address, a name or a filename is what a caller writes one from
ALIKE = (("slot@A", "slot@a"), ("invoice:Bob@shop.com", "invoice:bob@shop.com"), ("user:José", "user:Jose"))


@pytest.mark.parametrize("first, second", ALIKE)
async def test_two_keys_that_only_read_alike_are_two_runs_and_never_one(app, first, second):
    """the column mysql builds by default is `utf8mb4_0900_ai_ci`, which folds case and accents away before it compares — so the unique index behind a key held 'invoice:Bob@shop.com' and 'invoice:bob@shop.com' to be the same occurrence. the second caller was handed the run of the first, its own was never written, and the work of it never happened. memory, sqlite, postgres and redis every one of them keep the two apart"""
    app.register(Task(name="work", handler=lambda: None))

    one = await app.enqueue("work", key=first)
    two = await app.enqueue("work", key=second)

    assert one.id != two.id, "a key is what makes a run single, and two keys that are not the same key are two runs"
    assert (await app.find(first)).key == first
    assert (await app.find(second)).key == second


async def test_a_worker_never_claims_from_a_queue_it_was_not_given(app):
    """the queue of a run is in every claim a worker makes, and mysql compared it under a collation that folds case away — so a worker serving 'mail' claimed and ran what was written to 'Mail', which is two applications sharing a database taking each other's work"""
    app.register(Task(name="urgent", handler=lambda: None, queue="Mail"))

    await app.enqueue("urgent")

    assert await Worker(app, queues=("mail",)).run_once() == [], "a queue this worker was never given is not one it serves"
    assert await app.count(queue="Mail") == 1
    assert await app.count(queue="mail") == 0


async def test_a_worker_never_answers_for_the_attempt_a_name_that_reads_alike_holds(app):
    """what says a run is this worker's is the name and the attempt together, and mysql compared that name under a collation that folds case away — so a second worker closed, and put back in the queue, a run the first was still working"""
    written = await app.store.add(Run(name="work", due_at=now()))
    taken = await app.store.claim("Machine-A", ("default",), 1, timedelta(seconds=60), now())

    assert [run.id for run in taken] == [written.id]

    assert not await app.store.complete(written.id, "machine-a", taken[0].attempts, now(), None), "a name that is not this worker's name never closes its run"
    assert not await app.store.retry_later(written.id, "machine-a", taken[0].attempts, now(), "boom", "Error")
    assert not await app.store.heartbeat(written.id, "machine-a", taken[0].attempts, timedelta(seconds=60), now())
    assert (await app.get(written.id)).status == RunStatus.RUNNING


def pooled(worker: Worker) -> int:
    """the threads that worker made for itself, counted by the name it gives them — a driver of its own runs threads too, and those are not what this is about"""
    return len([thread for thread in threading.enumerate() if thread.name.startswith(f"queuefy-{worker.name}")])


async def test_a_worker_that_stopped_leaves_no_thread_of_its_own_behind(app):
    """a worker holds a pool of its own, one thread per run it may hold, because the one asyncio shares with the whole process is too small for it. those threads wait on that pool for ever, so a worker that came and went left every one of them alive — an api running a worker in its lifespan and coming back up on a reload holds both sets until the process goes"""

    @app.task("plain")
    def plain():
        return {"ok": True}

    for _ in range(3):
        await app.enqueue("plain")

    worker = Worker(app, concurrency=3, poll=0.01)
    polling = asyncio.create_task(worker.run())

    await wait_until(lambda: app.count(RunStatus.DONE))

    assert pooled(worker) > 0, "the plain handler ran on a thread of this worker's, which is what there is to leave behind"

    worker.stop()
    await polling

    # bounded short, because a pool told to close wakes at once every thread waiting on it, and one still there a moment later is one nothing ever closes
    with suppress(TimeoutError):
        await wait_until(lambda: pooled(worker) == 0, patience=5.0)

    assert pooled(worker) == 0, "the pool belongs to the worker, so what is left of a worker that stopped is nothing"


async def test_a_worker_whose_polling_was_cancelled_leaves_no_thread_of_its_own_behind(app):
    """being cancelled is how a worker is stopped as often as being asked: a task group, a supervisor and `asyncio.run` with the polling still pending every one of them cancels rather than asks. the close of the pool sat after the drain, so a cancellation went straight past it and left every thread the worker made for its plain handlers alive for as long as the process was — an api whose lifespan is cancelled on a slow shutdown holds one more set on every reload"""

    @app.task("plain")
    def plain():
        return {"ok": True}

    for _ in range(3):
        await app.enqueue("plain")

    worker = Worker(app, concurrency=3, poll=0.01)
    polling = asyncio.create_task(worker.run())

    await wait_until(lambda: app.count(RunStatus.DONE))

    assert pooled(worker) > 0, "the plain handler ran on a thread of this worker's, which is what there is to leave behind"

    polling.cancel()

    with pytest.raises(asyncio.CancelledError):
        await polling

    with suppress(TimeoutError):
        await wait_until(lambda: pooled(worker) == 0, patience=5.0)

    assert pooled(worker) == 0, "a worker that was cancelled is as gone as one that was asked, and the threads go with it either way"


async def test_the_last_instant_a_datetime_holds_is_read_back_as_the_instant_it_was_written(app):
    """redis held an instant as the seconds since the epoch, which `timestamp()` hands back as a double — and past the microseconds a double names, the last instant a datetime holds came back as the first of the year after it. every read of that run raised, the claim that built it raised with it, and the runs taken beside it in the same batch went back on their leases: a run written once, never worked, and taking a pass of a worker down with it on every lease from then on"""
    for moment in (WIDEST_SLOT, EPOCH, datetime(1, 1, 1, tzinfo=timezone.utc), datetime(2112, 6, 1, 12, 0, 0, 123456, tzinfo=timezone.utc)):
        written = await app.store.add(Run(name="work", key=moment.isoformat(), due_at=moment, created_at=moment, lease_until=moment, started_at=moment, finished_at=moment))
        read = await app.store.get(written.id)

        assert (read.due_at, read.created_at, read.lease_until, read.started_at, read.finished_at) == (moment, moment, moment, moment, moment), "an instant a store cannot read back is a run nothing can ever build again"


async def test_a_lease_runs_out_at_the_instant_the_claim_that_made_it_said(app):
    """a lease is measured against what is left of the range, so the widest one a worker may hold is one every guard allows — and it puts the instant it runs out far enough out that a double no longer names the microsecond. redis read that instant back as another, which is a run taken back before or after the lease anybody wrote"""
    app.register(Task(name="work", handler=lambda: None))

    written = await app.enqueue("work")
    lease = timedelta(seconds=WIDEST_SECONDS - int((now() - EPOCH).total_seconds()) - 1)
    moment = now()

    taken = await app.store.claim("a-worker", ("default",), 10, lease, moment)

    assert [run.id for run in taken] == [written.id]
    assert taken[0].lease_until == moment + lease, "the lease runs out where the claim put it, and never where a double happened to land"
    assert (await app.get(written.id)).lease_until == moment + lease


def test_a_span_to_keep_a_run_for_that_names_no_instant_is_refused_where_the_worker_is_built(app):
    """the span is taken off the instant every pruning is asked at, and it was only ever measured for being negative. one past what lies behind now was accepted where it was written and raised inside the pruning — on the hour, every hour, while nothing that was over was ever dropped and the queue grew for ever"""
    with pytest.raises(QueueError, match="no datetime holds"):
        Worker(app, keep=timedelta.max)


async def test_a_worker_keeping_a_run_for_a_span_it_can_prune_by_still_prunes(app):
    """the guard is what lies behind now, and a century of keeping is well inside it"""
    worker = Worker(app, keep=timedelta(days=36500))

    assert await worker.tidy(now()) == 0, "the pruning was asked and answered, instead of raising where the span was taken"


async def test_a_trigger_that_could_never_say_when_is_refused_where_the_task_is_declared(app):
    """the one field `register` never answered for. what writes the next slot is read on every pass of every worker, so one that cannot answer raised inside the pass and not on a run of the task — the pass carried on, the task never fired, and `materialize` walks the tasks in order, so every recurring task declared after it was skipped along with it on every poll for ever"""
    with pytest.raises(QueueError, match="what says when a task comes round again is a Trigger"):
        app.register(Task(name="broken", handler=lambda: None, trigger="every 5 seconds"))

    app.register(Task(name="hourly", handler=lambda: None, trigger=Interval(1)))

    assert len(await app.materialize()) == 1, "the task that was declared is the task that fires, and nothing declared beside it takes the pass down"


@pytest.mark.parametrize("key", [None, "", 41])
async def test_a_name_no_run_could_have_been_written_under_is_refused_where_it_is_looked_for(app, key):
    """the key was answered for where a run is written under one and nowhere at all where one is looked for, and a caller reading a key off a request that did not carry it asks with nothing. memory compared that to the key of every run and answered with the first written under none, sqlalchemy read it as `IS NULL` and raised the moment two runs had no key between them, and redis went looking for a key spelled after it — three stores, three answers, and an application handed somebody else's run by where its runs happen to live"""
    app.register(Task(name="work", handler=lambda: None))

    await app.enqueue("work")

    named = await app.enqueue("work", key="invoice-7")

    with pytest.raises(QueueError):
        await app.find(key)

    assert (await app.find("invoice-7")).id == named.id, "and the name a run really was written under still finds it"


async def test_a_key_that_is_not_text_is_refused_by_the_gate_every_other_key_passes(app):
    """the same guard on the way in. a key that is not text reached the encoding and raised an `AttributeError` out of it, which is not one of the failures this library names — so `except QueueError` around an enqueue caught everything except this"""
    app.register(Task(name="work", handler=lambda: None))

    with pytest.raises(QueueError, match="what tells one run from another is text"):
        await app.enqueue("work", key=41)

    assert await app.count() == 0, "nothing was written under a name no store could have told apart"


@pytest.mark.parametrize("options,reason", [({"timeout": True}, "keeps a span as text"), ({"retry_delay": True}, "keeps a span as text"), ({"max_retry_delay": True}, "keeps a span as text")])
def test_a_span_no_store_could_read_back_is_refused_where_it_is_declared(app, options, reason):
    """redis keeps a span as text and reads it back with `float`, so a boolean was written down as 'True' — taken where the task was declared, kept whole by memory, rounded to 1.0 by a `Double` column, and raised on by every claim of every redis worker from then on. what came up was a worker that logged the pass, waited and asked again, and never took anything at all"""
    with pytest.raises(QueueError, match=reason):
        app.task("broken", **options)(lambda: None)

    assert app.task("fine", timeout=1, retry_delay=2, max_retry_delay=2.5)(lambda: None), "and a plain number, whole or not, is one every store reads back"


async def test_a_policy_no_store_could_write_down_is_refused_where_it_is_declared(app):
    """a store writes a run out under the value of its policy and memory keeps the object it was handed, so a policy given as a plain string ran for as long as an application was built against memory and then raised on the enqueue itself everywhere the runs really live"""
    with pytest.raises(QueueError, match="is a RetryPolicy"):
        app.task("broken", retry_policy="exponential")(lambda: None)

    app.task("fine", retry_policy=RetryPolicy.FIXED)(lambda: None)

    assert await app.enqueue("fine"), "and the policy this library names is one every store writes down"


async def test_a_task_allowed_more_attempts_than_a_column_holds_is_refused_where_it_is_declared(app):
    """mysql and postgres keep that count in four bytes and refuse a write past it outright, while memory, sqlite and redis every one of them take it — so a task declared this way answered a whole suite and then raised on every enqueue of it in production"""
    with pytest.raises(QueueError, match="column holding"):
        app.task("broken", max_attempts=ATTEMPT_LIMIT + 1)(lambda: None)

    app.task("fine", max_attempts=ATTEMPT_LIMIT)(lambda: None)

    assert await app.enqueue("fine"), "and the most attempts the column holds is a count every store writes down"


async def test_a_run_written_as_running_is_not_handed_to_the_next_worker_that_asks(store):
    """a store is handed whatever run it is given, and redis put every one of them in the queue lane whatever state it carried — while a claim takes what is in a lane without ever reading a status. so a run written as running, with a lease that had not run out and a worker still working it, was claimed out from under that worker by the next one to ask, its attempt counted again and its holder's ending refused, which is the one thing none of this may ever do"""
    moment = now()
    held = await store.add(Run(name="held", key="held", status=RunStatus.RUNNING, worker="somebody-else", attempts=1, max_attempts=5, started_at=moment, lease_until=moment + timedelta(seconds=300), due_at=moment))

    assert await store.claim("me", ("default",), 5, timedelta(seconds=60), moment) == [], "a run somebody else is holding is not one this claim may take"

    standing = await store.get(held.id)

    assert (standing.status, standing.worker, standing.attempts) == (RunStatus.RUNNING, "somebody-else", 1), "and nothing about it moved"
    assert await store.complete(held.id, "somebody-else", 1, moment, {"ok": True}), "so the worker that holds it still answers for it"


async def test_a_run_written_as_over_is_never_claimed_again_and_is_still_pruned(store):
    """the same write one state further along: a run already settled went into the queue lane like any other, so redis handed it out to be run a second time — and it never reached the settled set, which is the only place a pruning reads, so it was kept for as long as the store lived"""
    moment = now()
    over = await store.add(Run(name="over", key="over", status=RunStatus.DONE, result={"ok": True}, finished_at=moment - timedelta(days=30), due_at=moment - timedelta(days=30)))

    assert await store.claim("me", ("default",), 5, timedelta(seconds=60), moment) == [], "what is over is never claimed again"
    assert await store.purge(moment, 50) == 1, "and a pruning is what drops it"
    assert await store.get(over.id) is None


@pytest.mark.parametrize("expression", [41, None, ["0", "4", "*", "*", "*"]])
def test_a_cron_that_is_not_text_is_refused_by_the_gate_every_other_trigger_passes(expression):
    """an interval answers for the type it was handed and a cron never did, so an expression that is not text reached the split and raised an `AttributeError` out of the parser — which is not one of the failures this library names, so `except QueueError` around a declaration caught every other way of getting a trigger wrong and not this one"""
    with pytest.raises(QueueError, match="the five fields are read out of is text"):
        Cron(expression)


async def test_a_task_asking_for_a_cron_that_is_not_text_is_refused_where_it_is_declared(app):
    """the decorator is what builds the trigger, so the gate has to be the one a declaration meets"""
    with pytest.raises(QueueError, match="the five fields are read out of is text"):
        app.task("broken", cron=41)(lambda: None)

    app.task("nightly", cron="0 4 * * *")(lambda: None)

    assert len(await app.materialize()) == 1, "and the expression that really is one still writes the slot it fires on"


@pytest.mark.parametrize("options", [{"name": 41}, {"queues": (41,)}, {"queues": ("default", None)}])
def test_a_worker_named_or_serving_something_that_is_not_text_is_refused_by_the_gate_every_other_name_passes(app, options):
    """the name and the queues go into every claim, every beat and every ending a worker writes, and the gate they pass through asked the encoder before it asked the type — so anything that is not text raised an `AttributeError` about a method, out of a worker being built, which is not one of the failures this library names and which `except QueueError` around a startup never caught"""
    with pytest.raises(QueueError, match="what tells one run from another is text"):
        Worker(app, **options)


@pytest.mark.parametrize("options", [{"name": 41}, {"queue": None}])
def test_a_task_named_or_queued_by_something_that_is_not_text_is_refused_where_it_is_declared(app, options):
    """the same gate, met from the other side: a name and a queue are what a store finds a run by, and one that is not text reached the encoder instead of a refusal"""
    with pytest.raises(QueueError, match="what tells one run from another is text"):
        app.register(Task(**{"name": "work", "handler": lambda: None} | options))


@pytest.mark.parametrize("options", [{"lease": 60}, {"lease": None}, {"keep": 7}, {"keep": "a week"}])
def test_a_worker_given_a_span_that_is_not_one_is_refused_where_it_is_built(app, options):
    """a lease and the span a run that is over is kept for are both timedeltas, and both were read by calling `total_seconds()` on whatever arrived — so `lease=60`, which is the mistake anybody makes once, raised an `AttributeError` about a method instead of saying that a span is a timedelta"""
    with pytest.raises(QueueError, match="a span is a timedelta"):
        Worker(app, **options)


def test_a_worker_given_the_spans_it_wants_is_still_built(app):
    """the gate refuses what is not a span, and never the spans a worker is meant to be given"""
    assert Worker(app, lease=timedelta(seconds=30), keep=timedelta(days=1)).keep == timedelta(days=1)
    assert Worker(app, keep=None).keep is None


# a driver that went away, and a pool with nothing left to hand out. the second is the one no `except DBAPIError` ever caught: a row is taken and committed one at a time, so the connection goes back to the pool between two of them and sqlalchemy raises this itself — an ordinary batch under ordinary load, with nothing having gone wrong anywhere
BREAKING = (DBAPIError("select 1", {}, Exception("the connection went away")), PoolTimeout("QueuePool limit of size 5 overflow 10 reached"))


@pytest.mark.parametrize("breaking", BREAKING)
@pytest.mark.parametrize("wins", [2, 0])
async def test_a_claim_that_broke_partway_still_hands_back_the_runs_it_already_took(app, wins, breaking):
    """a batch is taken one row at a time, and a connection that went away halfway through took every run the claim had already won with it: each of those had its attempt spent and its lease running while the worker that held them was alive and about to start them. what met them instead was the reclaim their leases ran out into, and `max_attempts` is one by default — so every one of them was failed outright under `LeaseExpired`, its handler never called and nothing anywhere saying so"""
    from queuefy.store.sqlalchemy import SqlAlchemyStore

    if not isinstance(app.store, SqlAlchemyStore):
        pytest.skip("only a store that takes its batch one row at a time can lose half of one")

    ran = []

    @app.task("work")
    async def work(marker: int):
        ran.append(marker)

    for marker in range(4):
        await app.enqueue("work", marker=marker)

    taking, asked = app.store.take, []

    async def going_away(*arguments):
        asked.append(None)

        if len(asked) > wins:
            raise breaking

        return await taking(*arguments)

    app.store.take = going_away
    worker = Worker(app)

    if not wins:
        # nothing was won, so there is nothing to lose by saying what happened
        with pytest.raises(type(breaking)):
            await worker.run_once()

        assert await app.count(RunStatus.RUNNING) == 0, "and nothing was left claimed by a worker that never got one"

        return

    claimed = await worker.run_once()

    await worker.drain()

    assert len(claimed) == wins, "the claim answered with the runs it had already taken instead of dropping them"
    assert sorted(ran) == list(range(wins)), "and the worker ran every one of them"
    assert await app.count(RunStatus.DONE) == wins, "which is what the store was told, rather than a lease running out on work nobody started"


async def test_a_row_a_claim_could_not_read_is_one_it_never_took(app):
    """the row was taken in one transaction and read in the next, so a connection that went away between them left the run claimed with its attempt spent and handed to nobody. it is the very failure a batch broken halfway through was already answered for, met one row earlier: what came for it next was the reclaim its lease ran out into, and `max_attempts` is one by default, so it was failed under `LeaseExpired` with its handler never called"""
    from queuefy.store import sqlalchemy as store_module

    if not isinstance(app.store, store_module.SqlAlchemyStore):
        pytest.skip("only a store that reads back the row it took can fail to read it")

    @app.task("work")
    async def work():
        return None

    written = await app.enqueue("work")
    reading = store_module.to_run

    def going_away(row):
        raise PoolTimeout("QueuePool limit of size 5 overflow 10 reached")

    store_module.to_run = going_away

    try:
        with pytest.raises(PoolTimeout):
            await Worker(app).run_once()
    finally:
        store_module.to_run = reading

    left = await app.get(written.id)

    assert left.status == RunStatus.PENDING, "the row this claim could not read is one it never took"
    assert left.attempts == 0, "so the attempt it would have spent is one the run still has"
    assert left.worker is None, "and nobody is holding it"


@pytest.mark.parametrize("name", [[], {}, set()])
def test_a_task_name_that_could_never_be_one_is_refused_before_it_is_looked_up(app, name):
    """the registry is keyed by the name, and the duplicate check asked it that question before anything answered for what it was. a name that cannot be a key raised a `TypeError` about hashing, out of a task being declared, under a name `except QueueError` never catches — while every other way of getting a name wrong was already refused with a sentence saying names are text"""
    with pytest.raises(QueueError, match="what tells one run from another is text"):
        app.register(Task(name=name, handler=lambda: None))


def test_a_worker_given_queues_that_empty_as_they_are_read_is_refused_where_it_is_written(app):
    """what a worker serves is read again on every claim it makes, and it was only ever walked once to answer for the names in it. a generator written where a tuple was meant was emptied by that very walk, so the worker came up, validated, and then made every claim with no queues at all — which memory, sqlalchemy and redis each answer with nothing. it polled for ever and took nothing, and the runs it was named for waited in the queue with nothing anywhere saying why"""
    with pytest.raises(QueueError, match="a tuple of names"):
        Worker(app, queues=(name for name in ("email", "reports")))


@pytest.mark.parametrize("queues", [41, 1.5, None, object()])
def test_a_worker_given_queues_that_are_not_a_sequence_at_all_says_so(app, queues):
    """the same gate one step earlier: what is not iterable never reached the walk that answers for the names, it reached the `for` behind it and raised a `TypeError` about iteration — out of a worker being built, which is not one of the failures this library names"""
    with pytest.raises(QueueError):
        Worker(app, queues=queues)


def test_a_worker_given_a_list_of_queues_serves_them(app):
    """a list is read as many times as a claim asks for it, so it is a worker and not a mistake"""
    assert Worker(app, queues=["email", "reports"]).queues == ["email", "reports"]


async def test_an_expression_the_parser_takes_is_one_the_search_can_always_answer(app):
    """the horizon was eight years, sized for the leap day. but a day field written as a star always holds the first of the month, and a star beside one month and one named weekday asks for the first of that month falling on that weekday — which the century that is not a leap year stretches to twelve years. the expression parsed where it was declared and then raised inside every pass of every worker, so the task never fired and every recurring task declared after it was skipped along with it, on every poll, for as long as the process lived"""
    app.register(Task(name="rare", handler=lambda: None, trigger=Cron("0 0 */31 2 5")))
    app.register(Task(name="hourly", handler=lambda: None, trigger=Cron("0 * * * *")))

    written = await app.materialize(datetime(2020, 2, 2, tzinfo=timezone.utc))

    assert [run.name for run in written] == ["rare", "hourly"], "the far slot is written, and the task declared after it is not taken down with it"


async def test_the_leap_day_falling_on_one_named_weekday_is_a_slot_the_search_still_reaches(app):
    """the horizon was twelve years, sized for the first of a month falling on one named weekday. but a weekday field written as a star always holds sunday, and '*/7' holds nothing else — so '0 0 29 2 */7' asks for february the 29th on a sunday, which the century that is not a leap year puts forty years apart. the expression parsed where it was declared and worked until the slot it wanted next was more than twelve years out, and from that day it raised inside every pass of every worker: the task never fired, and every recurring task declared after it was skipped along with it, on every poll, for as long as the process lived"""
    app.register(Task(name="leaping", handler=lambda: None, trigger=Cron("0 0 29 2 */7")))
    app.register(Task(name="hourly", handler=lambda: None, trigger=Cron("0 * * * *")))

    written = await app.materialize(datetime(2088, 3, 1, tzinfo=timezone.utc))

    assert [run.name for run in written] == ["leaping", "hourly"], "the far slot is written, and the task declared after it is not taken down with it"
    assert written[0].due_at == datetime(2128, 2, 29, tzinfo=timezone.utc), "which is the leap day forty years out, and the first one after 2088 that falls on a sunday"


def test_a_step_over_a_single_value_is_refused_where_it_is_written():
    """'0 9/2 * * *' parsed, and the step did nothing: the field named nine o'clock alone, so a task written for every other hour from nine ran once a day. no other cron reads it that way — some take it as the range up to the end of the field and the rest refuse it — and what came out here was neither, on a schedule nothing anywhere reports as wrong"""
    with pytest.raises(CronError, match="steps over the single value"):
        Cron("0 9/2 * * *")

    assert Cron("0 9-23/2 * * *").next_after(datetime(2026, 1, 1, 10, tzinfo=timezone.utc)) == datetime(2026, 1, 1, 11, tzinfo=timezone.utc), "and the range the step really walks is still an expression"
