import asyncio
import inspect
import json
import logging
import os
import random
import socket
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import suppress
from contextvars import copy_context
from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import partial
from time import monotonic
from typing import Callable
from uuid import uuid4

from queuefy.app import Queuefy, exact, writable
from queuefy.clock import kept, now, real, spanned, waited
from queuefy.errors import PermanentError, QueueError, UnknownTask, UnwritableAnswer, WorkNeverStarted
from queuefy.retry import delay_for
from queuefy.run import Run
from queuefy.store.base import ERROR_LIMIT, ERROR_TYPE_LIMIT, QUEUE_LIMIT, WORKER_NAME_LIMIT

logger = logging.getLogger(__name__)

# the lease is pushed well before it runs out, so a slow run never has its run taken from under it
HEARTBEAT_SHARE = 3

# pruning is a housekeeping write and not the work, so it happens on the hour and never on the poll
PURGE_EVERY = 3600.0

# how many settled runs one pruning drops, so a year that was never pruned is caught up over some passes instead of in one statement that holds the table
PURGE_LIMIT = 1000


def worker_name() -> str:
    """a worker is told apart from every other one anywhere: the host names the machine, the pid names the process, and the draw covers a pid the system handed out again"""
    tail = f":{os.getpid()}:{uuid4().hex[:8]}"

    # a pod is named after its deployment, its namespace and its cluster, which is long past what a store keeps a worker name in — and the draw is what tells two machines apart once the domain is cut off the end
    return socket.gethostname()[: WORKER_NAME_LIMIT - len(tail)] + tail


def cut(value: str, limit: int) -> str:
    """what fits, and a mark where the rest was. a value quietly cut short reads as the whole of it, which for a failure is an operator reading half a message as all there was"""
    return value if len(value) <= limit else value[: limit - 3] + "..."


def storable(value: str) -> str:
    """the same text in what a store can actually hold. a posix path carries whatever bytes it had, and read back it carries them as lone surrogates — which are not utf-8 at all, and which sqlite, mysql, postgres and redis each refuse in their own way. a nul byte is one postgres refuses on its own. both are escaped into the text that says what they were, because a message nothing can write is an ending nobody records"""
    return value.encode("utf-8", "backslashreplace").decode("utf-8").replace("\x00", "\\x00")


def escaped(value):
    """the same answer with every string in it made storable, for the reason a message is: a posix path read back off a filesystem carries whatever bytes it had as lone surrogates, which mysql refuses inside a json value where every other store holds it. a tuple is left a tuple here rather than made the list a store reads one back as, because a key that is not hashable is a `TypeError` raised where the ending of the run is being written — which is the one thing all of this exists to stop"""
    if isinstance(value, str):
        return storable(value)

    if isinstance(value, dict):
        return {escaped(name): escaped(held) for name, held in value.items()}

    if isinstance(value, tuple):
        return tuple(escaped(held) for held in value)

    if isinstance(value, list):
        return [escaped(held) for held in value]

    return value


def keepable(answer: dict) -> dict:
    """the answer as every store reads it back, settled here for the reason the arguments of a run are settled where they are written — one value, meaning one thing, wherever the runs live. a key that is not a string comes back as one in every store but the one that keeps the object it was handed, and a tuple comes back as a list. what no store could write raises instead: `nan` and `infinity` are words only python's own reader of json has, so sqlite and redis hold what mysql and postgres refuse, and an object no serialiser takes is one memory keeps and every other store throws the ending away over. a whole number past what mysql holds inside a json value is the one refused for answering rather than for raising, because what it costs is not an ending nobody records but a result nobody can tell was changed"""
    exact(answer, "what the handler answered")

    return json.loads(json.dumps(escaped(answer), ensure_ascii=False, allow_nan=False))


def spoken(error: BaseException) -> str:
    """what the failure says for itself, and the name of its class when it cannot say anything. asking an exception for its message runs whatever built it, so one carrying an object with a broken `__str__` raises where the ending is being written — which is the very thing the escaping and the cutting below exist to stop"""
    try:
        return str(error) or type(error).__name__
    except Exception:
        return type(error).__name__


def described(error: BaseException) -> tuple[str, str]:
    """what a store is told a run failed with, made storable and then cut to what it keeps — in that order, because escaping a character no store takes is what makes a message longer than the column. a message is whatever the code being run put in it, so unlike a name or a key there is nowhere to refuse one where it is written, and a store that will not take it is an ending nobody records, a run left claimed, and the same handler run again on every lease after that. only the length is asked of the class, because python refuses to name one with a character utf-8 cannot hold"""
    return cut(storable(spoken(error)), ERROR_LIMIT), cut(type(error).__name__, ERROR_TYPE_LIMIT)


@dataclass
class Handed:
    """what became of the work a plain handler was handed to a thread on, which is the one thing the failure itself can never say. what a handler raised is code being run, so reading a class out of it as a condition of the worker is how the run of a rolling deploy came to repeat for ever — and a handler raising `TimeoutError` out of a socket of its own looks exactly like the wait this worker gave up on"""

    started: bool = True


class Worker:
    """claims what is due and runs it. any number of these may run against one store, on one machine or on twenty"""

    def __init__(self, app: Queuefy, *, name: str | None = None, queues: tuple[str, ...] = ("default",), concurrency: int = 8, poll: float = 1.0, lease: timedelta = timedelta(seconds=60), jitter: float = 0.25, grace: float = 30.0, keep: timedelta | None = timedelta(days=7)) -> None:
        name = name or worker_name()

        # every one of these fails silently rather than loudly when it is wrong, so nothing is built out of them until they are all answered for
        leased = spanned(lease, "the lease of a worker")

        if leased <= 0:
            raise QueueError(f"a lease of {leased}s is one that has already run out, and a worker cannot hold a run for it")

        # the lease is added to the instant every claim and every beat is made at, so one past what is left of the range raises inside the claim itself — where the pass logs and the worker goes on polling and never takes anything
        waited(leased, "the lease of a worker")

        real(poll, "the poll of a worker")

        if poll <= 0:
            raise QueueError(f"a poll of {poll}s is not a wait, and a worker that does not wait spends a core asking")

        # what it becomes is the limit of every claim, which memory slices a list by and every other store hands to the server. the type is asked for and not the interface, because a boolean is an int to python and a count of runs to nothing else
        if type(concurrency) is not int:
            raise QueueError(f"a concurrency of {concurrency!r} is not a whole number of runs, and what it becomes is the limit every claim is made with — a worker that comes up, polls, raises on each of them and never takes anything while it says so once a second")

        if concurrency < 1:
            raise QueueError(f"a concurrency of {concurrency} is a worker that never takes anything")

        if not queues:
            raise QueueError("a worker serving no queues claims nothing, and there is nothing in the queue that would ever explain why")

        # a string is a sequence of its letters, so one queue given as a name is a worker serving each letter of it: memory finds the name inside the string and works, sqlalchemy refuses the statement on every pass, and redis quietly claims nothing at all
        if isinstance(queues, str):
            raise QueueError(f"the queues are the one name '{queues}', and a string is read one letter at a time — so this worker serves a queue for every letter of it and never the queue it was named for")

        # what a worker serves is read again on every claim it ever makes, so it has to be something that can be read more than once. a generator written where a tuple was meant is emptied by the very loop below that answers for it, and every claim after that is made with no queues at all — which memory, sqlalchemy and redis each answer with nothing, so what comes up is a worker that polls for ever and takes nothing while the runs it was named for wait in the queue
        if not isinstance(queues, (tuple, list)):
            raise QueueError(f"the queues are {type(queues).__name__} and what a worker serves is a tuple of names — one read out of something that empties as it is walked leaves every claim after the first asking for no queues at all")

        for queue in queues:
            # the name goes into every claim this worker makes, so one no store can write is a claim the driver refuses on every pass while the process stays up
            writable(queue, f"'{queue}', which this worker serves")

            if len(queue) > QUEUE_LIMIT:
                raise QueueError(f"'{queue}' is {len(queue)} characters and a queue holds {QUEUE_LIMIT}, so no task could ever be declared with it and this worker would serve an empty name for ever")

        real(grace, "the grace of a worker")

        if grace < 0:
            raise QueueError(f"a grace of {grace}s is not a span to wait for what is in flight")

        real(jitter, "the jitter of a worker")

        if jitter < 0:
            raise QueueError(f"a jitter of {jitter} draws a delay shorter than the policy asked for, and one far enough below it is a retry due in the past")

        if keep is not None:
            keeping = spanned(keep, "the span a worker keeps a run that is over for")

            if keeping < 0:
                raise QueueError(f"keeping a run for {keeping}s asks for what is over to be dropped before it is over, which is all of it")

            # the span is taken off the instant every pruning is asked at, so one past what lies behind now raises on the hour inside the pruning and never where it was written
            kept(keeping, "the span a worker keeps a run that is over for")

        # the name is in every claim, every beat and every ending this worker writes, exactly as the queues it serves are — and it is not one anybody had to type: `socket.gethostname()` carries whatever bytes the host had, and python reads the ones that were never utf-8 back as lone surrogates. memory claims under it without a word while sqlite, mysql, postgres and redis each refuse it in their own way, so what comes up is a worker that logs the pass, waits and asks again, and never takes anything at all
        writable(name, f"'{name}', which this worker is called")

        if len(name) > WORKER_NAME_LIMIT:
            raise QueueError(f"a name of {len(name)} characters does not fit the {WORKER_NAME_LIMIT} a store keeps for one, and a claim the database refuses is a worker that polls forever and takes nothing")

        self.app = app
        self.name = name
        self.queues = queues
        self.concurrency = concurrency
        self.poll = poll
        self.lease = lease
        self.jitter = jitter
        self.grace = grace
        self.keep = keep

        # threads of its own, one per run it may hold, because the pool asyncio hands out by default is sized for the whole process and shared with everything else in it. that pool holds min(32, cores + 4) threads, which is six on a two core container and under the eight runs a worker holds by default — so the runs past it waited in a queue for a thread, and a timeout, which starts where the run was claimed, ran out on work that had never started. the run was written down as one that took too long, its attempt was spent, and the handler was never called at all. the threads are made as they are needed, so a worker whose handlers are all coroutines makes none
        self.threads = ThreadPoolExecutor(max_workers=concurrency, thread_name_prefix=f"queuefy-{name}")

        # drawn, so ten workers coming up together do not all reach for the same rows in the same instant — the same reason a retry delay is drawn
        self.purged: float | None = monotonic() - random.uniform(0, PURGE_EVERY)
        self.running: set[asyncio.Task] = set()

        # the threads still inside a handler this worker stopped waiting for, which are threads it does not have and must not claim against
        self.stranded: set[Future] = set()
        self.stopping = asyncio.Event()
        self.starting: list[Callable] = []
        self.finishing: list[Callable] = []
        self.failing: list[Callable] = []

    def on_start(self, listener: Callable) -> Callable:
        """called with the run, before its handler runs"""
        self.starting.append(listener)

        return listener

    def on_finish(self, listener: Callable) -> Callable:
        """called with the run, what the handler answered and how long it took"""
        self.finishing.append(listener)

        return listener

    def on_error(self, listener: Callable) -> Callable:
        """called with the run, what broke, how long it took, and whether it comes back for another attempt"""
        self.failing.append(listener)

        return listener

    async def announce(self, listeners: list[Callable], *arguments) -> None:
        """a listener that breaks breaks alone: an audit trail that fails must never take the outcome of the run with it"""
        for listener in listeners:
            try:
                outcome = listener(*arguments)

                # a plain function is a listener too, and the same ambiguity a handler already answers for: `await` on what it returned is the error nobody reads
                if inspect.isawaitable(outcome):
                    await outcome
            except asyncio.CancelledError:
                # the shutdown asking, and swallowing it here would leave a worker nobody can stop
                raise
            except BaseException:
                # `sys.exit()` deep inside a library is not an `Exception`, and announced before the handler runs it took the whole run with it
                logger.exception("[queuefy] a listener of %s failed", self.name)

    @property
    def free(self) -> int:
        return self.concurrency - len(self.running) - self.holding()

    def holding(self) -> int:
        """the threads still inside a handler this worker stopped waiting for. a timeout ends the waiting and only a coroutine ends the work, so a plain handler that ran past one carries on to its own end and keeps the thread it is on the whole time — while the slot that handler was claimed into is handed straight back. counted against the concurrency, because a slot counted free against a thread that is not is a run claimed onto a pool with nothing left to run it on: it waits there for a thread while its own timeout, which starts where the work was handed over and not where a thread picked it up, runs out on work that never began. measured on a concurrency of four against four handlers that each ran four times their timeout and then returned: eight of the twenty runs behind them were written down as having taken too long, their attempts spent, with their handler never entered"""
        return len(self.stranded)

    async def run_once(self) -> list[Run]:
        """one pass: what a dead worker left goes back, what is due is written, and what this worker can hold is claimed and started"""
        moment = now()

        await self.housekept(self.app.store.reclaim(moment), "take back what a worker that stopped answering was holding")
        await self.housekept(self.tidy(moment), "prune what is over")
        await self.housekept(self.app.materialize(moment), "write the slots that have come due")

        if self.free <= 0:
            return []

        # the instant is asked for again, because a lease runs from when the run was taken and not from when the pass began: reclaiming, pruning and materializing are round trips to the store, and a run started on a lease those already spent half of is one the next reclaim takes back while it is still working
        claimed = await self.app.store.claim(self.name, self.queues, self.free, self.lease, now())

        for run in claimed:
            self.spawn(run)

        return claimed

    async def housekept(self, work, what: str) -> None:
        """everything a pass does before the claim, and none of it is the work: what the store refused here must never cost the pass the claim it was on its way to make, and the next pass asks for all of it again. writing the slots that came due is the one most likely to be refused for a reason a claim never meets, because every worker of a fleet aims that write at the same key in the same instant — and taking back an expired lease is the statement a database is likeliest to roll back under contention. either of them ending the pass is a fleet that stops claiming for reasons nothing in the queue explains"""
        try:
            await work
        except Exception:
            logger.exception("[queuefy] %s could not %s", self.name, what)

    async def tidy(self, moment: datetime) -> int:
        """a queue nobody prunes grows for ever, and what it grows by is rows nothing reads again"""
        if self.keep is None:
            return 0

        if self.purged is not None and monotonic() - self.purged < PURGE_EVERY:
            return 0

        # the hour starts before the pruning rather than after it, so one the store refuses waits the hour out like one that worked instead of being asked again on every poll
        self.purged = monotonic()
        gone = await self.app.store.purge(moment - self.keep, PURGE_LIMIT)

        # a full batch means there is more of it, so the next pass takes the next one instead of waiting the hour out: a year that was never pruned would take a year of hours to catch up on
        if gone == PURGE_LIMIT:
            self.purged = None

        return gone

    def spawn(self, run: Run) -> None:
        running = asyncio.create_task(self.execute(run))
        self.running.add(running)
        running.add_done_callback(self.running.discard)

    async def drain(self) -> None:
        """waits for what this worker started, and says what it gave up on. one bounded wait and never a loop over the set: a task is taken out of it by a callback the loop runs later, so asking again as soon as the wait returns spins, and ten workers spinning starve the very callbacks they are waiting for"""
        if not self.running:
            return

        _, pending = await asyncio.wait(tuple(self.running), timeout=self.grace)

        if pending:
            logger.warning("[queuefy] %s stopped with %s runs still in flight, and their leases are what bring them back", self.name, len(pending))

    async def run(self) -> None:
        """polls until `stop`, then lets what is in flight land"""
        try:
            while not self.stopping.is_set():
                try:
                    await self.run_once()
                except Exception:
                    # a store that blinked must not end the worker, or one bad minute stops every run forever
                    logger.exception("[queuefy] the pass of %s failed", self.name)

                await self.wait()

            await self.drain()
        finally:
            # the pool is this worker's own and its threads wait on it for ever, so one nobody closes is every one of them left alive for as long as the process is — an api running a worker in its lifespan and coming back up on a reload holds both sets. closed however this loop ends, because a worker is stopped by having its task cancelled as often as by being asked: a task group, a supervisor and `asyncio.run` with this still pending every one of them cancels rather than asks, and each of those went straight past the close. never waited on: a plain handler is one no code here can end, and what is still in flight is what the leases bring back
            self.threads.shutdown(wait=False)

    async def wait(self) -> None:
        """the poll is a wait on being told to stop and never a sleep, so a worker asked to go does not spend one more of them first"""
        with suppress(TimeoutError):
            await asyncio.wait_for(self.stopping.wait(), timeout=self.poll)

    def stop(self) -> None:
        self.stopping.set()

    async def execute(self, run: Run) -> None:
        resting = asyncio.Event()
        beat = asyncio.create_task(self.beat(run, resting))

        try:
            await self.attempt(run)
        except asyncio.CancelledError:
            raise
        except BaseException:
            # the outcome never reached the store, so nothing on either side knows what became of this run: the lease is what brings it back, and this line is the only place that says why it came
            logger.exception("[queuefy] %s could not record the outcome of %s", self.name, run.name)
        finally:
            # the beat is asked to stop and then waited for, never cancelled: a command interrupted halfway leaves the connection it was using with an answer nobody read, and whoever takes that connection next waits for a reply that already went somewhere else
            resting.set()
            await asyncio.gather(beat, return_exceptions=True)

    async def attempt(self, run: Run) -> None:
        started = monotonic()

        await self.announce(self.starting, run)

        # looked up before anything is called, and never caught around the call: `UnknownTask` means this worker does not declare the name, which only the lookup can say. a handler raising it is a handler with a bug, and reading that as a rolling deploy hands the run back with the attempt given back — for ever, at every poll, repeating whatever the handler did before it raised
        try:
            handler = self.app.task_for(run.name).handler
        except UnknownTask as error:
            # nothing was attempted: the name belongs to the code beside this worker, which is what a rolling deploy is. failing it here destroys a run that is perfectly good, and `max_attempts` is one by default — so this one never counts against it
            await self.broke(run, error, started, await self.release(run, error))

            return

        handed = Handed()

        try:
            result = await self.call(run, handler, handed)
        except asyncio.CancelledError:
            # cancellation is the shutdown asking, and swallowing it would leave a worker that cannot be stopped
            raise
        except PermanentError as error:
            await self.broke(run, error, started, await self.settle(run, error, retryable=False))
        except Exception as error:
            await self.broke(run, error, started, await self.after(run, error, handed))
        except BaseException as error:
            # asyncio never swallows SystemExit or KeyboardInterrupt inside a task: it hands them to the loop, which ends the whole worker and every run it was holding. one handler calling sys.exit must not do that, and it is not something another attempt fixes either
            logger.exception("[queuefy] %s asked the process to stop, and it was refused", run.name)
            await self.broke(run, error, started, await self.settle(run, error, retryable=False))
        else:
            await self.answered(run, result, started)

    async def answered(self, run: Run, result, started: float) -> None:
        """closes the run with what the handler answered, made storable and settled into what every store reads back. an answer no store could write down is a handler with a bug and never a bad minute, so it is the end of the run however many attempts were allowed: left to the store the ending never lands at all, the run stays claimed, and the lease runs the very same handler again on every one of them until the attempts are spent — under a message about a worker that stopped answering, which is not what happened. an answer that holds itself, or one nested past what a reader of json can walk, is refused here for that same reason: what says so is a `RecursionError`, which the escaping raises before the writing ever gets to"""
        try:
            answer = keepable(result) if isinstance(result, dict) else None
        except (TypeError, ValueError, RecursionError, QueueError) as refusal:
            broken = UnwritableAnswer(f"'{run.name}' answered something no store can write down, and what a run is closed with is json wherever the runs live: {refusal}")

            await self.broke(run, broken, started, await self.settle(run, broken, retryable=False))

            return

        if self.recorded(run, await self.app.store.complete(run.id, self.name, run.attempts, now(), answer)):
            await self.announce(self.finishing, run, answer, monotonic() - started)

    async def broke(self, run: Run, error: BaseException, started: float, coming_back: bool | None) -> None:
        """tells the listeners how an attempt ended, unless the store would not take that ending"""
        if coming_back is None:
            return

        await self.announce(self.failing, run, error, monotonic() - started, coming_back)

    def recorded(self, run: Run, taken: bool) -> bool:
        """whether the store took the outcome. it refuses when the lease ran out and the run went to another attempt — this worker's own next one among them, because a pass reclaims what expired and then claims it back under the same name — and announcing an outcome the store threw away puts a run in an audit trail twice, once here and once under whoever is running it now"""
        if taken:
            return True

        logger.warning("[queuefy] %s no longer held %s when it was over, so the outcome was dropped and whoever holds it now answers for the run", self.name, run.name)

        return False

    async def after(self, run: Run, error: Exception, handed: Handed) -> bool | None:
        """what a failed attempt becomes, which is not the same thing as what the failure says it was. work still queued for a thread when the timeout passed never ran a line, so that run goes back with its attempt given back — the path a name this worker does not declare already takes, and for the same reason: an attempt that never happened is not one the run has spent, and with `max_attempts` at one writing it down as spent is the work gone"""
        if handed.started:
            return await self.settle(run, error, retryable=True)

        return await self.release(run, error)

    async def call(self, run: Run, handler: Callable, handed: Handed):
        """a handler may be a coroutine or a plain function, and a plain one runs off the loop so it never blocks the others"""
        if inspect.iscoroutinefunction(handler):
            return await self.bounded(self.settled(handler(**run.payload)), run.timeout)

        # the context is carried into the thread the way `asyncio.to_thread` carries it, or a handler reads nothing where whoever enqueued it set a contextvar
        work = self.threads.submit(partial(copy_context().run, handler, **run.payload))

        try:
            return await self.bounded(self.settled(asyncio.wrap_future(work)), run.timeout)
        except TimeoutError:
            # the cancel decides it and never a look at the state, because a thread picks the work up between the two: it is taken only while nothing has, and from that moment nothing ever will. a handler raising `TimeoutError` out of a socket of its own is already finished here, so what it gets is the ending it asked for
            if not work.cancel():
                raise

            handed.started = False

            raise WorkNeverStarted(f"the timeout of '{run.name}' passed while its work was still queued for a thread of {self.name}, so no line of the handler ever ran") from None
        finally:
            # the thread is what the worker claims against, and this is the one place it learns the thread did not come back with the answer. what takes it out again is the work itself as it ends, the way a run this worker started leaves the set it was put in — and a thread that finished between the two is one the callback drops the moment it is added
            if not work.done():
                self.stranded.add(work)
                work.add_done_callback(self.stranded.discard)

                logger.warning("[queuefy] %s stopped waiting for %s and python cannot end the thread it is on, so this worker holds %s fewer runs until that handler returns on its own", self.name, run.name, self.holding())

    async def bounded(self, answer, timeout: float | None):
        """what a timeout puts on an answer, which is a bound on the waiting and never one on the work"""
        if timeout is None:
            return await answer

        return await asyncio.wait_for(answer, timeout=timeout)

    async def settled(self, answer):
        """a callable object with an async `__call__`, or a decorator whose wrapper is plain, does not look like a coroutine function and still answers a coroutine — awaiting only the thread would close the run as done with the work never started"""
        outcome = await answer

        return await outcome if inspect.isawaitable(outcome) else outcome

    async def release(self, run: Run, error: Exception) -> bool | None:
        """hands a run back to the queue with the attempt given back, because this worker never had anything to try — and a spent attempt is what a reclaim reads as a run with nothing left"""
        description, kind = described(error)
        taken = await self.app.store.release(run.id, self.name, run.attempts, now() + timedelta(seconds=self.waiting(run)), description, kind)

        return True if self.recorded(run, taken) else None

    def waiting(self, run: Run) -> float:
        return delay_for(run.retry_policy, run.retry_delay, run.attempts, self.jitter, run.max_retry_delay)

    async def settle(self, run: Run, error: BaseException, retryable: bool) -> bool | None:
        """what a failed attempt becomes: another attempt while the policy allows one, and the end of the run when it does not. answers whether it comes back, and nothing at all when the store would not take the outcome"""
        description, kind = described(error)
        coming_back = retryable and not run.exhausted

        if coming_back:
            taken = await self.app.store.retry_later(run.id, self.name, run.attempts, now() + timedelta(seconds=self.waiting(run)), description, kind)
        else:
            taken = await self.app.store.fail(run.id, self.name, run.attempts, now(), description, kind)

        return coming_back if self.recorded(run, taken) else None

    async def beat(self, run: Run, resting: asyncio.Event) -> None:
        """the lease belongs to this attempt for as long as it is working, and this is what keeps saying so — it only ever stops between two of them, and never inside one"""
        period = self.lease.total_seconds() / HEARTBEAT_SHARE

        while not await self.resting_within(resting, period):
            await self.push(run)

    async def resting_within(self, resting: asyncio.Event, period: float) -> bool:
        """the event is read before the wait, because `wait_for` with nothing left to wait cancels without ever looking at it"""
        if resting.is_set():
            return True

        try:
            return await asyncio.wait_for(resting.wait(), timeout=period)
        except TimeoutError:
            return False

    async def push(self, run: Run) -> None:
        """a beat that could not reach the store is not fatal, and it must never become the outcome of the run: the lease is what says this attempt is still here, and losing it is exactly how the run comes back"""
        try:
            await self.app.store.heartbeat(run.id, self.name, run.attempts, self.lease, now())
        except Exception:
            logger.exception("[queuefy] the beat of %s could not reach the store", run.name)
