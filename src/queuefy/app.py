import inspect
import json
from datetime import datetime, timedelta
from typing import Callable

from queuefy.clock import WIDEST_SLOT, as_utc, now, real, waited
from queuefy.errors import QueueError, UnknownTask
from queuefy.retry import RetryPolicy
from queuefy.run import Run, RunStatus
from queuefy.store.base import ATTEMPT_LIMIT, KEY_LIMIT, PRIORITY_LIMIT, QUEUE_LIMIT, TASK_NAME_LIMIT, WHOLE_CEILING, WHOLE_FLOOR, Store
from queuefy.task import Task
from queuefy.trigger import Cron, Interval, Trigger

# how many times a write is asked again after the key it was refused for turned out to be held by nobody. what opens that window is a pruning landing between the two calls, which is a coincidence measured in microseconds — so what this bounds is never the coincidence but a store answering both ways for ever, which would hang the enqueue of whoever called it
REWRITES = 3


def occurrence_key(name: str, moment: datetime) -> str:
    """what makes one slot of a recurring task a single run, computed the same way by every worker of every machine"""
    return f"{name}@{moment.isoformat()}"


def writable(value: str, what: str) -> None:
    """text a store can actually write down. a lone surrogate is what a posix path carries when the bytes it was read from were never utf-8, and sqlite, mysql, postgres and redis each refuse one in their own way — a nul byte is one postgres refuses on its own. memory keeps either without a word, so an application built against it enqueues them and only meets the refusal in production, inside a driver that never says which value it was. these are refused rather than escaped, unlike a message or an answer: what they name is which run this is, and escaping one would quietly fold two callers into a single row. the type is asked for first, because what is not text never reaches any of that: it reaches the encoder, and raises an `AttributeError` out of where the task or the worker is being declared — under a name `except QueueError` never catches"""
    if not isinstance(value, str):
        raise QueueError(f"{what} is {type(value).__name__} and what tells one run from another is text — anything else reaches the encoder instead of a guard, and raises from where it is declared under a name nothing here answers for")

    try:
        value.encode("utf-8")
    except UnicodeEncodeError as refusal:
        raise QueueError(f"{what} holds a character at {refusal.start} that no store can write down, which is what a posix path carries when the bytes behind it were never utf-8 — memory keeps it and every real store refuses it deep inside a driver") from refusal

    if "\x00" in value:
        raise QueueError(f"{what} holds a nul byte, which postgres refuses on its own while every other store keeps it — so the very same call writes a run on one of them and raises on another")


def holdable(value: str, limit: int, what: str) -> None:
    """what a store can hold a name in: text it can write, and no more of it than the column keeps. there is nothing further down that can tell a value is too long — a database in strict mode refuses the write, and one that is not quietly cuts the value short, which for a key is two occurrences of two different minutes ending up as one run"""
    writable(value, what)

    if len(value) > limit:
        raise QueueError(f"{what} is {len(value)} characters and a store keeps {limit} of them, so the write is one the database refuses or quietly cuts short")


def keyed(key: str) -> None:
    """what a key has to be for a store to tell one run from another by it, asked wherever one is written and wherever one is looked for. nothing at all is the value the stores cannot agree on: memory compares it to the key of every run and answers with the first that was written under none, sqlalchemy reads it as `IS NULL` and raises the moment two runs have no key between them, and redis looks for one spelled 'None' — so a caller reading a key off a request that did not carry it is handed somebody else's run, an exception, or nothing, by where the runs happen to live. a key of no characters is the same disagreement one step along: a column takes it as a name held once, and redis reads it as no key at all"""
    if not isinstance(key, str):
        raise QueueError(f"the key is {key!r}, and what tells one run from another is text — memory answers with the first run written under no key, a database raises as soon as two of them have none, and redis looks for a key spelled after it")

    if not key:
        raise QueueError("a key of no characters names nothing, and what makes a run single is the name no other run may take")

    holdable(key, KEY_LIMIT, f"the key '{key}'")


def scheduled(when, what: str) -> datetime:
    """the instant a run comes due, settled here the way its arguments are. it is the one field nothing further down can do without: memory holds whatever it is handed and every claim that meets it then raises comparing that to the moment — not the one run, but every claim of every worker from then on, which is a fleet that polls for ever and takes nothing. a database refuses the column and redis refuses the score, so a nullable column handed straight to `enqueue_at` is a divergence no store names the same way"""
    if not isinstance(when, datetime):
        raise QueueError(f"{what} is {when!r}, and a run comes due at an instant — memory holds whatever it is given and every claim of every worker raises against it from then on, while a database refuses the column and redis refuses the score")

    return as_utc(when)


def exact(value, what: str) -> None:
    """every whole number in a value read back as the very same number. mysql keeps one inside a json value for as long as it fits a 64 bit integer and turns everything either side of that into a double, so a number nothing here refuses is one four stores read back whole while the fifth answers with a number that is not the one that was written — and the handler is called on a value nobody enqueued, with nothing anywhere saying so. `bool` is an `int` to python and `true` to every reader of json, which is why the type is asked for and not the interface. only what a value holds is walked and never what it is keyed by: json names every key with a string, so a payload keyed by a number that wide is settled into one every store already agrees on"""
    if isinstance(value, dict):
        for held in value.values():
            exact(held, what)

        return

    if isinstance(value, (list, tuple)):
        for held in value:
            exact(held, what)

        return

    if type(value) is int and not WHOLE_FLOOR <= value <= WHOLE_CEILING:
        raise QueueError(f"{what} holds {value}, which is past the whole numbers a store keeps inside json — mysql answers with the double nearest to it while every other store answers with the number that was written, so the very same call means two different things depending on where the runs live")


def as_written(payload: dict, what: str) -> dict:
    """the arguments as every store reads them back, refused here when no store could write them down at all. a payload is json wherever the runs live, so one that is not is a write every real store refuses deep inside a driver and memory takes without a word — an application built against memory enqueues it and only meets it in production. a lone surrogate is the same divergence one store further along: it is what a posix path carries when the bytes it was read from were never utf-8, and mysql refuses one inside a json value where the other four hold it. `nan` and `infinity` are that divergence again: python writes them as words no other reader of json takes, so sqlite and redis hold what mysql and postgres refuse. what does survive comes back changed — a tuple as the list a store answers with, a key that is not a string as the string one answers with — so the arguments are read back here and settled once, for the reason the instant is settled here and never in a store"""
    if not isinstance(payload, dict):
        raise QueueError(f"{what} is {type(payload).__name__} and the arguments of a run are a mapping, because a handler is called with keyword arguments — and no store agrees on one that is not: a database reads an empty list, an empty string and a zero back as no arguments")

    try:
        # walked inside the catch, because a payload that holds itself is a `RecursionError` raised here rather than the circular reference `json` names — and one refusal escaping where the other is answered for is the caller told nothing at all
        exact(payload, what)

        written = json.dumps(payload, ensure_ascii=False, allow_nan=False)

        # a lone surrogate is refused by the encoding and by nothing above it, so the text is encoded for the refusal and never for the bytes
        written.encode("utf-8")
    except (TypeError, ValueError, RecursionError) as refusal:
        raise QueueError(f"{what} is not something a store can write down, and the arguments of a run are json wherever they live: {refusal}") from refusal

    return json.loads(written)


def whole(value, what: str) -> None:
    """what a store can write a count down as and read back unchanged. redis keeps these as text and reads them with `int`, so a float is written as a lane called '5.0' and a boolean as one called 'True' — neither of which reads back, and what raises on it is not the run but the claim that met it, on every poll of every worker from then on. the type is asked for and not the interface, because a boolean is an int to python and a word to a store"""
    if type(value) is not int:
        raise QueueError(f"{what} is {value!r}, and what a store keeps a count as is a whole number — a float or a boolean is written down as text nothing reads back, and every claim that meets it raises instead of taking anything at all")


def ranked(priority: int, what: str) -> None:
    """a priority names a lane and never a score. redis holds one sorted set per priority a queue has seen and a claim walks all of them, so a number drawn from a clock or an id is a lane a second for ever — and every poll of every worker walks the lot of them, in a script that holds the server while it runs"""
    whole(priority, what)

    if not -PRIORITY_LIMIT <= priority <= PRIORITY_LIMIT:
        raise QueueError(f"{what} is {priority}, and a priority runs from -{PRIORITY_LIMIT} to {PRIORITY_LIMIT} because what it names is a lane — one drawn from something that keeps changing is a lane a second, and a claim that walks every one of them")


def generative(handler: Callable) -> bool:
    """whether calling this runs none of its body and answers a generator instead. an object is asked about its `__call__` and never about itself, because what `inspect` reads is the code of what it is handed and an instance has none — so a class based handler whose `__call__` yields went straight past a gate written for the plain function, and a callable object is a handler this library takes"""
    return any(inspect.isgeneratorfunction(shape) or inspect.isasyncgenfunction(shape) for shape in (handler, getattr(handler, "__call__", None)))


def trigger_for(every: float | timedelta | None, cron: str | None) -> Trigger | None:
    if every is not None:
        return Interval(every.total_seconds() if isinstance(every, timedelta) else every)

    return Cron(cron) if cron is not None else None


class Queuefy:
    """what an application holds: the tasks it knows, and the store where their runs live"""

    def __init__(self, store: Store) -> None:
        self.store = store
        self.tasks: dict[str, Task] = {}

        # the slot each recurring task was last asked to write, so a poll every second is not an insert every second
        self.written: dict[str, datetime] = {}

    def register(self, task: Task) -> Task:
        """the one gate every task passes through, however it was built. a `Task` is a dataclass anybody may write by hand, so a policy answered for by the decorator alone is one a task declared this way carries straight into a worker — and a wait of `nan` there is an ending the store never takes, a run left claimed, and the very same handler run again on every lease after that"""
        # answered for before it is looked up, because the name is what the registry is keyed by: one that cannot be a key raises out of the lookup itself, under a name `except QueueError` never catches and with nothing said about the name being text
        holdable(task.name, TASK_NAME_LIMIT, f"the name of '{task.name}'")

        if task.name in self.tasks:
            raise QueueError(f"'{task.name}' is registered twice, and a name has to mean one thing")

        holdable(task.queue, QUEUE_LIMIT, f"the queue '{task.queue}' of '{task.name}'")
        ranked(task.priority, f"the priority of '{task.name}'")

        # calling a generator runs none of its body: the worker is handed the generator itself, keeps nothing of it because it is not a mapping, and closes the run as done with the work never started
        if generative(task.handler):
            raise QueueError(f"'{task.name}' is a generator, and calling one runs none of what it was written to do — every run of it would be closed as done with nothing having happened, which is a queue that takes everything it is given and works none of it")

        self.policed(task)

        if task.trigger is not None:
            # what writes the next slot is read on every pass of every worker, and one that cannot answer raises inside the pass rather than on the run: the task never fires, and every recurring task declared after it is skipped along with it, on every poll, for as long as the process lives
            if not isinstance(task.trigger, Trigger):
                raise QueueError(f"the trigger of '{task.name}' is {type(task.trigger).__name__} and what says when a task comes round again is a Trigger — one that is not raises where the slots that came due are written, which is this task and every recurring task declared after it never firing")

            # measured against the widest slot and never the one it wants next, because an interval of half a second lands on microseconds every other slot
            holdable(occurrence_key(task.name, WIDEST_SLOT), KEY_LIMIT, f"the key each slot of '{task.name}' is written under")

        self.tasks[task.name] = task

        return task

    def policed(self, task: Task) -> None:
        """what a run of this task is tried and waited under, every one of which fails silently rather than loudly when it is wrong"""
        if not isinstance(task.retry_policy, RetryPolicy):
            raise QueueError(f"the retry policy of '{task.name}' is {task.retry_policy!r} and what says how long an attempt waits is a RetryPolicy — memory keeps whatever it is given and runs, while every store that writes a run out asks it for the value it is written under and raises where the run is enqueued")

        whole(task.max_attempts, f"the attempts allowed to '{task.name}'")

        if task.max_attempts < 1:
            raise QueueError(f"'{task.name}' allows {task.max_attempts} attempts, and a run has to be tried at least once")

        if task.max_attempts > ATTEMPT_LIMIT:
            raise QueueError(f"'{task.name}' allows {task.max_attempts} attempts and a store keeps that count in a column holding {ATTEMPT_LIMIT}, so every enqueue of it is a write mysql and postgres refuse while memory, sqlite and redis take it")

        if task.timeout is not None:
            real(task.timeout, f"the timeout of '{task.name}'")

            if task.timeout <= 0:
                raise QueueError(f"'{task.name}' has a timeout of {task.timeout}s, which stops every run before it starts")

        waited(task.retry_delay, f"the wait '{task.name}' takes before another attempt")

        if task.retry_delay < 0:
            raise QueueError(f"'{task.name}' waits {task.retry_delay}s before another attempt, and a wait is never negative")

        waited(task.max_retry_delay, f"the longest wait '{task.name}' takes before another attempt")

        if task.max_retry_delay <= 0:
            raise QueueError(f"'{task.name}' waits at most {task.max_retry_delay}s before another attempt, which is a retry due before the one that failed and a queue that hammers instead of backing off")

    def task(self, name: str, *, queue: str = "default", every: float | timedelta | None = None, cron: str | None = None, max_attempts: int = 1, timeout: float | None = None, retry_policy: RetryPolicy = RetryPolicy.EXPONENTIAL, retry_delay: float = 5.0, max_retry_delay: float = 3600.0, priority: int = 0) -> Callable:
        """declares a task. with neither `every` nor `cron` it runs when somebody enqueues it, and with one of them it also runs on its own"""
        if every is not None and cron is not None:
            raise QueueError(f"'{name}' asks for an interval and a cron, and a task runs on one clock")

        def declare(handler: Callable) -> Callable:
            self.register(Task(name=name, handler=handler, queue=queue, trigger=trigger_for(every, cron), max_attempts=max_attempts, timeout=timeout, retry_policy=retry_policy, retry_delay=retry_delay, max_retry_delay=max_retry_delay, priority=priority))

            return handler

        return declare

    def task_for(self, name: str) -> Task:
        if name not in self.tasks:
            raise UnknownTask(f"nothing here is called '{name}'")

        return self.tasks[name]

    async def setup(self) -> None:
        await self.store.setup()

    def build(self, task: Task, due_at: datetime, payload: dict, key: str | None, priority: int | None) -> Run:
        """the instant is settled here and never in a store: a datetime with no zone is the utc one it reads as, and three stores each deciding that for themselves is the same value naming three instants"""
        if key is not None:
            keyed(key)

        if priority is not None:
            ranked(priority, f"the priority this call gives '{task.name}'")

        moment = scheduled(due_at, f"the instant this call gives '{task.name}'")
        arguments = as_written(payload, f"the payload this call gives '{task.name}'")

        return Run(name=task.name, queue=task.queue, payload=arguments, key=key, due_at=moment, max_attempts=task.max_attempts, timeout=task.timeout, retry_policy=task.retry_policy, retry_delay=task.retry_delay, max_retry_delay=task.max_retry_delay, priority=task.priority if priority is None else priority)

    async def store_once(self, run: Run) -> Run:
        """a key already taken is not a failure: the caller is handed the run that got there first. writing and finding are two calls with the world between them, so a pruning that drops the holder in that moment leaves the key free and the run nowhere — and answering nothing there hands back `None` where the caller is owed a run. asking again is what settles it, because a key nobody holds is one this write can take"""
        for _ in range(REWRITES):
            written = await self.store.add(run)

            if written is not None:
                return written

            held = await self.store.find(run.key)

            if held is not None:
                return held

        raise QueueError(f"the key '{run.key}' was refused as taken and then found held by nobody, {REWRITES} times over — a store answering both of those at once is one nothing here can write a run to")

    async def enqueue(self, name: str, /, *, key: str | None = None, priority: int | None = None, payload: dict | None = None, **shorthand) -> Run:
        """runs as soon as a worker is free, which is what an email leaving a request wants"""
        return await self.enqueue_at(name, now(), key=key, priority=priority, payload=payload, **shorthand)

    async def enqueue_at(self, name: str, when: datetime, /, *, key: str | None = None, priority: int | None = None, payload: dict | None = None, **shorthand) -> Run:
        """runs once, at a stated instant. a worker that was down when it came due picks it up as soon as it is back"""
        return await self.store_once(self.build(self.task_for(name), when, self.payload_of(payload, shorthand), key, priority))

    def payload_of(self, payload: dict | None, shorthand: dict) -> dict:
        """keywords are the short way to say it, and `payload` is the way to say anything — a task whose argument is called `key` has to be reachable too"""
        if payload is None:
            return shorthand

        if shorthand:
            raise QueueError("a payload was given whole and in pieces at the same time, and only one of them can be the arguments")

        return payload

    async def materialize(self, moment: datetime | None = None) -> list[Run]:
        """writes the next slot of every recurring task. every worker does this and the key is what leaves one run, so nothing here elects a leader"""
        moment = moment or now()
        written = []

        for task in self.tasks.values():
            if task.trigger is None:
                continue

            # the slot this worker wrote last is still ahead, so there is nothing to write and nothing to work out: asking the trigger first would walk a yearly expression minute by minute on every poll, which is a third of a core spent answering the same thing
            ahead = self.written.get(task.name)

            if ahead is not None and ahead > moment:
                continue

            due_at = task.trigger.next_after(moment)
            slot = await self.store.add(self.build(task, due_at, {}, occurrence_key(task.name, due_at), None))

            # remembered only once the store has it: marking it first would let a store that blinked drop this slot for good, and a daily task would lose a whole day in silence
            self.written[task.name] = due_at

            if slot is not None:
                written.append(slot)

        return written

    async def cancel(self, run_id) -> bool:
        return await self.store.cancel(run_id, now())

    async def get(self, run_id) -> Run | None:
        return await self.store.get(run_id)

    async def find(self, key: str) -> Run | None:
        """the key is answered for here exactly as it is where a run is written under one, because a name no run could ever have been written under is one every store looks for differently"""
        keyed(key)

        return await self.store.find(key)

    async def count(self, status: RunStatus | None = None, queue: str | None = None) -> int:
        return await self.store.count(status, queue)
