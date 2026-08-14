"""every round trip a store makes, refused one at a time, because what a call leaves behind when it stops halfway is what neither gate ever asks about: the coverage gate counts the lines a test reached and every one of these was reached, and load on its own breaks nothing at a named point. what found the claim that took a row in one transaction and read it back in the next was a failure placed exactly between the two. each round trip is refused twice over, once by a refusal that ends the call and once by the one this library is written to ask again after — the second walks the rollback and the second statement that the first never reaches, and a claim rolling a row back and taking it again is the only test there is of it"""

import random
from contextlib import contextmanager
from datetime import timedelta

import pytest
from redis.asyncio import Redis
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession

from queuefy.clock import now
from queuefy.run import Run, RunStatus
from queuefy.store.memory import MemoryStore
from queuefy.store.sqlalchemy import SqlAlchemyStore

from .test_contention import refusal
from .test_differential import closing, readable

# the most round trips one call is walked through, which is well past the thirteen a claim of four rows makes. what it bounds is never a real call but a store answering for ever, which would walk this sweep for ever with it
TRIPS = 64

# every ending a worker writes, each of them a status and the fields that status is read by
ENDINGS = ("complete", "fail", "retry_later", "release")

# the same draw every time, so the ending a cut call was asked for is the ending an uncut one wrote
SEED = 0


class Cut(ConnectionError):
    """a connection going away mid call, which is the shape a driver that went away and a pool with nothing left to hand out both arrive in"""


def gone():
    """the refusal that ends the call it was made in, because nothing here reads it as contention and nothing here asks again after it"""
    return Cut("the connection went away")


def deadlocked():
    """the refusal innodb rolls one side of a deadlock back with, which is the one this library is written to ask again after — so what it cuts is the call and the call made in its place, and what it reads is what the two of them left between them"""
    return refusal(1213)


# a refusal that ends a call and a refusal a call is made again after, because the second walks what the first never reaches: the rollback that has to leave a session usable, and a statement asked a second time over a row the first may already have written
REFUSALS = (gone, deadlocked)


class Counter:
    """the round trips one call makes, with the nth of them refused and every other one let through. it refuses the once, so a store that asks again is asking against a store that answers"""

    def __init__(self, nth: int, refused) -> None:
        self.nth = nth
        self.refused = refused
        self.made = 0

    def asked(self) -> None:
        self.made += 1

        if self.made == self.nth:
            raise self.refused()


@contextmanager
def interrupted(store, nth: int, refused):
    """what crosses the process boundary, counted: a statement and a commit for a database, and a command for redis — which is what every script this store runs reaches the server through as well"""
    counter = Counter(nth, refused)

    if isinstance(store, SqlAlchemyStore):
        execute, commit = AsyncSession.execute, AsyncSession.commit

        async def cut_execute(self, *arguments, **options):
            counter.asked()

            return await execute(self, *arguments, **options)

        async def cut_commit(self, *arguments, **options):
            counter.asked()

            return await commit(self, *arguments, **options)

        AsyncSession.execute, AsyncSession.commit = cut_execute, cut_commit

        try:
            yield counter
        finally:
            AsyncSession.execute, AsyncSession.commit = execute, commit

        return

    command = Redis.execute_command

    async def cut_command(self, *arguments, **options):
        counter.asked()

        return await command(self, *arguments, **options)

    Redis.execute_command = cut_command

    try:
        yield counter
    finally:
        Redis.execute_command = command


def crossing(store, refused) -> None:
    """which stores a refusal of this shape ever reaches"""
    if isinstance(store, MemoryStore):
        pytest.skip("a store that lives in this process crosses nothing, so there is no round trip to cut")

    if refused is deadlocked and not isinstance(store, SqlAlchemyStore):
        pytest.skip("asking again after a deadlock is what a database asks for, and no other store here answers a refusal that way")


async def every_round_trip(store, refused, prepared, called, checked) -> None:
    """one call answered once for every round trip it makes, each of them cut in turn and each cut met by a call nothing cut before it. the sweep ends where a cut no longer fires, which is one round trip past the whole of a call — everything the call is prepared with and everything the cut is read by happens outside the cut, or the sweep would be measuring itself"""
    for nth in range(1, TRIPS + 1):
        ready = await prepared(nth)

        with interrupted(store, nth, refused) as counter:
            answer = None

            try:
                answer = await called(ready)
            except (Cut, DBAPIError):
                pass

        await checked(nth, ready, answer)

        if counter.made < nth:
            return

    raise AssertionError(f"a call was still making round trips after {TRIPS} of them, so this sweep never reached the end of one")


async def holding(store, written) -> set:
    """which of these runs a worker is still on, which is what a claim answers for and what a reclaim is counted by"""
    return {run_id for run_id in written if (await store.get(run_id)).status == RunStatus.RUNNING}


@pytest.mark.parametrize("refused", REFUSALS)
async def test_a_claim_hands_back_every_run_it_took_however_it_was_cut(app, refused):
    """a run moved to running that the claim never answered with is one nobody is holding: its attempt is spent and its lease is running, while the worker that would have started it came away with nothing. what meets it next is the reclaim that lease runs out into, and `max_attempts` is one by default — so it is failed outright under `LeaseExpired`, its handler never called and nothing anywhere saying so"""
    store = app.store

    crossing(store, refused)

    moment = now()

    async def prepared(nth):
        queue = f"lane-{nth}"

        return queue, [(await store.add(Run(name="work", queue=queue, due_at=moment))).id for _ in range(4)]

    async def called(ready):
        queue, _ = ready

        return await store.claim("worker-1", (queue,), 4, timedelta(seconds=60), moment)

    async def checked(nth, ready, answer):
        _, written = ready
        held = {run.id for run in answer or []}
        running = await holding(store, written)

        assert running == held, f"cutting round trip {nth} left {len(running - held)} runs claimed and handed to nobody"

    await every_round_trip(store, refused, prepared, called, checked)


@pytest.mark.parametrize("refused", REFUSALS)
async def test_a_write_that_was_cut_leaves_its_key_naming_a_run_or_naming_nothing(app, refused):
    """the reservation and the run were two steps once, so a process that stopped between them left that occurrence held by nothing — and no worker of any machine could write it ever again"""
    store = app.store

    crossing(store, refused)

    moment = now()

    async def prepared(nth):
        return f"slot-{nth}"

    async def called(key):
        return await store.add(Run(name="work", key=key, due_at=moment))

    async def checked(nth, key, answer):
        held = await store.find(key)

        if answer is not None:
            assert held is not None and held.id == answer.id, f"cutting round trip {nth} answered with a run the key it was written under does not name"

            return

        if held is not None:
            assert await store.get(held.id) is not None, f"cutting round trip {nth} left '{key}' naming a run that is not there"

            return

        assert await store.add(Run(name="work", key=key, due_at=moment)) is not None, f"cutting round trip {nth} left '{key}' held by nothing, so that occurrence could never be written again"

    await every_round_trip(store, refused, prepared, called, checked)


@pytest.mark.parametrize("refused", REFUSALS)
@pytest.mark.parametrize("choice", ENDINGS)
async def test_an_ending_that_was_cut_is_the_one_that_landed_or_the_one_that_never_did(app, choice, refused):
    """an ending is a status and every field that status is read by, so a store writing those over more than one round trip could leave a run holding half of each — a lease dropped without the ending that belongs with it, or an attempt given back to a run still marked as running, which the next claim reads as something it never was"""
    store = app.store

    crossing(store, refused)

    moment = now()

    async def claimed():
        # everything waits in one queue, and what this sweep puts back comes due five seconds after the instant every claim here is made at — so the only run any of them ever reaches is the one just written
        await store.add(Run(name="work", due_at=moment, max_attempts=3))

        return (await store.claim("worker-1", ("default",), 1, timedelta(seconds=60), moment))[0]

    whole = await claimed()

    await closing(store, choice, whole.id, "worker-1", whole.attempts, moment, random.Random(SEED))

    landed = readable(await store.get(whole.id))

    async def prepared(nth):
        run = await claimed()

        return run, readable(await store.get(run.id))

    async def called(ready):
        run, _ = ready

        return await closing(store, choice, run.id, "worker-1", run.attempts, moment, random.Random(SEED))

    async def checked(nth, ready, answer):
        run, found = ready
        left = readable(await store.get(run.id))

        if answer:
            assert left == landed, f"cutting round trip {nth} of a {choice} answered that the ending landed and left a run it never reached"

            return

        assert left in (found, landed), f"cutting round trip {nth} of a {choice} left a run that is neither the one it found nor the one it was asked for"

    await every_round_trip(store, refused, prepared, called, checked)


@pytest.mark.parametrize("refused", REFUSALS)
async def test_a_reclaim_that_was_cut_took_back_exactly_what_it_answered_for(app, refused):
    """a batch half taken back is a lease dropped without the status that belongs with it, which is a run pending with a worker still named on it — and the count is what a worker logs and an operator reads, so one that took rows back and then stopped is a number nothing can be told from"""
    store = app.store

    crossing(store, refused)

    moment = now()
    written = []

    async def prepared(nth):
        queue = f"lane-{nth}"

        for _ in range(3):
            written.append((await store.add(Run(name="work", queue=queue, due_at=moment, max_attempts=3))).id)

        # the lease is already over where the claim is made, so every one of these is what a reclaim comes for
        await store.claim("worker-1", (queue,), 3, timedelta(seconds=-30), moment)

        return await holding(store, written)

    async def called(ready):
        return await store.reclaim(moment)

    async def checked(nth, ready, answer):
        left = await holding(store, written)

        assert len(ready - left) == (answer or 0), f"cutting round trip {nth} took back {len(ready - left)} runs while answering for {answer or 0}"

    await every_round_trip(store, refused, prepared, called, checked)


@pytest.mark.parametrize("refused", REFUSALS)
async def test_a_pruning_that_was_cut_leaves_no_key_behind_the_run_it_dropped(app, refused):
    """what is left of a run a pruning dropped is the key that named it, and a key held by nothing is an occurrence no worker of any machine could write again. the run, the entry that orders it and the reservation are three writes, and a pruning that made only some of them is the failure a write cut halfway leaves, met from the other end"""
    store = app.store

    crossing(store, refused)

    moment = now()
    over = moment - timedelta(minutes=5)

    async def prepared(nth):
        queue = f"lane-{nth}"
        keys = [f"gone-{nth}-{index}" for index in range(3)]

        for key in keys:
            await store.add(Run(name="work", queue=queue, key=key, due_at=moment))

            taken = await store.claim("worker-1", (queue,), 1, timedelta(seconds=60), moment)

            await store.complete(taken[0].id, "worker-1", taken[0].attempts, over, None)

        return keys

    async def called(keys):
        return await store.purge(moment, 100)

    async def checked(nth, keys, answer):
        for key in keys:
            held = await store.find(key)

            if held is not None:
                assert await store.get(held.id) is not None, f"cutting round trip {nth} left '{key}' naming a run that is not there"

                continue

            assert await store.add(Run(name="work", key=key, due_at=moment)) is not None, f"cutting round trip {nth} dropped the run '{key}' named and left the key held by nothing"

    await every_round_trip(store, refused, prepared, called, checked)
