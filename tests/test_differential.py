"""one script of operations, answered by every store and compared against the same script answered in memory. what a store promises is the same wherever the rows live, and a suite written by hand only ever asks the questions somebody thought to ask — this asks the ones nobody did"""

import random
from datetime import timedelta

import pytest

from queuefy.clock import now
from queuefy.retry import RetryPolicy
from queuefy.run import Run, RunStatus
from queuefy.store.memory import MemoryStore

QUEUES = ("default", "email", "reports")

# every way a store is asked to change a run, drawn one at a time. writing and claiming are drawn twice as often, because everything else needs a run to work on
OPERATIONS = ("add", "add", "claim", "claim", "complete", "fail", "retry_later", "release", "cancel", "reclaim", "purge", "heartbeat", "find")

STEPS = 150

# what a run is drawn from, held out here so the call that writes one stays on a line
NAMES = ("work", "grüßen 😀")
PRIORITIES = (-5, 0, 3, 5)

# spans a single precision float cannot hold, because a tidy 1.5 is the same number either way and says nothing: mysql held these three columns as a `FLOAT` and read 12345.678 back as 12345.7, which every value this script drew was too round to notice
TIMEOUTS = (None, 1.5, 12345.678)
DELAYS = (2.5, 3599.999999, 0.30000000000000004)

POLICIES = tuple(RetryPolicy)
RESULTS = (None, {}, {"ok": True}, {"total": 2**64 - 1})

# arguments that are not a mapping, because a store is handed whatever run it is given: the read of one of them turned everything falsy into no arguments at all, where memory and redis read back what they were handed
PAYLOADS = ({}, [], "", 0, {"n": -(2**63)})

# what makes a run single, and pairs of them that only read alike: every key here was lowercase and unaccented, so the collation mysql builds a column with by default folded two occurrences into one row and nothing in this script ever asked
KEYS = (None, "slot-0", "slot-1", "slot-2", "Slot-0", "SLOT-1", "café-0", "cafe-0")

# and what a run is looked for by, which is every key one may be written under. nothing at all is not among them: it is refused where the call is made, because no two stores look for it the same way
NAMED = tuple(key for key in KEYS if key is not None)

# the state a run is written in, drawn mostly pending because that is what an enqueue writes and because everything else in the script needs one of those to work on. a store is handed whatever run it is given and every other one reads that state straight back, where redis put all of them in the queue lane alike
STATES = (RunStatus.PENDING, RunStatus.PENDING, RunStatus.PENDING, RunStatus.RUNNING, RunStatus.DONE, RunStatus.FAILED, RunStatus.CANCELED)


def state_of(dice: random.Random, base) -> dict:
    """the state a run is written in, and the fields that state is read by. a state without them is one nothing ever reads again — a lease that is not there is a run no reclaim takes back, and an ending that never happened is one no pruning drops — so both are drawn either side of the instant the script runs at"""
    status = dice.choice(STATES)

    if status == RunStatus.PENDING:
        return {"status": status}

    if status == RunStatus.RUNNING:
        return {"status": status, "attempts": 1, "worker": f"worker-{dice.randint(1, 3)}", "started_at": base, "lease_until": base + timedelta(seconds=dice.choice([-5, 60]))}

    return {"status": status, "attempts": 1, "finished_at": base + timedelta(seconds=dice.choice([-60, 30]))}


def readable(run: Run | None):
    """every field a store writes and reads back, because one it quietly forgets is a policy the worker stops honouring — the instants included, because they are the field a store is easiest to get wrong and the two worst drifts found so far were both in one. `created_at` is the one left out: every store is handed a run of its own, and that field is stamped off the clock as each of them is built"""
    if run is None:
        return None

    return (run.name, run.queue, run.key, run.status.value, run.priority, run.attempts, run.max_attempts, run.timeout, run.retry_policy.value, run.retry_delay, run.max_retry_delay, run.worker, run.error, run.error_type, run.result, run.payload, run.due_at, run.lease_until, run.started_at, run.finished_at)


async def written_by(store, dice: random.Random, base, step: int):
    due_at = base + timedelta(seconds=dice.choice([-30, 0, 5, 500]))
    key = dice.choice(KEYS)
    payload = dice.choice(PAYLOADS) if dice.random() < 0.3 else {"step": step, "who": "münchen 😀"}
    delay, ceiling = dice.choice(DELAYS), dice.choice(DELAYS)
    state = state_of(dice, base)

    return await store.add(Run(name=dice.choice(NAMES), queue=dice.choice(QUEUES), key=key, priority=dice.choice(PRIORITIES), max_attempts=dice.randint(1, 3), due_at=due_at, timeout=dice.choice(TIMEOUTS), retry_policy=dice.choice(POLICIES), retry_delay=delay, max_retry_delay=ceiling, payload=payload, **state))


async def closing(store, choice: str, run_id, worker: str, attempt: int, moment, dice: random.Random) -> bool:
    if choice == "complete":
        return await store.complete(run_id, worker, attempt, moment, dice.choice(RESULTS))

    if choice == "fail":
        return await store.fail(run_id, worker, attempt, moment, "boom", "Error")

    if choice == "retry_later":
        return await store.retry_later(run_id, worker, attempt, moment + timedelta(seconds=5), "boom", "Error")

    if choice == "release":
        return await store.release(run_id, worker, attempt, moment + timedelta(seconds=5), "gone", "UnknownTask")

    if choice == "cancel":
        return await store.cancel(run_id, moment)

    return await store.heartbeat(run_id, worker, attempt, timedelta(seconds=60), moment)


async def script(store, seed: int, base):
    """the same sequence every store is asked, with the ids each of them hands out mapped back to the order they were written in — because an id is the one thing a store is allowed to name for itself"""
    dice = random.Random(seed)
    seen = []
    trail = []

    for step in range(STEPS):
        moment = base + timedelta(seconds=step)
        choice = dice.choice(OPERATIONS)

        if choice == "add":
            written = await written_by(store, dice, base, step)
            trail.append((step, choice, written is not None))

            if written is not None:
                seen.append(written.id)

            continue

        if choice == "claim":
            served = tuple(dice.sample(QUEUES, dice.randint(1, 3)))
            taken = await store.claim(f"worker-{dice.randint(1, 3)}", served, dice.randint(1, 4), timedelta(seconds=dice.choice([-1, 60])), moment)
            trail.append((step, choice, [readable(run) for run in taken]))

            continue

        if choice == "reclaim":
            trail.append((step, choice, await store.reclaim(moment)))

            continue

        if choice == "purge":
            trail.append((step, choice, await store.purge(moment, 50)))

            continue

        if choice == "find":
            trail.append((step, choice, readable(await store.find(dice.choice(NAMED)))))

            continue

        # everything left needs a run to work on, and it is drawn by the order it was written in so the same step names the same run in every store
        if not seen:
            continue

        # the attempt is drawn as the worker is, because what holds a run is the two of them together and a store has to answer the same way for a caller on the wrong one
        index = dice.randrange(len(seen))
        trail.append((step, choice, index, await closing(store, choice, seen[index], f"worker-{dice.randint(1, 3)}", dice.randint(1, 3), moment, dice)))

    return trail, [readable(await store.get(run_id)) for run_id in seen], await store.count(), [await store.count(status=status) for status in RunStatus], [await store.count(queue=queue) for queue in QUEUES]


@pytest.mark.parametrize("seed", range(8))
async def test_every_store_answers_the_same_script_the_same_way(store, seed):
    """a store that drifts from the others is a promise the library keeps on one backend and breaks on another, and nothing in an application would ever say which"""
    reference = MemoryStore()
    await reference.setup()

    base = now()

    assert await script(store, seed, base) == await script(reference, seed, base), f"{type(store).__name__} answered the script differently from the store the whole library is defined by"
