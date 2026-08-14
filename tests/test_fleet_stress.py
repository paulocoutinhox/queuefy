"""many machines against one real server, with leases running out under them the whole time. this is the load nothing else in the suite puts on a store: a claim racing a claim is settled by one conditional write, and a claim racing a reclaim is settled by two, on rows both of them are already holding"""

import asyncio
import json
import os
import subprocess
import sys
from collections import Counter
from datetime import timedelta
from pathlib import Path

import pytest
import pytest_asyncio

from queuefy.clock import now
from queuefy.run import SETTLED, RunStatus
from queuefy.worker import Worker
from tests.conftest import PORTS, SERVERS, reachable, wait_until
from tests.fleet import QUEUES, TASKS, build_queue, empty

pytestmark = pytest.mark.stress

MACHINES = 6
BATCH = 40
ROUNDS = 10
SECONDS = 15.0

# workers sharing one process, which is the cheaper half of this file and the one that carries the most pressure per second. its numbers are smaller than the fleet's on purpose: past this much sustained contention innodb rolls transactions back faster than any bounded retry budget can absorb, and what a store refuses there is a lease handing the run back — at least once, which is what this library promises and not what this asks
PRESSING = 10
PRESSED_BATCH = 20
PRESSED_ROUNDS = 6

# one machine killed outright a round, each replaced by a fresh one. fewer rounds than the fleet above takes, because killing and replacing a process costs more than a batch does and what this asks is answered by every one of them
KILL_ROUNDS = 6

# and those machines outlive the whole round of killing several times over, because what says a machine was really holding runs when it went is that it died of the signal — which is the one thing a machine that had already ended on its own cannot say. the round is forty writes and a claim over each of them, so what makes it slow is a loaded runner and never a bug, and a margin measured against a laptop is one a shared runner spends on the release it cannot take back
KILLED_SECONDS = 30.0

# short, so a machine that stops answering is one the others take over from inside the run and not after it
LEASE = 4.0

# the claim of the machine that is already gone spends one, and what is left is what the fleet needs to finish the run
ATTEMPTS = 5

# generous enough for a loaded runner, and short enough that a machine that never starts is a failure and not a hang
PATIENCE = 180.0

ROOT = Path(__file__).resolve().parent.parent

# a real server only: the point is many processes against one store, which a file and a dictionary are not
STRESSED = [name for name in ("redis", "mysql", "postgres") if reachable(SERVERS[name], PORTS[name])]


@pytest_asyncio.fixture(params=STRESSED)
async def fleet(request, tmp_path):
    url = SERVERS[request.param]
    await empty(url)

    output = tmp_path / "done"
    output.mkdir()

    app, closing = build_queue(url, str(output), ATTEMPTS)
    await app.setup()

    yield app, url, output

    await closing()


def start_machines(url: str, output: Path, count: int, first: int = 0, seconds: float = SECONDS) -> list:
    """the output of every machine is kept, so one that never starts says why instead of failing in silence. the numbering carries on from `first`, because a machine started to replace one that was killed must not write over the log of whoever it replaced. how long each of them polls for is asked of the caller, since a test that waits its machines out and one that kills them want opposite things of it"""
    environment = os.environ | {"PYTHONPATH": str(ROOT)}
    logs = [(output.parent / f"machine-{index}.log").open("w") for index in range(first, first + count)]
    settings = {"url": url, "output": str(output), "seconds": seconds, "attempts": ATTEMPTS, "lease": LEASE, "concurrency": 4, "queues": list(QUEUES)}

    return [(subprocess.Popen([sys.executable, "-m", "tests.machine", json.dumps(settings)], cwd=ROOT, env=environment, stdout=log, stderr=subprocess.STDOUT), log) for log in logs]


def told(output: Path) -> str:
    return "\n".join(path.read_text(errors="ignore") for path in sorted(output.parent.glob("machine-*.log")))


def executions(output: Path) -> list[str]:
    return [path.name for path in output.iterdir() if path.name.startswith("run-")]


async def abandon(app, written: int, batch: int, queues: tuple[str, ...] = ("default",)) -> list:
    """a batch written and taken by a machine that is already gone, so every pass of every machine has expired leases to take back while the others are claiming. it answers with the runs it wrote, because reading one is how anybody asks what became of it"""
    runs = []

    for marker in range(written, written + batch):
        runs.append(await app.enqueue(TASKS[marker % len(queues)], marker=f"run-{marker:05d}"))

    await app.store.claim("a-machine-that-died", queues, batch, timedelta(seconds=-LEASE), now())

    return runs


def broken(run, settled_before: set) -> list:
    """what has to be true of one run whatever is being done to it, and by however many machines at once. what every other test here counts is executions, which say a run was handed out twice — and say nothing at all about a run left in a state no line of this library ever writes"""
    faults = []

    if not 0 <= run.attempts <= run.max_attempts:
        faults.append(f"{run.id} has been tried {run.attempts} times out of the {run.max_attempts} it was allowed")

    if run.status == RunStatus.RUNNING and (run.worker is None or run.lease_until is None):
        faults.append(f"{run.id} is running and nothing says who holds it or until when")

    if run.status != RunStatus.RUNNING and (run.worker is not None or run.lease_until is not None):
        faults.append(f"{run.id} is {run.status.value} and still says {run.worker} holds it until {run.lease_until}")

    if run.status in SETTLED and run.finished_at is None:
        faults.append(f"{run.id} is over and says nothing about when it ended")

    if run.id in settled_before and run.status not in SETTLED:
        faults.append(f"{run.id} was over and is {run.status.value} again")

    return faults


async def sweep(store, written: list, faults: list, settled_before: set) -> None:
    """one look at every run there is, and what it finds is added to what the look before it found"""
    for run in list(written):
        held = await store.get(run.id)

        if held is None:
            faults.append(f"{run.id} is not in the store at all, and nothing here prunes")

            continue

        faults.extend(broken(held, settled_before))

        if held.status in SETTLED:
            settled_before.add(held.id)


async def audit(store, written: list, stopping: asyncio.Event, faults: list, settled_before: set) -> None:
    """reads every run over and over while the fleet works them, because a run in a state nothing writes is only ever there between two writes — an ending read afterwards is the state it settled in and never the one it passed through"""
    while not stopping.is_set():
        await sweep(store, written, faults, settled_before)

        await asyncio.sleep(0.05)


async def test_a_fleet_under_leases_running_out_hands_every_run_to_exactly_one_machine(fleet):
    app, url, output = fleet
    machines = start_machines(url, output, MACHINES)
    written = []

    for _ in range(ROUNDS):
        written += await abandon(app, len(written), BATCH)

        await asyncio.sleep(0.4)

    for process, log in machines:
        code = process.wait(timeout=PATIENCE)
        log.close()

        assert code == 0, f"a machine ended with {code}:\n{told(output)}"

    # the machines are gone, and whatever their leases still hold is taken back and finished by one survivor
    survivor = Worker(app, concurrency=8, poll=0.05, lease=timedelta(seconds=30))
    polling = asyncio.create_task(survivor.run())

    await wait_until(lambda: len(executions(output)) >= len(written))

    survivor.stop()
    await polling

    done = Counter(name.rsplit(".", 2)[0] for name in executions(output))

    assert sorted(done) == [f"run-{marker:05d}" for marker in range(len(written))], f"every run was executed:\n{told(output)}"
    assert [marker for marker, count in done.items() if count > 1] == [], "and not one of them twice"
    assert await app.count(status=RunStatus.FAILED) == 0
    assert await app.count(status=RunStatus.RUNNING) == 0

    # the work really was shared, and did not all land on whoever started first
    assert len({name.rsplit(".", 2)[1] for name in executions(output)}) > 1


async def test_a_fleet_serving_many_queues_still_hands_every_run_to_exactly_one_machine(fleet):
    """a claim spans every queue a machine serves and every priority inside them, which is one ordering read out of several places at once. it is the path the fleet above never walks: one queue at one priority asks nothing of the merging, and a run named by two lanes at once is a run two machines run"""
    app, url, output = fleet
    machines = start_machines(url, output, MACHINES)
    written = []

    for _ in range(ROUNDS):
        written += await abandon(app, len(written), BATCH, QUEUES)

        await asyncio.sleep(0.4)

    for process, log in machines:
        code = process.wait(timeout=PATIENCE)
        log.close()

        assert code == 0, f"a machine ended with {code}:\n{told(output)}"

    survivor = Worker(app, queues=QUEUES, concurrency=8, poll=0.05, lease=timedelta(seconds=30))
    polling = asyncio.create_task(survivor.run())

    await wait_until(lambda: len(executions(output)) >= len(written))

    survivor.stop()
    await polling

    done = Counter(name.rsplit(".", 2)[0] for name in executions(output))

    assert sorted(done) == [f"run-{marker:05d}" for marker in range(len(written))], f"every run of every queue was executed:\n{told(output)}"
    assert [marker for marker, count in done.items() if count > 1] == [], "and not one of them twice"
    assert await app.count(status=RunStatus.FAILED) == 0
    assert await app.count(status=RunStatus.RUNNING) == 0

    # every queue really carried some of it, because a run that all landed in one lane asks the merging nothing
    for queue in QUEUES:
        assert await app.count(queue=queue) > 0, f"'{queue}' carried none of the work"


async def test_workers_sharing_one_process_under_leases_running_out_settle_every_run(fleet):
    """the same interleaving without the process boundary, at a pressure the ordinary suite cannot carry: tracing costs an order of magnitude, and it was this load untraced that turned up a contention budget giving up while the burst was still going. a store that refuses an outcome leaves the run claimed and the work quietly done again a lease later, so what this asks is not only that nothing ran twice but that everything was written down"""
    app, url, output = fleet

    workers = [Worker(app, concurrency=4, poll=0.01) for _ in range(PRESSING)]
    polling = [asyncio.create_task(worker.run()) for worker in workers]
    written = []

    for _ in range(PRESSED_ROUNDS):
        written += await abandon(app, len(written), PRESSED_BATCH)

        await asyncio.sleep(0.05)

    await wait_until(lambda: len(executions(output)) >= len(written))

    for worker in workers:
        worker.stop()

    await asyncio.gather(*polling)

    done = Counter(name.rsplit(".", 2)[0] for name in executions(output))

    assert sorted(done) == [f"run-{marker:05d}" for marker in range(len(written))], "every run was executed"
    assert [marker for marker, count in done.items() if count > 1] == [], "and not one of them twice"

    # the interval task settles slots of its own beside all this, and every one of them is a run the store closed too
    ticks = len([path for path in output.iterdir() if path.name.startswith("tick.")])

    assert await app.count(status=RunStatus.DONE) == len(written) + ticks, "and every outcome reached the store, instead of a run left claimed for a lease to hand back"
    assert await app.count(status=RunStatus.RUNNING) == 0


async def test_a_fleet_losing_machines_outright_still_runs_every_job(fleet):
    """a lease running out is a machine that went quiet, and this is one that went away mid-sentence: killed while it holds runs, with whatever it had half written left exactly as it fell. what the suite has of this is one process against a file, and a fleet is where it matters — the survivors meet leases nobody will ever renew while they are claiming, beating and closing on the same rows. a hard kill is the one case where a run may happen twice, because the work can land and the ending never reach the store, so what is asked is that every run happened and that none was lost or given up on"""
    app, url, output = fleet
    machines = start_machines(url, output, MACHINES, seconds=KILLED_SECONDS)
    written = []
    replacements = MACHINES

    for round_number in range(KILL_ROUNDS):
        written += await abandon(app, len(written), BATCH)

        await asyncio.sleep(0.4)

        # killed and not stopped, so nothing of the worker runs: no drain, no ending written, and every run it held left on a lease nobody is going to renew
        dying, log = machines[round_number % MACHINES]
        dying.kill()
        code = dying.wait(timeout=PATIENCE)
        log.close()

        # the signal is what it died of, and a machine that had already ended on its own is a round this test did not test anything in
        assert code < 0, f"machine {round_number} ended with {code} before it could be killed, so nothing was holding runs when it went"

        # and one comes up in its place, the way an orchestrator replaces a pod, so the fleet meets what the dead one left while it is still filling up
        machines[round_number % MACHINES] = start_machines(url, output, 1, replacements, KILLED_SECONDS)[0]
        replacements += 1

    for process, log in machines:
        code = process.wait(timeout=PATIENCE)
        log.close()

        assert code == 0, f"a machine ended with {code}:\n{told(output)}"

    survivor = Worker(app, concurrency=8, poll=0.05, lease=timedelta(seconds=30))
    polling = asyncio.create_task(survivor.run())

    await wait_until(lambda: len(executions(output)) >= len(written))

    survivor.stop()
    await polling

    done = Counter(name.rsplit(".", 2)[0] for name in executions(output))

    assert sorted(done) == [f"run-{marker:05d}" for marker in range(len(written))], f"every run was executed, however many machines were killed under it:\n{told(output)}"
    assert await app.count(status=RunStatus.FAILED) == 0, f"and none of them was given up on:\n{told(output)}"
    assert await app.count(status=RunStatus.RUNNING) == 0, "and nothing was left claimed by a machine that is gone"


async def test_no_run_is_ever_left_in_a_state_nothing_here_writes(fleet):
    """every other test here asks whether the work happened once, which is a question an execution count answers. this asks the other one: whether a run is ever *in* a state no line of this library writes — tried more times than it was allowed, waiting while it still names a holder, running while it names none, or over and then not over. those are what a claim racing a reclaim racing a close leaves behind, and none of them shows up in a count of what ran. so somebody reads every run over and over while the fleet works them, because a state nothing writes only exists between two writes"""
    app, url, output = fleet

    workers = [Worker(app, concurrency=4, poll=0.01) for _ in range(PRESSING)]
    polling = [asyncio.create_task(worker.run()) for worker in workers]

    stopping, faults, settled_before, written = asyncio.Event(), [], set(), []
    watching = asyncio.create_task(audit(app.store, written, stopping, faults, settled_before))

    for _ in range(PRESSED_ROUNDS):
        written += await abandon(app, len(written), PRESSED_BATCH)

        await asyncio.sleep(0.05)

    await wait_until(lambda: len(executions(output)) >= len(written))

    for worker in workers:
        worker.stop()

    await asyncio.gather(*polling)

    stopping.set()
    await watching

    # one last look with nothing moving, because a sweep that ran mid-flight can also have missed the ending it was watching for
    await sweep(app.store, written, faults, settled_before)

    done = Counter(name.rsplit(".", 2)[0] for name in executions(output))

    assert sorted(done) == [f"run-{marker:05d}" for marker in range(len(written))], "every run was executed"
    assert faults == [], "and no run was ever seen in a state nothing here writes:\n" + "\n".join(faults[:10])
    assert settled_before == {run.id for run in written}, "and every one of them was watched all the way to an ending"
