# 🔁 Retries and failures

## 🧭 What an attempt becomes

| What the handler did | What happens |
| --- | --- |
| returned | the run is done, and a dictionary it returned is kept as the result |
| returned what no store can write | the run ends as failed **now**, with `UnwritableAnswer` |
| raised, attempts left | the run comes back, due after the policy's delay |
| raised, attempts spent | the run ends as failed, with the message and the class that broke |
| raised `PermanentError` | the run ends as failed **now**, however many attempts were allowed |
| ran past its `timeout` | the worker stops waiting for it and treats that as a failure that may be retried |
| never started, because its `timeout` passed while the work was still queued for a thread | the run goes back to the queue with `WorkNeverStarted`, **and the attempt is given back** |
| the worker died | the lease runs out and the run comes back, or ends as failed when nothing is left |
| raised `SystemExit` or `KeyboardInterrupt` | the run ends as failed, and the worker keeps going |
| was cancelled | the cancellation is passed on, and the lease is what brings the run back |

A pass that works clears what the pass before it wrote, so a run that succeeded on its third attempt
carries no error.

## 📊 Policies

```python
from myapp.queue import app
from queuefy.retry import RetryPolicy


@app.task("send_email", max_attempts=5, retry_policy=RetryPolicy.EXPONENTIAL, retry_delay=5)
async def send_email(to: str):
    ...
```

| Policy | Waits before attempt 2, 3, 4 |
| --- | --- |
| `FIXED` | 5, 5, 5 |
| `LINEAR` | 5, 10, 15 |
| `EXPONENTIAL` | 5, 10, 20 |
| `EXPONENTIAL_JITTER` | the above, plus a **drawn** fraction of it, up to the worker's `jitter` |

> **A policy is one of these four and never the string that spells it.** Every store that writes a run
> out asks the policy for the value it is written under, and memory keeps the object it was handed —
> so a task declared with a plain string ran for as long as an application was built against memory
> and then raised on the enqueue itself everywhere else. It is refused where the task is declared.

**No wait is longer than `max_retry_delay`**, an hour by default, and a ceiling of zero or less is
refused where the task is declared: it asks for a retry due before the attempt that failed, which is
hammering rather than backing off. Doubling has no ceiling of its own, and five seconds over twenty
attempts is a retry a month away — which nobody ever meant to ask for.

Jitter matters when a shared dependency falls over: without it every run that failed in the same
second comes back in the same second.

> **The fraction is drawn per run, and that is the whole point.** Ten thousand runs work the delay out
> from the same numbers, so a fixed multiplier — even a large one — hands the herd back whole an hour
> later. Below the ceiling the draw only ever adds, so backing off is still backing off.
>
> **The ceiling is what the draw happens under, and never what the drawn delay is cut down to.** A herd
> that has doubled its way past `max_retry_delay` would otherwise work out the very same wait every
> time, which is the one case jitter exists for and the one case it used to do nothing in. So the
> growth is held under the ceiling first and spread afterwards: the wait still lands within
> `max_retry_delay`, and it still lands somewhere different for each of them.

## 🚚 A name this worker has never heard of

A rolling deploy runs two versions at once, so the older replica meets runs the newer one enqueued
for
tasks it does not declare. **That never costs the run an attempt and never ends it**: the worker
hands
it straight back to the queue with `UnknownTask` written on it, and the replica that knows the name
picks it up.

It has to work that way because `max_attempts` is **one** by default — counting it would mean the
older
replica destroying a perfectly good run, silently, every time it got there first.

The claim is what spends the attempt, so handing the run back is what gives it back. A run left
sitting
on an attempt it never used is a run the first reclaim that meets it ends for good, and after a
deploy
that bounced it three times a task allowed three attempts would die on its first real failure.

> **A name nobody knows waits instead of dying**, which is the other side of that. It comes back at the
> policy's first delay each time, because nothing is counting up behind it to back off from — so a typo
> sits in the queue, asked for once every `retry_delay`, where an operator can see it and cancel it.

That path belongs to the **lookup** and never to the handler. A handler that raises `UnknownTask`
itself — one fanning out to a task nobody registered — has a bug, and it is retried and ended by the
policy like any other failure. Reading it as a rolling deploy instead would hand the run back with
its
attempt given back for ever, repeating on every poll everything the handler managed before it
raised.

## 🧵 A handler that was never called at all

A plain handler runs on a thread, and its `timeout` starts where the work is handed over and not
where a thread picks it up. So a run whose timeout passes while it is still queued for one has run
**no line of its handler** — and writing that down as work that took too long is a message naming a
task nobody called, an attempt spent on nothing, and, with `max_attempts` at one, the work gone.

The worker tells the two apart by cancelling the work rather than by looking at it: the cancel is
taken only while no thread has picked the work up, and from that moment none ever will. When it is
taken, nothing was attempted — so the run goes back to the queue with `WorkNeverStarted` and **the
attempt is given back**, exactly the way a name this worker does not declare does.

What the failure is called can never decide this. A handler that raises `TimeoutError` out of a
socket, a driver or a request of its own has run the whole of its work and then said so, and reading
that as a wait nobody was serving would hand its attempt back for ever and repeat everything it did
before it raised.

> Seeing `WorkNeverStarted` at all means threads are the thing this worker is short of, and
> [workers](workers.md) is where that is explained.

## 🛑 A handler that asks the process to stop

Neither `SystemExit` nor `KeyboardInterrupt` is an `Exception`, and asyncio never swallows them
inside a
task — it hands them to the event loop, which ends the **whole worker** and every run it was
holding.
A library calling `sys.exit()` somewhere deep would do exactly that.

So they are caught, the run ends as failed, and the worker carries on. One handler does not get to
take down the runs beside it, and it is not something another attempt would fix either.

The one exception to the rule is `asyncio.CancelledError`: it is passed on untouched, because that
is
the shutdown asking and a worker that swallowed it would be one nobody can stop.

## 🚫 Never retry this one

```python
from myapp.queue import app
from queuefy.errors import PermanentError


@app.task("charge", max_attempts=5)
async def charge(account_id: int, cents: int):
    if cents <= 0:
        raise PermanentError("a charge of nothing is a bug and not a blip")
```

A malformed payload does not get better by being tried four more times, and a card that was declined
is a decision and not an outage.

## ⏱️ Timeouts

```python
@app.task("transcode", timeout=3600)
async def transcode(path: str):
    ...
```

Without a timeout a run may run forever, holding a slot of the worker's concurrency. With one, the
worker stops waiting and the failure is retried like any other. Set `timeout` below `lease` only if
you
would rather the timeout fire than the lease.

> **A timeout stops the waiting, and only a coroutine stops the work.** A plain handler runs in a
> thread, and Python cannot end a thread from outside — so `wait_for` cancels the await while the
> function carries on to its own end. Measured: a plain handler with `timeout=0.2` and a retry ran
> **twice at once**, and both copies finished.
>
> So on a plain handler a timeout is a promise about the worker and never about the work. Where the
> work itself has to stop, the handler has to be a coroutine — or it has to watch its own deadline and
> give up on its own.

> **And a plain handler that never returns keeps the whole process alive.** The threads a pool runs on
> are joined when the interpreter goes down, so the process waits for the handler after the worker has
> already given up on it and returned: the shutdown finishes, the loop does not close, and the container
> has to be killed rather than stopped. Measured: a worker with `grace=0.2` returned in a fifth of a
> second and the process was still running fourteen seconds later. Python cannot end a thread, so the
> deadline has to be inside the work — a socket timeout, a request timeout, a driver timeout. A plain
> handler that can block for ever is the one thing this library cannot take back from you.

**The threads a worker runs plain handlers on are its own**, one for every run it may hold, made as
they are needed. The pool asyncio hands out by default is sized for the whole process at
`min(32, cores + 4)` and shared with everything else in it — six on a two core container, under the
eight runs a worker holds by default. The runs past it used to wait in a queue while the timeout, which
starts where the run was claimed, ran out on work that had never begun: written down as having taken
too long, the attempt spent, the handler never called.

## 🌩️ When the outcome never reaches the store

A run whose close could not be written stays claimed until its lease expires, and then comes back
like
any other abandoned run. The worker says so in its log — otherwise a store that blinked shows up as
a
run executed twice with nothing anywhere explaining why.

A heartbeat that cannot reach the store is never the outcome of a run either. It is logged and the
loop carries on: the lease is what says a worker is still here, and losing it is exactly how the run
comes back to somebody else.

> **A failure is written down in what a store keeps for one.** The message and the class that raised
> it are whatever the code being run put in them, so unlike a name or a key there is nowhere to refuse
> one where it is written — and past the column MySQL refuses the message while PostgreSQL refuses the
> class. The ending then never reaches the store at all: the run stays claimed, and every lease after
> that runs the very same handler again. So the worker cuts both to `ERROR_LIMIT` and
> `ERROR_TYPE_LIMIT` on its way in, and what was cut ends in `...` rather than reading as the whole of
> it.

> **The length is not the only thing a store refuses.** A path is bytes, and read back off a POSIX
> system it carries whatever bytes it had as lone surrogates — which are not UTF-8 at all, so a
> handler that opened a file somebody uploaded from another machine raises with one of them in the
> message, and SQLite, MySQL, PostgreSQL and Redis every one of them refuse it. A NUL byte is one
> PostgreSQL refuses on its own. The worker escapes both into the text that says what they were, and
> it does that **before** it cuts, because escaping is what makes a message longer than the column.
> The class is only ever cut: Python refuses to name one with a character UTF-8 cannot hold, so there
> is nothing there to escape.

> **What the handler answered is settled where it is written, exactly as the arguments are.** A result
> comes out of the code being run, so there is nowhere to refuse one at the call — and left to the
> store the divergence is the same one the arguments are already settled against. A path read back off
> a filesystem carries its bytes as lone surrogates, which MySQL refuses inside a JSON value; `nan` and
> `infinity` are words only Python's own reader of JSON has, so SQLite and Redis hold what MySQL and
> PostgreSQL refuse; an object no serialiser takes is one memory keeps and every other store throws the
> ending away over; and a key that is not a string reads back as one everywhere but in memory. So the
> worker escapes every string on its way in, by the same escaping a message gets, and then writes the
> answer out and reads it again — one value, meaning one thing, wherever the runs live.

> **An answer no store could write down ends the run where it happened.** It is the handler that has
> the bug, and no attempt makes it answer something else, so the run ends as failed with
> `UnwritableAnswer` however many attempts were allowed. Left to the store the ending never lands at
> all: the run stays claimed, the lease hands it back, and the very same handler runs again on every
> one of them until the attempts are spent — under a message about a worker that stopped answering,
> which is not what happened. An answer that holds itself is refused there too: the escaping walks it
> before anything writes it, and what a structure referring back to itself raises there is a
> `RecursionError` rather than either of the two refusals a value no store takes raises.

## 🎯 Exactly once, or at least once

This library is **at least once**, like every queue that survives a power cut. A run that was
claimed
and executed but whose worker died before recording the outcome will be executed again.

Make the handler idempotent — that is the only real answer, and it is cheap: a payment keyed by an
idempotency key, a file written to a name derived from the payload, an `INSERT` guarded by a unique
constraint. For work that genuinely must not repeat, `max_attempts=1` turns the second execution
into
a failure instead.
