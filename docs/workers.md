# 👷 Workers

A worker claims what is due and runs it. Any number of them may run against one store, on one
machine
or on twenty.

```python
from datetime import timedelta

from myapp.queue import app
from queuefy.worker import Worker

worker = Worker(app, concurrency=8, poll=1.0, lease=timedelta(seconds=60))
await worker.run()
```

| Option | What it decides |
| --- | --- |
| `queues` | which queues this worker serves |
| `concurrency` | how many runs it holds at once, and how many threads it keeps for the plain ones among them — a thread still inside a handler that ran past its timeout is counted out of it until it returns |
| `poll` | how long it waits between passes, which is also the delay a due task may see |
| `lease` | how long a claim is good for before another worker may take it over, and it has to be a span the instant it names exists for |
| `jitter` | at most how much of a retry delay is drawn at random on top of it, a quarter by default, and never below zero |
| `grace` | how long a shutdown waits for what is in flight before leaving it to the lease |
| `keep` | how long a run that is over is kept before it is pruned, a week by default, and `None` keeps everything |
| `name` | who it says it is, drawn from host, process and a random tail when left out |

A worker that could never work is refused where it is written: a poll of zero is not a wait, a
concurrency of zero never takes anything and one that is not a whole number is the limit every claim
is made with, a lease of zero has already run out, a lease wider than what is left between now and the
last instant a datetime holds names an instant nothing has — which is a claim that raises inside itself,
so the worker comes up, logs the pass and asks again and never takes a thing — and a worker serving
no
queues claims nothing while there is nothing in the queue that would ever explain why. A count of
cores halved is a float, and left to run time it came up, polled, raised on every pass and never took
a thing while it said so once a second. The queues are
a tuple and never a single name, refused in the same place: a string is a sequence of its letters, so
a worker given `emails` serves a queue for each letter of it and never the one it was named for —
which memory happens to survive and no other store does. They are a tuple and never something that
empties as it is walked either, because what a worker serves is read again on every claim it makes: a
generator written where a tuple was meant is emptied by the very check that answers for the names in
it, and every claim after that asks for no queues at all — which every store answers with nothing, so
the worker polls for ever and takes nothing while the runs it was named for wait in the queue. Each of
those fails quietly rather than loudly when it is only checked at run time.

The `poll`, the `jitter` and the `grace` are real numbers, refused in the same place when they are
not. Every check above is a comparison, and `nan` is false against all of them while `infinity` is
past every one, so both pass each one and become a jitter drawing a delay of `nan` — which raises
exactly where the ending of a run is being written — or a grace nothing in flight ever comes back
from.

## 🧹 What is over is pruned

**A queue nobody prunes grows for ever**, and what it grows by is rows nothing reads again: four
tasks
on five minute schedules write about a thousand runs a day, which is four hundred thousand a year
that
every count, every index and every backup carries.

So a worker prunes what is over and older than `keep` — done, failed, given up on and called off
alike.
It happens **once an hour** and never on the poll, and it takes a thousand at a time, so a
deployment
that was never pruned is caught up over some passes instead of in one statement that holds the
table.

> **A `keep=None` prunes nothing**, which is the right answer whenever those rows are the record. It is
> also the answer that grows for ever, and both of those are the point of saying it out loud. A `keep`
> below zero is refused where it is written: it asks for what is over to be dropped before it is over,
> which is every one of them. So is one wider than what lies between the first instant a datetime holds
> and now — the span is taken off the instant each pruning is asked at, so one past the range raises
> inside the pruning on the hour, every hour, while nothing that is over is ever dropped.

**A full batch means the next pass takes the next one**, and only a short one starts the hour — so a
deployment that ran a year unpruned catches up in passes instead of in a year of hours. The first
pruning of each worker is drawn somewhere inside the hour, for the same reason a retry delay is
drawn:
ten workers coming up together must not all reach for the same rows in the same instant.

**And housekeeping the store refuses never costs the pass.** Taking back what a dead worker left,
pruning what is over and writing the slots that have come due are all logged and the pass carries on
to the claim it was on its way to make, because housekeeping that ends a pass is a worker that stops
working for a reason nothing in the queue explains. The slot write is the one likeliest to be refused
for a reason a claim never meets: every worker of a fleet aims it at the same key in the same instant,
so one burst of that outlasting the retry budget used to end the pass of every one of them at once.

> **The key of a run goes with it.** A key is what makes a run single, so pruning a run frees the key
> it was written under — which is what you want for a cron slot two weeks old, and what to think about
> before giving a one-shot a key you intend to reuse.

## 🔄 One pass

```python
await worker.run_once()   # reclaim, prune, materialize, claim, start
await worker.drain()      # wait for what it started
```

The `run_once` returns the runs it claimed and starts them; it does not wait for them, and `drain`
is what
waits. Together they are the whole of `run`, and they are what a test should use instead of
sleeping.

## 🪪 Identity

A worker names itself `host:pid:draw`. The host tells two machines apart, the pid tells two
processes
apart, and the draw covers a pid the operating system handed out again after a restart. Everything
that decides ownership is conditional on that name.

> **A name is at most `WORKER_NAME_LIMIT` characters, and the host is what gives way.** A pod is named
> after its deployment, its namespace and its cluster, which is well past what a store keeps a worker
> name in — and a name that does not fit is not a worker that logs a warning, it is a worker whose
> every claim the database refuses while the process stays up and polls forever. So the host is cut to
> fit and the draw is what still tells two machines apart. A `name` you pass yourself is refused where
> it is written if it is longer than that.

> **So is a name no store could write down**, exactly as the queues a worker serves are, and for the
> same reason: the name is in every claim, every beat and every ending it writes. It is not a name
> anybody had to type either — the host comes from `socket.gethostname()`, which carries whatever bytes
> the machine had, and Python reads the ones that were never UTF-8 back as lone surrogates. Memory
> claims under such a name without a word while SQLite, MySQL, PostgreSQL and Redis each refuse it deep
> inside a driver, so what comes up is a worker that logs the pass, waits, asks again, and never takes
> anything at all. Anything that is not text at all is refused in the same place, and so is a queue that
> is not: what reads a name is the encoder, and one that reaches it instead of a guard raises about a
> method rather than about a name.

> **The `lease` and the `keep` are spans and never numbers of seconds.** Both are read by asking a
> `timedelta` how many seconds it is, so a `lease=60` — the mistake anybody makes once — used to raise
> about a method that an integer does not have, from inside the worker being built. It is refused where
> it is written now, with a sentence saying a span is a `timedelta`.

## ⏳ Leases and a worker that dies

A claim is good for `lease`. While a task runs, the worker pushes its own lease every third of that
period, so a run that takes an hour is never taken from under it.

A process that is killed pushes nothing. Its lease runs out, the next pass of any worker notices,
and
the run goes back to the queue — unless its attempts are spent, in which case it ends as failed with
`LeaseExpired`. This is why `max_attempts=1` and a task that must not run twice are a pair: a run
that
was interrupted halfway is indistinguishable from one that never started.

## 🌩️ When the store blinks

A pass that raises is logged and the loop carries on — one bad minute never ends a worker. A run
whose
close never reached the store stays claimed until its lease expires, and then comes back like any
other abandoned run.

That is why **the store client needs its own timeouts**. A client that waits forever turns a network
blip into a worker that is alive, polling nothing and answering nobody. See
[Stores](stores.md) for what each one needs.

## 🛑 Shutting down

```python
worker.stop()
```

The `stop` ends the polling loop and `run` then waits for what is in flight, for up to `grace`
seconds.
Nothing new is claimed after `stop`, so a deploy loses no work.

A run still going when the grace runs out is left where it is and said out loud. Its lease is what
brings it back — to this worker's replacement, or to any other. **A shutdown always ends**, because
one that waits forever is a deploy that never finishes.

**The threads go with the worker.** The pool a worker keeps for its plain handlers is its own, and
threads waiting on one wait for ever, so a pool nobody closes is every one of them left alive for as
long as the process is — an api that runs a worker in its lifespan and comes back up on a reload
holds both sets. It is closed once the grace is over and never waited on: a plain handler is one no
code here can end, and what is still in flight is what the leases bring back.

That close happens however the polling ends, and not only when `stop` was what ended it. Being
cancelled is the other half of how a worker is stopped — a task group, a supervisor and
`asyncio.run` with the polling still pending every one of them cancels rather than asks — and a
worker stopped that way is as gone as one that was asked, so its threads go with it either way.

**A `timeout` on a plain handler bounds the waiting and never the work.** Python cannot end a thread
from outside, so a handler that ran past its timeout carries on to its own end while the worker has
already written the run down and handed the slot back. The thread it is on belongs to nobody else
until it returns, and **the worker counts it out of its concurrency for as long as that lasts** —
a slot counted free against a thread that is not is a run claimed onto a pool with nothing left to
run it on, waiting there for a thread while its own timeout, which starts where the work was handed
over, runs out on work that never began. Measured on a concurrency of four against four handlers that
each ran four times their timeout and then returned: eight of the twenty runs behind them were
written down as having taken too long, their attempts spent, with their handler never entered.

So a worker that meets one of those says so once, in a line naming the task and how many fewer runs
it now holds, and goes on with what it can actually run. One whose every thread is inside a handler
that never returns claims nothing rather than claiming what it cannot start, and the work waits in
the queue for a worker that can. What that costs is throughput, which the depth of the queue shows;
what it used to cost was the runs themselves. The call `Worker.holding()` is that number if you would
rather watch it than read for it.

**The way out is a deadline inside the work** — the timeout of the socket, of the driver, of the
request — or an `async` handler, where cancelling really does end it. A coroutine never touches the
pool at all.

## 🕰️ The clocks have to agree

Every worker asks **its own machine** what time it is, and everything that decides who holds what is
a
timestamp: when a run is due, and when a lease runs out. Workers never talk to each other, so a
machine
whose clock is wrong does not disagree with anybody — it simply acts on a different now.

A machine running ahead by more than a `lease` treats a lease that is very much alive as expired.
Measured with five minutes of skew and a sixty second lease: it took a run back from a worker that
was
still working on it, and the outcome of the worker that finished was dropped.

**So keep the clocks in sync — NTP, and nothing more exotic than that.** A `lease` comfortably
longer
than the drift you could ever have is the second half of it, and the default of sixty seconds is
already far outside what a synced machine ever drifts.

## 📐 How many

Start with `concurrency` around what your task's blocking profile justifies and one worker per
process. Ten processes each holding eight runs is eighty at once, which the store handles with one
`UPDATE` per claim.

Splitting by queue is how a heavy task is kept away from a light one. Splitting by machine needs
nothing: workers coordinate through the store and never talk to each other.
