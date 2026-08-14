# Queuefy

Standing context for anybody — human or model — working on this repository. Read it before writing a
line, and keep it true when the code moves.

---

## 1. What this is

An asynchronous task queue **and** scheduler for Python, where one task runs on exactly one worker.
Pure `asyncio`, no broker, no scheduler process, no leader election, no external coordination.

Everything reduces to a single row: **a run that is due, which exactly one worker claims.** An
immediate task, an interval, a fixed datetime and a cron expression differ only in *when the next run
is written*, and in nothing else.

The core package has **zero dependencies**. SQLAlchemy and Redis are optional extras.

| Question | Answer |
| --- | --- |
| who runs a due run? | whoever wins a conditional `UPDATE ... WHERE status = 'pending'` — the store decides |
| what stops ten workers writing ten copies of the 04:00 run? | a unique key per occurrence, `name@2026-08-01T04:00:00+00:00` |
| what happens when a worker dies mid-run? | its lease runs out and the run goes back to the queue |
| what elects the scheduler? | nothing — every worker writes the next occurrence, and the key leaves one |

### What it deliberately does not do

- It is not a broker. Delivery is polled, so a run becomes due within one poll interval, not within a microsecond.
- It does not spread one run across workers. One run, one worker.
- It is not a result store. A run holds a small JSON result; anything bigger belongs where your data lives.
- It is **at least once**, like every queue that survives a power cut. Handlers must be idempotent.

---

## 2. Layout

```
src/queuefy/
  __init__.py        empty, always — nothing is ever placed here
  app.py             Queuefy: the registry of tasks and the entry point for enqueueing
  worker.py          Worker: the polling loop, the lease heartbeat, the hooks
  run.py             Run dataclass and RunStatus
  task.py            Task dataclass (frozen)
  trigger.py         Trigger, Interval, Cron
  cron.py            POSIX five-field parser and the next-match search
  retry.py           RetryPolicy and the delay arithmetic
  clock.py           now(), as_utc(), naive_utc(), real(), spanned(), waited(), kept(), EPOCH, WIDEST_SLOT, EARLIEST_SLOT, WIDEST_SECONDS — the only place time is decided
  errors.py          QueueError, and UnknownTask, PermanentError, CronError, WorkNeverStarted and UnwritableAnswer under it — one family, so `except QueueError` is every failure this library raises
  asgi.py            lifespan_for(worker) — the lifespan protocol, which every asgi framework speaks
  store/
    __init__.py      empty, always
    base.py          the abstract Store contract and the constants every store shares
    memory.py        MemoryStore
    sqlalchemy.py    SqlAlchemyStore (PostgreSQL, MySQL, SQLite)
    redis.py         RedisStore (Lua scripts)

tests/               pytest, asyncio_mode=auto, parametrized over every reachable store
docs/                the prose, kept honest by tests/test_docs.py
```

`src` layout, built with hatchling. `pyproject.toml` is the single source of tooling config.

---

## 3. The domain model

### Task — what a name means

Frozen dataclass. Declared once, at import time, and never mutated.

```
name, handler, queue, trigger, max_attempts, timeout,
retry_policy, retry_delay, max_retry_delay, priority
```

The **name** is what travels in the store, so it stays stable while the function behind it moves, is
renamed or changes module. `Queuefy.register` refuses a duplicate name.

### Run — one execution of one task

The only thing a worker ever claims. Every field of it is written by the store and read back
unchanged, and a field a store forgets is a policy the worker silently stops honouring.

```
name, queue, payload, key, status, priority, due_at,
attempts, max_attempts, timeout, retry_policy, retry_delay, max_retry_delay,
worker, lease_until, created_at, started_at, finished_at,
result, error, error_type, id
```

`RunStatus` is `pending | running | done | failed | canceled`. `SETTLED` is the last three — nothing
claims a run in one of them again.

`Run.exhausted` is `attempts >= max_attempts`.

### Trigger — when the next run is written

- `Interval(seconds)` — slots counted **from the unix epoch**, never from process start, so every worker of every machine names the same slot. Counted in **whole slots** and never in seconds: carrying the count back through a multiplication a float cannot undo answered the instant it was asked after, and a task rewriting the occurrence it already wrote never reaches the one after. An interval finer than `RESOLUTION`, the microsecond every store keeps an instant to, is refused — every slot of it is the slot before it under another name. One wider than `WIDEST_SECONDS` is refused as well: its first slot is a whole span past the epoch, so it names an instant no datetime holds, and left to the worker it is a pass that raises before anything is ever claimed.
- `Cron(expression)` — five POSIX fields, rounded to the minute.

Both validate in `__post_init__`, so a bad trigger raises where it is declared and not on the night it
would first have run. `Trigger` is an abstract base, like `Store`: a third one that never answers
`next_after` is refused where it is built, instead of raising inside `materialize` at three in the
morning on every worker of the fleet.

### Store — where runs live

Abstract base with **fourteen** methods. One rule runs through all of it:

> **Every method that changes a run is conditional on the state that run was in.**

That is what makes two workers safe without a lock anywhere. A store that answers "changed" for a row
it did not change breaks the guarantee for everybody.

| Method | Conditional on |
| --- | --- |
| `setup` | — builds what the store needs, and does nothing when it is already there |
| `add` | the key is free; answers `None` when it is taken |
| `claim` | `status = pending` and `due_at <= moment` and the queue matches |
| `heartbeat` | `worker` is the caller, `attempts` is the attempt it was handed, and `status = running` |
| `complete` | same |
| `fail` | same |
| `retry_later` | same — the attempt that happened stands |
| `release` | same — the attempt is **given back** |
| `reclaim` | `status = running` and the lease has run out |
| `cancel` | `status = pending` |
| `get` / `find` / `purge` / `count` | reads and housekeeping |

`retry_later` and `release` are the same write with one difference, and it matters: a store that
treats them as one method spends a run's whole retry budget on a rolling deploy.

**What holds a run is an attempt, and never a name.** A worker whose beat could not reach the store
meets its own expired lease on its own next pass — `reclaim` and `claim` are two steps of one pass —
and claims the very same run again under the very same name. From then on the attempt it lost and the
attempt it is running are both the caller, so a condition written on the name alone let the one that
lost the run close it, or put it back in the queue while the other was still mid handler — a third
worker claiming it, and the very same work multiplying with every lease. A claim mints an `attempts`
nobody else is on, so the name and that number together are what say which attempt is asking.

---

## 4. The flow

### One worker pass — `Worker.run_once`

```
reclaim  → what a dead worker left goes back to the queue, or fails for good
tidy     → prune settled runs older than `keep`, once an hour, a batch at a time
materialize → write the next slot of every recurring task
claim    → take up to `free` due runs and start each in its own asyncio task
```

`run` ends by closing the pool its plain handlers ran on, because those threads are the worker's own —
and it closes it however that loop ends, because being cancelled is how a worker is stopped as often
as being asked: a task group, a supervisor and `asyncio.run` with the polling still pending every one
of them cancels rather than asks, and each of those went straight past a close that sat after the
drain.

The first three go through `housekept`, which logs what the store refused and lets the pass carry on:
none of them is the work, and the claim is. The slot write is the one likeliest to be refused for a
reason a claim never meets — every worker of a fleet aims it at the same key in the same instant — and
the reclaim is the statement a database is likeliest to roll back under contention. Either of them
ending the pass is a whole fleet that claims nothing that second.

`run_once` returns what it claimed and does **not** wait for it. `drain` is what waits. Together they
are the whole of `run`, and they are what a test uses instead of sleeping.

`run` catches every `Exception` per pass and logs it: one bad minute must never end a worker.

### Running one run — `Worker.execute`

```
spawn a heartbeat task, pushing the lease every lease/HEARTBEAT_SHARE
announce on_start
look the name up
  UnknownTask       → store.release    → announce on_error   (the attempt is given back)
call the handler (coroutine directly, plain function on the worker's own threads), under `timeout`
  returned          → settle the answer → store.complete → announce on_finish
  answered what no store can write      → store.fail     → announce on_error
  PermanentError    → store.fail       → announce on_error
  Exception         → retry_later or fail, by the policy → announce on_error
  SystemExit / KeyboardInterrupt → store.fail → announce on_error
  CancelledError    → re-raised untouched
ask the heartbeat to stop, and await it — never cancel it
```

Four rules hide in there and all four were bugs once:

- **The heartbeat is asked to stop and then awaited, never cancelled.** A command interrupted halfway leaves the connection with an answer nobody read, and whoever takes that connection next waits for a reply that already went somewhere else.
- **An outcome the store refused is not announced.** A worker whose lease ran out no longer holds the run; announcing anyway writes that run into an audit trail twice.
- **The lookup is outside the call, and `UnknownTask` is never caught around the handler.** Only the lookup can say this worker does not declare the name. A handler that raises it — one fanning out to a name nobody registered — is a handler with a bug, and reading that as a rolling deploy hands the run back with its attempt given back for ever, repeating on every poll everything the handler did before it raised.
- **A listener that breaks breaks alone, and `SystemExit` is not an `Exception`.** `announce` holds everything a handler is already held against, because `sys.exit()` deep inside a library reaches a listener too. Announced before the work starts, it ended the attempt where it stood: nothing ran, no outcome was written, and a lease closed the run as one nobody answered for. `CancelledError` is the one thing it still lets through, or the worker is one nobody can stop.

### How a recurring task fires exactly once — `Queuefy.materialize`

Every worker, on every poll, computes the next slot of every recurring task and writes it under
`f"{name}@{due_at.isoformat()}"`. The key is unique, the store keeps one, the other nine writers are
told the key is taken. Then the run is claimed like any other.

`Queuefy.written` caches the slot each task was last asked for, so a poll every second is not a
cron expression walked every second. It is written **after** the store accepted the row — marking it
first would let a store that blinked drop that occurrence for good.

There is no catch-up. A fleet that was down for an hour writes the next slot, not the sixty it missed.

`register` is the one gate every task passes through, and it answers for every field of one. `Task` is
a frozen dataclass anybody may build and hand straight to it, so a policy the decorator alone stood for
was one a task declared that way carried into a worker untouched — a `retry_delay` of `nan` among them,
which is an ending the store never takes, a run left claimed, and the very same handler run again on
every lease after that. The decorator answers for its own two arguments, an interval and a cron at once,
and for nothing else.

**A trigger that is not one is refused there as well.** What writes the next slot is read on every pass
of every worker, so one that cannot answer raises inside the pass rather than on a run of the task: the
pass carries on, that task never fires, and `materialize` walks the tasks in order — so every recurring
task declared after it is skipped along with it, on every poll, for as long as the process lives.

`register` measures the name against `WIDEST_SLOT`, the widest instant a slot is ever named for, and
never against the one the task happens to want next. A half-second interval lands on microseconds every
other slot, which is seven characters a whole second does not spend — so a name measured against the
short form was accepted where it was declared and then refused by `build` on every pass of every worker,
before anything was ever claimed.

---

## 5. Invariants that must never be broken

1. **Every instant is UTC, decided in `clock.py` and nowhere else.** A naive datetime is the UTC instant it reads as. `datetime.timestamp()` on a naive value reads it as the local wall clock of whichever machine wrote it — the same value would name two instants in two stores. Never call `.timestamp()` directly in a store; go through `redis.stamp()` / `UtcDateTime`.
2. **A run is claimed by a conditional write, never by read-then-write.** Read candidates without a lock, take each with a write conditional on the state it was in, and let the store pick the winner.
3. **A key is what makes a run single.** Everything idempotent in this library — cron slots, interval slots, one-shots declared on every boot — is that one mechanism.
4. **A lease is what says a worker is still here.** Losing it is exactly how a run comes back. A heartbeat that could not reach the store is logged and never becomes the outcome of a run.
5. **Housekeeping is never the work.** Reclaiming, pruning and writing the slots that came due are all things the store may refuse, and none of them must cost the pass the claim it was on its way to make.
6. **Everything that decides ownership is conditional on the worker name and on the attempt that claim minted**, and the name always fits `WORKER_NAME_LIMIT`. The name alone is not enough: a worker meets its own expired lease on its own next pass and claims the run back under it.
7. **Nothing bounded is silently bounded.** Reclaim, purge and claim all take batches, and every batch size is a named constant with the reason beside it.

---

## 6. Tuning constants, and why each one exists

| Constant | Where | Value | Why |
| --- | --- | --- | --- |
| `CLAIM_SPREAD` | `store/base.py` | 5 | how many rows past the limit a claim looks at, so ten workers wanting one task do not all reach for the same row |
| `RECLAIM_BATCH` | `store/base.py` | 500 | a whole cluster dying expires everything at once; one statement over all of it holds the store while every other worker waits |
| `WORKER_NAME_LIMIT` | `store/base.py` | 128 | what every store sizes the `worker` column for — a name that does not fit is a worker whose every claim the database refuses while the process stays up |
| `HEARTBEAT_SHARE` | `worker.py` | 3 | the lease is pushed at a third of its span, well before it runs out |
| `PURGE_EVERY` | `worker.py` | 3600.0 | pruning happens on the hour, never on the poll |
| `PURGE_LIMIT` | `worker.py` | 1000 | a year that was never pruned is caught up over passes, not in one statement that holds the table |
| `MAX_DOUBLINGS` | `retry.py` | 64 | past this the exponent is a number a float no longer holds, and an ambitious `max_attempts` becomes a retry that raises instead of one that waits |
| `HORIZON` | `cron.py` | 41 × 366 | the furthest any expression has to look is the leap day falling on one named weekday, which a weekday field written as a star always asks for because such a field always holds sunday — `*/7` names sunday alone, so `0 0 29 2 */7` is february the 29th on a sunday, and the century that is not a leap year stretches that gap to forty years: 2088 to 2128, which is 14609 days. the first of a month on a named weekday is twelve years and the leap day on any weekday is eight, and sized for either of those an expression that parsed where it was declared raised inside every pass of every worker from the day its next slot went past the bound — the task never fired, and `materialize` walks the tasks in order, so every recurring task declared after it was skipped along with it on every poll for as long as the process lived |
| `CONTENDED` | `store/sqlalchemy.py` | {1205, 1213} | MySQL deadlock and lock-wait timeout — the documented handling is to ask again |
| `TRIES` / `BACKOFF` / `SPREAD` | `store/sqlalchemy.py` | 8 / 0.005 / 1.0 | short to begin with, doubling because contention comes in bursts, and drawn so a herd InnoDB rolled back does not come back in lockstep — measured, five linear tries left refused outcomes on every run of a hot queue |
| `TASK_NAME_LIMIT` / `KEY_LIMIT` / `QUEUE_LIMIT` | `store/base.py` | 255 / 255 / 64 | what every store sizes those columns for, refused where the task is declared — a value past the column is a write the database refuses, or one it quietly cuts short, and two slot keys cut to the same length are one run where there should be two |
| `ERROR_LIMIT` / `ERROR_TYPE_LIMIT` | `store/base.py` | 4096 / 128 | the same two columns, for the two values nothing can refuse where they are written — a message is whatever the code being run put in it, so the worker cuts them on the way in. left to the column, MySQL refuses a long message and PostgreSQL a long class, the ending never reaches the store, and every lease after that runs the same handler again. the message is sized for being the **only** record of what broke: nothing logs a handler that raised, so with no `on_error` listener this field is all there is |
| `WHOLE_FLOOR` / `WHOLE_CEILING` | `store/base.py` | -(2^63) / 2^64-1 | the whole numbers a store keeps inside a JSON value as whole numbers. MySQL holds one for as long as it fits a 64-bit integer and turns everything either side of that into a double, so a payload of `10**40` is read back off it as 9.999999999999998e+39 while memory, SQLite, PostgreSQL and Redis every one of them read back the number that was written. it is the one divergence of the arguments that says nothing at all when it happens: nothing refuses it, nothing logs it, and the handler is called on a value nobody enqueued. what a run is closed with is bounded the same way, and there an answer past it ends the run — no attempt makes a handler answer a different number |
| `PRIORITY_LIMIT` | `store/base.py` | 1000 | a priority names a lane and not a score. Redis keeps one sorted set per priority a queue has seen and a claim walks all of them, so one drawn from a clock or an id is a lane a second for ever — and it is the count of them that has to be bounded |
| `ATTEMPT_LIMIT` | `store/base.py` | 2^31-1 | the most attempts a task may allow, which is what an `Integer` column holds. MySQL and PostgreSQL each keep that count in four bytes and refuse a write past it outright, while memory, SQLite and Redis every one of them take it — so a task allowed more answered a whole suite against the stores a laptop reaches and then raised on every enqueue of it where the runs really live |
| `WIDEST_SECONDS` | `clock.py` | `WIDEST_SLOT - EPOCH`, in whole seconds | the widest interval, and only that: an interval is counted from the epoch, so one past this names its first slot a whole span beyond it — an instant no datetime holds, and a pass that raises before anything is ever claimed. every other span here is added to `now()` rather than to the epoch, and what bounds those is `waited`, against what is left of the range. counted in whole seconds because that is the unit a span is given in, and because `total_seconds()` rounds the last microsecond of the range back up over it |
| `REWRITES` | `app.py` | 3 | how many times `store_once` asks again after the key it was refused for turns out to be held by nobody. writing and finding are two calls with the world between them, and a pruning landing there frees the key and leaves the run nowhere — what this bounds is never that coincidence but a store answering both ways for ever, which would hang the enqueue of whoever called it |
| `PREFIX` | `store/redis.py` | `queuefy` | renames every key at once, so the store shares a Redis without ever meeting the application |

---

## 7. The stores

### MemoryStore

The whole library minus durability. Right for tests and for a single process, wrong for two. One
`asyncio.Lock` guards every mutation. It keeps a **deep copy** of every run, because a caller that
changes the run it enqueued must never change the row — and of the result a run is closed with, which
is the one value that reaches a store after the run was written, so a handler that goes on changing
what it answered was editing a finished run from the outside.

### Versions

**PostgreSQL 14+, MySQL 8.0+, Redis 7.0+**, and whichever SQLite Python was built with. Both ends of
every range answer the whole suite on every push — a minimum nobody tests is a number in a table.

The floor is where a version is still maintained and not the oldest the code could technically sit on:
PostgreSQL 13 went end of life in November 2025 and MySQL 5.7 in October 2023, no managed service sells
either, and neither has ever answered this suite. When a version reaches end of life, raise the floor
and the CI job with it.

**MySQL 8.0 is a correctness floor and not a preference.** Before it, InnoDB rebuilt its auto-increment
counter on startup from the highest id still in the table, so pruning the newest settled runs and
restarting hands those ids straight back out. Measured on 5.7.44 against this schema: the run written
after the restart took the id of the one just pruned, where 8.4 gave the next. That is the very thing
`sqlite_autoincrement` exists to prevent, so **an id means one run for ever from 8.0 and does not on
5.7**.

### SqlAlchemyStore — PostgreSQL, MySQL, SQLite

One table, `queuefy_run`, under **metadata of its own**, so it never creates or drops anything of
the application's.

Three indexes carry the whole load:

- `queuefy_run_ready` on `(queue, status, priority, due_at)` — every claim
- `queuefy_run_lease` on `(status, lease_until)` — every reclaim
- `queuefy_run_settled` on `(status, finished_at)` — every pruning

Things that are the way they are for a reason:

- **`UtcDateTime`** holds naive UTC and reads back aware UTC, because MySQL keeps no offset and a store that guesses one runs everything an hour late. On MySQL it becomes `DATETIME(fsp=6)` — MySQL **rounds** a datetime with no fractional precision, and a run due at `10:00:00.9` stored as `10:00:01` is a run nothing claims until the second turns.
- **`setup` runs `create_all` twice on failure.** `create_all` asks whether the table is there and then creates it, which is a question and a statement with a gap between them; ten replicas booting together used to leave eight of them dead on that gap. It reads no error message: with the table there the second call does nothing, and with it still missing it raises for whatever the real reason was.
- **`under_contention`** retries a write when the database asked for it, and lets everything else through untouched. InnoDB answers a duplicate two transactions race for with a deadlock as often as with a duplicate-key error.
- **`batch()` reads the rows housekeeping is about to write, and the write names them by id.** Naming them inside the write let the database drive the statement off whichever index the condition reads by — the lease index for a reclaim, the settled one for a pruning — so it locked a secondary entry and then reached for the row, while every close of a run locks the row and then reaches for that same entry. Two orders around one row is a deadlock, and InnoDB rolled back the close: a run that had already finished, left claimed, and its work done again a lease later. Read out and ordered by id, every one of these locks the row first and in the same order — which is what a claim has always done.
- **A claim that broke halfway through its batch hands back the runs it already won.** The rows are taken one at a time, so a connection that went away partway through left the worker holding runs whose attempt was spent and whose lease was running — while the worker itself was alive and about to start them. Letting the database end the pass there dropped every one of them: what met them next was the reclaim their leases ran out into, and `max_attempts` is one by default, so each was failed outright under `LeaseExpired` with its handler never called and nothing anywhere saying so. What was cut short is logged, because a batch that stops early and says nothing reads as one that found no more. A claim that won nothing still raises, since there is nothing to lose by saying what happened. **And it is every failure and not only one the driver raised.** A row is taken and committed one at a time, so the connection goes back to the pool between two of them — and a pool with nothing left answers the next one with a `TimeoutError` SQLAlchemy raises itself, which no `except DBAPIError` ever caught. Measured: nothing is held between two rows of a batch, and a claim made while the pool is empty raises that refusal — which on a batch of four left two runs claimed with their attempt spent and handed to nobody. It is an ordinary batch under ordinary load, with nothing having gone wrong anywhere.
- **A row a claim could not read is one it never took.** Taking the row and reading it back are one transaction, so a claim that cannot read what it has just written leaves that row exactly as it found it. Committed first and read afterwards, the read was a second transaction with the world between the two — the connection gone, or a pool with nothing left to hand it one — and the row lost in that gap was the very failure a broken batch is already answered for, worn one row smaller: claimed, its attempt spent, handed to nobody, and failed a lease later under `LeaseExpired` with its handler never called. It is the one row a batch handing back what it already won can never hand back, because it is the row the worker came away with nothing of.
- **`take_back()` asserts the state again in the update, and never trusts the batch it was handed.** The ids are read in one statement and written in another, so a row the second waited on a lock for is written whatever it has since become. Without the condition on the update itself, a reclaim took back a run another worker had legitimately claimed in that moment — the same run on two workers at once, which is the one thing none of this may ever do.
- **`to_values` writes every field of the run and not the ones a fresh one happens to fill.** It wrote fourteen of the twenty-two columns and left the rest to their defaults, so a run handed to `add` with a worker, a lease, an ending or a result already on it came back from a database with all of that quietly gone — while memory and Redis read back what they were given. A field a store forgets is a policy the worker silently stops honouring, and this is the one place the rule was broken.
- **`insert` only reads an `IntegrityError` as "the key is taken" when there is a key.** A keyless run never raced anybody for one, so swallowing that refusal would hand the caller somebody else's run.
- **A timeout and the two retry delays are `Double` and never the generic `Float`.** A Python float is a double, and the generic one is a single-precision `FLOAT` on MySQL: a timeout of 12345.678 came back 12345.7 and one of 3599.999999 came back 3600.0, while memory, SQLite, PostgreSQL and Redis every one of them read back the number that was written. Past what a single holds it was not read back changed but refused outright, which is the same call writing a run on four stores and raising on the fifth.
- **`to_run` reads the payload the store was handed, and never `or {}`.** A run is written with whatever payload it carries, so a falsy one — an empty list, an empty string, a zero — came back as no arguments at all where memory and Redis came back with what they were given. What stops one being written is `as_written`, and this is what stops the read quietly rewriting one that was.
- **The three columns that say which run this is are compared code point by code point.** The key, the queue and the worker are what a claim, a beat and every ending are written against, and MySQL builds a column under `utf8mb4_0900_ai_ci` unless it is told otherwise — a collation that folds case and accents away before it compares. Behind the unique index that made `invoice:Bob@shop.com` and `invoice:bob@shop.com` one occurrence, so the second caller was handed the run of the first and its own work never happened. A worker serving `mail` claimed and ran what was written to `Mail`, and a worker named like another closed a run that other one was still holding. What `identifier` builds instead is `utf8mb4_0900_bin`, the one collation of MySQL 8 that compares the code points and pads nothing — which is what memory, SQLite, PostgreSQL and Redis each already do. The name is left alone: nothing ever compares it, and what carries it into a key is the key.
- **`sqlite_autoincrement` is what makes an id mean one run for ever.** SQLite hands out the highest id it can see plus one, so pruning the newest settled run gives its id to the run written after it — and a caller holding an id from before the pruning reads, and calls off, somebody else's. PostgreSQL sequences and the persisted InnoDB counter never go back, and this is what puts SQLite beside them.

**A production engine needs `pool_pre_ping` and a statement timeout**, for the same reason a Redis
client needs its own: the pass is one `await`, so a connection whose network went away leaves a worker
alive, holding its runs until their leases run out and never coming back on its own.

SQLite across processes needs WAL and a busy timeout — see `docs/stores.md`.

### RedisStore

For whoever would rather not put a queue in their database. `setup` builds nothing; it registers the
scripts.

| Key | Holds |
| --- | --- |
| `queuefy:run:{id}` | the run itself, as a hash |
| `queuefy:queue:{queue}:{priority}` | one lane per priority, scored by `due_at` |
| `queuefy:priorities:{queue}` | which lanes exist, so a claim walks them highest first |
| `queuefy:leased` | what is running, scored by when its lease runs out |
| `queuefy:key:{key}` | the reservation that makes an occurrence single |
| `queuefy:sequence` | the id counter |
| `queuefy:settled` | what is over, scored by `finished_at`, so pruning is a range and not a scan |

**One lane per priority is why priority works.** A sorted set orders by one number, and a claim needs
the highest priority *among what is due* — two questions one score cannot answer.

**A claim gathers the lanes of every queue it serves before it walks any of them**, and merges the ones
of a priority by `due_at`. Priority orders a claim and a queue is only where a run waits: walking one
queue to its end first handed out an ordinary run while an urgent one sat waiting next door, which is
the ordering every other store reads straight out of one index.

**A run is written into the set its own state waits in, and never into the queue lane whatever it
carried.** A store is handed whatever run it is given and every other one reads that state straight
back, so a run written as running belongs on the leases and one already over belongs among what is
pruned. Put in the lane regardless, a run its holder was still working was claimed out from under it
by the next worker to ask — its attempt counted a second time and that holder's ending refused, which
is the one thing none of this may ever do — and a run that was already over was handed out to be run
again and then kept for ever, because a pruning only ever reads `queuefy:settled`. The lane of its
priority is listed either way: a claim walks only the priorities its queue has listed, so a run a
reclaim later puts in a lane nobody listed is one no worker ever sees again. An instant the set orders
by and the run does not carry leaves it in none of them, exactly as a lease that is not there is one
no reclaim takes back.

Every mutation is Lua, so each is one atomic step on the server: `ADD`, `CLAIM`, `RECLAIM`, `SETTLE`,
`HEARTBEAT`, `CANCEL`, `PURGE`. A claim that read, decided and wrote in three round trips would let a
second worker in between them.

**And a claim answers with the runs themselves and never with their ids.** Reading them back was a
round trip of its own after the script had already taken them, so a connection that went away between
the two left every run of the batch claimed, its attempt spent and its lease running, with the worker
that won them holding nothing — and `max_attempts` is one by default, so the reclaim their leases ran
out into failed each of them under `LeaseExpired` with the handler never called. It is the same hole
`SqlAlchemyStore` closes by handing back what a broken batch already won, met from the other side: the
step that takes a run is the step that answers with it, which is one round trip fewer on the path every
worker walks on every poll and nothing at all left in between. An eviction is answered for by the
script's own check that the run is still there, so a hash it did take is one it read in the same step.

Hard constraints:

- **One instance, never Redis Cluster — and that is settled, not pending.** Every script builds the keys it touches from `ARGV` instead of declaring them in `KEYS`, because a claim discovers which run it took only while it runs. A replica for failover is fine, sharding is not. Every way of forcing the keys into one slot costs a guarantee: tagging them all into one slot puts the data on one node anyway, sharding by queue makes a claim across queues cross slots so priority stops being decided across them, and any sharding at all separates the idempotency key from the run it names — which cannot be reserved on one node and written on another without a process dying between them poisoning that key for ever. **When one Redis is not enough, shard where the data already divides: one app and one Redis per tenant, or move the queue to `SqlAlchemyStore`.**
- **`maxmemory-policy` must be `noeviction`.** An eviction takes the run hash without touching the lane it waits in. The store steps over that everywhere — the claim script and the reclaim both check the run is still there before they write a field of it — but what an eviction costs is the run itself, which no code can give back.
- **A production client needs its own timeouts.** redis-py waits forever by default, and a connection whose network went away leaves a worker alive, polling nothing and answering nobody.
- **A client built with `decode_responses` is refused where the store is built.** This store reads what Redis answers as bytes, and that setting is one an application sharing its client very often has on. Nothing below could tell: enqueueing goes on working while every claim raises, which is a queue that takes everything it is given and runs none of it.
- **An instant is held as the whole microseconds since the epoch**, which is the finest every store keeps one to. Held as the seconds `datetime.timestamp()` hands back it was a double, and a double stops being able to name a microsecond somewhere past the year 2112: the last instant a datetime holds came back off it as the first of the year after, which is a `ValueError` raised where `claim` builds the runs it has already taken — that pass ends, the runs beside it in the batch go back on their leases, and the one that raised comes round again on every lease from then on. A whole number is exact where Python reads it, and a score wherever Lua sorts it.
- **`count` is a scan.** Redis has no index over a hash. It is for an operator watching depth, not for a hot path, and it skips a run a pruning already dropped mid-scan.
- **The ranges `RECLAIM` and `PURGE` read stop short of the instant they are given.** A sorted-set range is inclusive where every other store is strict, and a lease taken back the instant it runs out — rather than after it — puts the run on a second worker while the first is still working it.

---

## 8. Retries and failures

| What the handler did | What happens |
| --- | --- |
| returned | done, and a `dict` it returned is kept as the result, settled into what every store reads back |
| returned what no store can write | failed **now** with `UnwritableAnswer`, because no attempt makes a handler answer something else |
| raised, attempts left | comes back, due after the policy's delay |
| raised, attempts spent | failed, with the message and the class that broke |
| raised `PermanentError` | failed **now**, however many attempts were allowed |
| ran past its `timeout` | the worker stops waiting and treats it as a retryable failure |
| never started, its `timeout` having passed while the work was still queued for a thread | back to the queue with `WorkNeverStarted`, **and the attempt is given back** |
| carries a name this worker never declared | back to the queue, **and the attempt is given back** |
| the worker died | the lease runs out, and the run goes back to the queue or fails with `LeaseExpired` |
| raised `SystemExit` / `KeyboardInterrupt` | failed, and the worker keeps going |
| was cancelled | the cancellation is passed on, and the lease brings the run back |

Policies: `FIXED`, `LINEAR`, `EXPONENTIAL`, `EXPONENTIAL_JITTER`. No wait exceeds `max_retry_delay`.
The jitter fraction is **drawn per run** — a fixed multiplier, however large, hands the herd back
whole an hour later. The ceiling is what the draw happens **under**, and never what the drawn delay is
cut down to: a herd that doubled its way past the ceiling would otherwise work the very same wait out
from the very same numbers, which is the one case the policy exists for.

**A timeout stops the waiting, and only a coroutine stops the work.** Python cannot end a thread from
outside, so a plain handler carries on to its own end while the worker has already given up on it —
and the threads a pool runs on are joined when the interpreter goes down, so one that never returns
keeps the whole process alive after the worker has already stopped. Measured: `grace=0.2` returned in
a fifth of a second and the process was still up fourteen seconds later. The deadline has to be inside
the work, and no code here can put it there.

**So the thread is what a worker claims against, and never the slot.** The run was written down and
its slot handed back the instant the timeout passed, while the thread it was on stays inside that
handler — and a slot counted free against a thread that is not is a run claimed onto a pool with
nothing left to run it on. It waits there for a thread while its own `timeout`, which starts where the
work was handed over and not where a thread picked it up, runs out on work that never began: written
down as having taken too long, its attempt spent, and its handler never entered. That is the very
failure the worker's own pool was made to end, arriving by the other door, and it compounds — every
handler that outlives its timeout takes one more thread out of the pool, so a worker that met
`concurrency` of them ran no plain handler ever again while writing down everything it claimed as too
slow. Measured on a concurrency of four against four handlers that each ran four times their timeout
and then returned: eight of the twenty runs behind them were failed with their handler never called.
`Worker.holding()` is what those threads are counted by, and it is subtracted from every `free` the
claim is made with — so a worker with none of them left claims nothing and the work waits in the queue
for one that can run it, which is throughput the depth of a queue shows rather than runs nothing
anywhere records. A worker says so the once, naming the task and what it now holds, because a
capacity that quietly halved reads as a store gone slow.

**And a run whose timeout passed before a thread ever picked its work up ran no line of its handler,
so it is not an attempt.** Accounting for the threads leaves one free for every run claimed, but free
still means the pool has a moment's handing over to do — so a timeout on the order of that handover
lands on work that never began, and it is `release` and not `fail` that belongs there: the claim
spent the attempt, nothing used it, and with `max_attempts` at one writing it down as spent is the
work gone under a message naming a task nobody called. **What decides it is `Future.cancel()` and
never a look at the state**, which is a read a thread picks the work up between: the cancel is taken
only while none has, and from that moment none ever will. **And never what the failure is called
either** — `TimeoutError` is what a socket, a driver or a request inside a handler raises, and a
handler that ran the whole of its work and then said so is already finished, so its cancel is refused
and it gets the ending it asked for. That is what `Handed` carries: the exception says what broke and
only the worker can say whether anything ran, and reading one out of the other is the very mistake
that made the run of a rolling deploy repeat for ever.

**A worker runs plain handlers on threads of its own**, `ThreadPoolExecutor(max_workers=concurrency)`,
made as they are needed so a worker whose handlers are all coroutines makes none, and **closed once
`run` is over** — a thread waiting on a pool waits for ever, so one nobody closes is every one of them
left alive for as long as the process is, and an api running a worker in its lifespan and coming back
up on a reload holds both sets. It is never waited on, for the reason below. The pool asyncio
hands out by default is sized for the whole process at `min(32, cores + 4)` and shared with everything
else in it — six on a two core container, under the eight runs a worker holds by default. The runs past
it waited in a queue while `timeout`, which starts where the run was claimed, ran out on work that had
never begun: written down as having taken too long, the attempt spent, and the handler never called at
all. Measured on a pool of six against a concurrency of eight: two runs failed as `TimeoutError` with
their handler never entered.

**The context is carried into the thread**, the way `asyncio.to_thread` carries it, or a handler reads
nothing where whoever enqueued it set a contextvar.

**A failure is written down in what a store keeps for one.** The message and the class that raised it
come from the code being run, so unlike a name or a key there is nowhere to refuse either where it is
written — and past the column MySQL refuses the message while PostgreSQL refuses the class. The ending
then never reaches the store at all: the run stays claimed, and every lease after that runs the very
same handler again. The worker cuts both on the way in, and what was cut ends in `...`.

**Asking the failure what it says is itself code being run.** A message is built out of whatever the
exception was given, so one carrying an object with a broken `__str__` raises where the ending is being
written — before the escaping and the cutting that exist to stop exactly that. A failure that cannot
say what it was is recorded under the name of its class, which is the one thing left that can be read.

**The length is not the only thing a store refuses.** A path is bytes, and read back off a POSIX system
it carries whatever bytes it had as lone surrogates — which are not UTF-8 at all, so a handler that
opened a file somebody uploaded from another machine raises with one of them in the message, and SQLite,
MySQL, PostgreSQL and Redis every one of them refuse it. A NUL byte is one PostgreSQL refuses on its own.
The worker escapes both into the text that says what they were, and it does that **before** it cuts,
because escaping is what makes a message longer than the column. The class is only ever cut: Python
refuses to name one with a character UTF-8 cannot hold, so there is nothing there to escape.

**What the handler answered is settled where it is written, exactly as the arguments are.** A result
comes out of the code being run, so there is nowhere to refuse one at the call — and left to the store
the divergence is the same one the payload is already settled against. A path read back off a
filesystem carries its bytes as lone surrogates, which MySQL refuses inside a JSON value; `nan` and
`infinity` are words only Python's own reader of JSON has, so SQLite and Redis hold what MySQL and
PostgreSQL refuse; an object no serialiser takes is one memory keeps and every other store throws the
ending away over; and a key that is not a string reads back as one everywhere but in memory. So the
worker escapes every string on the way in, by the same escaping a message gets, and then writes the
answer out and reads it again — one value, meaning one thing, wherever the runs live.

**An answer no store could write down ends the run where it happened.** It is the handler that has the
bug, and no attempt makes it answer something else, so `UnwritableAnswer` closes the run however many
attempts were allowed. Left to the store the ending never lands at all: the run stays claimed, the
lease hands it back, and the very same handler runs again on every one of them until the attempts are
spent — under `LeaseExpired`, a message about a worker that stopped answering, which is not what
happened. **An answer that holds itself is refused there too.** The escaping walks the answer before
anything writes it, and a structure referring back to itself raises a `RecursionError` there — which is
neither of the two refusals a value no store can take raises, so it went straight past the catch that
exists for exactly this and left the run claimed.

**`UnknownTask` is the rolling-deploy path, and it belongs to the lookup and never to the handler.**
The older replica meets runs the newer one enqueued for tasks it does not declare. The claim already
spent the attempt, so handing the run back is what gives it back — and a run left sitting on an
attempt it never used is one the first reclaim that meets it ends for good. A handler that raises
`UnknownTask` itself is an ordinary failure, retried and ended by the policy like any other.

---

## 9. Testing

```bash
make install     # venv + the package with its development tools
make servers     # redis on 6399, mysql on 3399, postgres on 5499
make test        # the suite
make coverage    # the suite with the 100% branch gate
make stress      # many machines against every server that answers, minutes and not seconds
make lint        # ruff check + black --check
make format      # ruff --fix, then black — in that order
make build       # wheel and sdist
```

Rules the suite enforces on itself:

- **Coverage stays at 100%, branches included.** It is a gate, not an aspiration.
- **Every store answers the same contract.** `tests/test_store_contract.py` is written against the interface and parametrized over every reachable store. Add a new backend to the fixture in `tests/conftest.py` and it inherits the whole suite.
- **And it answers it the same way.** A suite written by hand only ever asks the questions somebody thought to ask, so `tests/test_differential.py` asks the ones nobody did: a seeded script of every operation, run against each store and against `MemoryStore`, compared on every field of every run, every answer and every count. Both of the drifts it found were invisible to a contract suite already passing everywhere. **The instants are compared too**, and they are the field a store is easiest to get wrong — `created_at` is the only one left out, because each store is handed a run of its own and that field is stamped off the clock as each is built.
- **And it answers it whole, or it answers nothing.** `tests/test_interruptions.py` cuts one round trip of one call at a time — a statement or a commit for a database, a command for Redis — and reads what that call left behind after each cut, sweeping until a cut no longer fires. The failure it injects is not an invented one: a pool with nothing left to hand out raises exactly it under ordinary load, which this repository had already measured. It is the third question neither gate asks, because the coverage gate counts the lines a test reached and every one of these was reached, and load on its own breaks nothing at a named point. What it found is a claim that took a row in one transaction and read it back in the next. **Every round trip is refused twice over**, once by a refusal that ends the call and once by the deadlock this library is written to ask again after, because the second walks what the first never reaches: the rollback that has to leave a session usable, and the statement asked a second time over a row the first may already have written. Measured over a claim of four rows, that is twelve real retries against three points — the update, the read back and the commit — where `tests/test_contention.py` asks at one. Nine of its ten sweeps are guard rather than finding, and what they are there for is the day somebody writes a run over two round trips instead of one.
- **A test never waits without a bound.** Use `wait_until` from `tests/conftest.py`. `pytest-timeout` is set to 120s with `timeout_method = "thread"`, so a hang becomes a failure with every stack dumped.
- **One session at a time against a server.** Every suite here owns the whole store — the ordinary one drops the table before each test it runs, and the stress one has six machines working it — so two sessions against one server leave both of them reading a table the other just took away. What that looks like is a run failing somewhere it never touched, under a relation that does not exist or a queue that carried none of the work, and nothing but remembering what else was running tells it from a real failure. CI never meets it, because every job there declares its own containers.
- **A store nobody can reach is not collected.** Memory and SQLite always run; Redis, MySQL and PostgreSQL join when their port answers. `make coverage` needs all three. The port answering is the whole test, so `make install` carries `aiomysql`, `asyncpg` and the `cryptography` MySQL 8 authenticates with: with the servers up and a driver missing, every test of that store fails on building the engine instead of being quietly left out. Nothing in `queuefy` imports them, which is why they are development tools and not extras of the package.
- **Run against a real MySQL before believing anything about MySQL.** Its `DATETIME` rounding is invisible to SQLite and PostgreSQL.
- **The stress suite is marked `stress` and left out of every ordinary run.** It is minutes rather than seconds, so a run of `make test` that included it is a run nobody waits for. Tracing costs an order of magnitude, which is why its load lives there and not in the graded suite.
- **The release runs the stress suite**, because a version on PyPI is permanent and load is the one thing no ordinary run ever applies. Locally it stays a `make stress` somebody runs after touching a store.
- **100% coverage is not the same as 100% of the interleavings, and neither of those is what a call that stopped halfway left behind.** Four of the worst bugs found so far were invisible to a suite already at 100%: coverage counts lines a test reached, and not one of them was a line nobody reached. Three were reached by load and a second connection — the last of those a deadlock two statements caused by locking one row in two orders, which no single-connection test can ever see. The fourth was reached by neither, because nothing about it needs two of anything: a claim took a row in one transaction and read it back in the next, so a row it could not read stayed claimed and handed to nobody. What reaches that is a failure placed at one named point, which is the sweep in `tests/test_interruptions.py`.
- **Coverage cannot see a conditional expression either.** `a if condition else b` is one statement, so branch coverage never asks whether both sides were taken. Two dead branches and one documented shorthand nobody tested were hiding in ternaries under a gate reading 100%. When something must be answered for, write it as an `if`.

Files worth knowing:

| File | What it is for |
| --- | --- |
| `tests/test_store_contract.py` | the one suite every store answers |
| `tests/test_differential.py` | one seeded script of every operation, answered by each store and compared field by field against `MemoryStore` |
| `tests/test_interruptions.py` | every round trip a store makes, cut one at a time, and what the call left behind read after each cut |
| `tests/test_review.py` | one test per bug a line-by-line reading found, each named after what it would have caught |
| `tests/test_disasters.py` | clock skew, dying processes, handlers calling `sys.exit`, results the store cannot write |
| `tests/test_many_machines.py` | separate interpreters against one database, which is what containers are |
| `tests/test_many_workers.py`, `test_contention.py` | many workers in one process |
| `tests/test_fleet_stress.py` | `make stress` — many machines and many workers against a real server, with leases running out under them the whole time |
| `tests/test_frameworks.py` | the lifespan protocol every asgi framework speaks, honoured without importing one |
| `tests/test_docs.py` | the prose goes stale in silence, so this keeps it honest |
| `tests/fleet.py`, `machine.py`, `survivor.py` | the app, the store and the processes a fleet test spawns, against whichever url it is given |

**When you fix a bug, add the test that would have caught it to `tests/test_review.py`**, named after
the behaviour and not the fix, with a docstring saying what went wrong. Then confirm it fails against
the unfixed source — a test that passes either way pins nothing.

---

## 10. CI and releasing

Three workflows, all under `.github/workflows/`.

**`test.yml`** runs on every push and pull request, with Redis, MySQL and PostgreSQL as service
containers on the same ports the local `make servers` uses. It also declares `workflow_call`, so the
release calls it instead of repeating it. Two jobs:

- `test` — Python 3.11, 3.12 and 3.13 against the newest servers, linting and then the coverage gate.
- `oldest` — one Python against the oldest supported version of each store, which is what keeps the number in the documentation from being a number in a table. Raise both together when a version reaches end of life.

**`stress.yml`** is `make stress` on a runner, on one Python version — what it asks about is the store
under load and never the syntax around it. It runs on demand and on the release, and **not on a
schedule**: the code does not change at night, so a run over an unchanged tree re-answers a question
already answered, and a pipeline nobody has a reason to read is one nobody reads. The release is where
it earns its minutes, because that is the one moment the result cannot be taken back.

**`release.yml`** runs on a `v*` tag and publishes to PyPI:

```
test    → the whole suite, called from test.yml
stress  → many machines against every server, called from stress.yml
build   → check the tag equals the version in pyproject.toml, then `python -m build`, upload the artifact
publish → download that same artifact, push it to PyPI, cut the GitHub release
```

Three things make it safe:

- **A version on PyPI is permanent**, so the suite answers for it before anything is built.
- **The tag has to equal `project.version`.** A tag that disagrees publishes under a number nobody asked for, and PyPI never lets that number be used again.
- **What is published is what was checked.** The publish job downloads the artifact the build job produced instead of building a second time.

It authenticates by **Trusted Publishing (OIDC)** — `id-token: write` and the `pypi` environment — so
there is no API token anywhere in the repository. The publisher registered on PyPI must say:

| Field | Value |
| --- | --- |
| PyPI Project Name | `queuefy` |
| Owner | `paulocoutinhox` |
| Repository name | `queuefy` |
| Workflow name | `release.yml` |
| Environment name | `pypi` |

To cut a release: bump `project.version` in `pyproject.toml`, commit, then push the matching tag.

```bash
git tag v1.0.0
git push origin v1.0.0
```

---

## 11. Code style — non-negotiable

Formatting is decided by `ruff` and `black` with **line length 320** and
`skip-magic-trailing-comma`. That number is not an accident: it exists so calls and signatures stay on
one line.

**Layout**

- Functions, methods, constructors and calls stay on **one line**, always. Never break parameters across lines. Never format a signature vertically. However many parameters there are, they stay on one line.
- Keep it compact. Use only the blank lines that separate one context from the next, and always separate blocks of different responsibility with exactly one.
- Never leave `if`s, validations, state changes and returns visually glued together. A complex method has a beginning, a middle and an end you can see at a glance.
- Prefer early returns. No `else` after a `return`. Avoid needless nesting.
- Extract a small private method when a block is accumulating responsibility — and never just to make something shorter. An artificial helper that breaks the main flow is worse than the block it replaced.
- No semicolons, ever — not to join statements and not inside a sentence in prose.

**What counts as a change**

- **A change earns its place by fixing something.** A bug, a race, a wrong result, a failure nobody records — that is what a change is for. Anything else is work done to look like work.
- **One thing is done one way.** Never add a second function, method or path that does what one already there does under another name or another shape. When something needs to change, change the one that exists and move every caller to it — there is never a spelling of it kept beside the new one.
- **A change that is only a comment is not a change.** Rewording one that is already true, adding one to code nobody touched, or explaining a line that explains itself is not a task. A comment is written with the code it belongs to and never on its own.
- Renaming, reshuffling or reformatting code nobody was fixing is not a change either. Refactor as far as a fix needs and no further.

**Comments**

- Rare, and only where they earn it. Well-named classes, methods and variables are the documentation.
- One-line comments starting with `#` or `//` are **lowercase**. Everything else reads normally.
- They explain **why** — context and intent — never what the line already says.
- Objective and natural. One complete sentence per line. Never continue a sentence on the next line: finish it, punctuate it, then start a new one.
- No decorative comments, no `# --- helpers ---` section banners, no comments narrating a change that was made.

**Python**

- `__init__.py` files are **empty**. Absolutely nothing goes in them.
- No `TYPE_CHECKING`, and no `if TYPE_CHECKING:` import blocks.
- No backward compatibility, no legacy paths, no "it used to work the other way" checks. There is one current version, and refactoring the whole thing to get there is expected.
- No generic fallbacks and no `else` branches invented for cases nobody understands. Something unknown fails loudly where it happens.
- No dead code.
- Everything — code, comments, docstrings, log messages, tests — is in **English**.

**Validation**

Anything that could never work is refused **where it is written**, not at three in the morning: a poll
of zero, a concurrency of zero, a lease that has already run out or one so wide the claim it is made
with names an instant nothing holds, a worker serving no queues or one no
task could ever be declared with, a cron expression that matches nothing, one whose field steps over a
single value rather than over a range, which is a step that walks nothing and leaves `0 9/2 * * *`
naming nine o'clock alone while the task behind it was written for every other hour, or one that is
not text at all —
an interval answers for the type it was handed and a cron never did, so anything else reached the split
and raised an `AttributeError` out of the parser, which is the one way of declaring a trigger wrong that
`except QueueError` never caught — an interval finer than the
microsecond a store keeps or wider than the last instant a datetime holds, a task asking for an
interval and a cron at once, a priority drawn from something that keeps changing, a priority or a
`max_attempts` that is not a whole number — which Redis writes down as text and reads back as a number,
so a float is a lane nothing reads back and every claim that meets it raises instead of taking anything
— a `max_attempts` past `ATTEMPT_LIMIT`, which is the four bytes MySQL and PostgreSQL count it in and a
write both of them refuse outright while memory, SQLite and Redis take it, a retry policy that is not a
`RetryPolicy`, which memory keeps as the object it was handed while every store that writes a run out
asks it for the value it is written under and raises on the enqueue itself, a concurrency that is not
one either, which is the limit every claim is made with and is what a count
of cores halved comes out as, the queues of a worker given as one name and not a tuple, which is a
string read one letter at a time — and ones given as anything that empties as it is walked, because
what a worker serves is read again on every claim it makes: a generator written where a tuple was
meant is emptied by the very loop that answers for the names in it, and every claim after that asks
for no queues at all, which memory, SQLAlchemy and Redis each answer with nothing — a key of no
characters, which a column takes as a name held once and
Redis reads as no key at all, a handler that is a generator, which runs none of its body when it is
called and closes every run of itself as done with the work never started — read off the `__call__` of
an object and never off the object, because that is what runs when one is called and `inspect` reads
the code of what it is handed, so a class based handler whose `__call__` yields walked through a gate
written for the plain function and lost every run of itself the same silent way — a payload that is not a
mapping, which is what a handler is called with and which a database reads back as no arguments at all
where memory and Redis read back the empty list, the empty string or the zero they were handed, and a
payload no store could
write down, which is JSON wherever the runs
live and which memory takes without a word while every real store refuses it inside a driver — `nan`
and `infinity` with it, which Python writes as words no other reader of JSON has, so SQLite and Redis
hold one where MySQL and PostgreSQL refuse it. Each one carries a message that says what was asked for
and why it cannot be.

**A whole number inside a payload is bounded by what every store reads back as that same number**, and
it is the one divergence of the arguments that says nothing at all when it happens. That is `exact`.
MySQL keeps one inside a JSON value for as long as it fits a 64-bit integer and turns everything either
side of that into a double, so `10**40` is read back off it as 9.999999999999998e+39 while memory,
SQLite, PostgreSQL and Redis every one of them read back the number that was written. Nothing refuses
it, nothing logs it, and the handler is simply called on a value nobody enqueued. What a run is closed
with goes through the same walk, and there it is `UnwritableAnswer`: no attempt makes a handler answer
a different number, and what it would otherwise cost is not an ending nobody records but a result
nobody can tell was changed.

**A name a run is found by is refused when no store could write it down**, and never escaped the way a
message or an answer is: the key, the name of a task and its queue are what say *which run this is*, so
escaping one folds two callers into a single row. That is `writable`, and `holdable` is it together
with the column length. **The same name is answered for where a run is looked for by one**, by the same `keyed`: a caller
reading a key off a request that did not carry it asks with nothing at all, and no two stores look for
that the same way — memory compares it to the key of every run and answers with the first written under
none, `SqlAlchemyStore` reads it as `IS NULL` and raises the moment two runs have no key between them,
and `RedisStore` goes looking for a key spelled after it. A lone surrogate is what a POSIX path carries
when the bytes behind it were
never UTF-8, and SQLite, MySQL, PostgreSQL and Redis each refuse one in their own way — a NUL byte is
one PostgreSQL refuses on its own. Memory keeps either without a word, so a key built out of a filename
works in a test and raises in production, inside a driver that never names the value. The queues a
worker serves go through it too, and **so does the worker's own name**: both are in every claim, every
beat and every ending it writes, and the name is not one anybody had to type — `worker_name` builds it
out of `socket.gethostname()`, which carries whatever bytes the host had, and Python reads the ones that
were never UTF-8 back as lone surrogates. Memory claims under such a name without a word while every
real store refuses it, which is a worker that logs the pass, waits, asks again, and never takes
anything at all. **The type is asked for before any of that**, for the reason a cron expression asks
for one: what is not text never reaches the encoding or the length, it reaches the `encode` behind them
and raises an `AttributeError` about a method — out of a task being declared or a worker being built,
under a name `except QueueError` never catches, which is every other way of getting a name wrong caught
by a startup and this one not.

**The instant a run comes due is settled where the call is made**, by `scheduled`, exactly as its
arguments are. It is the one field of a run nothing further down can do without, and a nullable column
handed straight to `enqueue_at` is what reaches it: memory holds whatever it is given and then every
claim that meets it raises comparing nothing to the moment — not that run alone but every claim of
every worker from then on, which is a fleet that polls for ever and takes nothing. A database refuses
the column and Redis refuses the score.

**A span this library is given is a plain, real number**, refused by `real` wherever one arrives: the
seconds of an interval, the timeout, the retry delay and its ceiling, and the poll, the jitter and the
grace of a worker. Every guard above is a comparison, and `nan` is false against all of them while
`infinity` is past every one — so both go through each one untouched and reach the arithmetic instead,
where a `timedelta` refuses them exactly as the ending of a run is being written. A retry delay of
`nan` left the run claimed with nothing recorded, and the lease ran the very same handler again on
every one of them until the attempts were spent. **The type is asked for and not the interface**, for
the very reason a count asks for one: a store keeps a span as text and reads it back with `float`, so a
boolean is written down as `True` and a fraction as `1/3`, and neither of them reads back. A timeout of
`True` was taken where the task was declared, kept whole by memory, rounded to `1.0` by a `Double`
column, and raised on by every claim of every Redis worker from then on — which is a fleet that logs
the pass, waits, asks again, and never takes anything at all.

**A span a worker is given is a `timedelta` and never a number of seconds**, which is `spanned`, and it
holds the two a worker is built with: the lease, and how long it keeps a run that is over. Every bound
above is read off `total_seconds()`, so a plain number never reached one of them — it reached that call
and raised an `AttributeError` about a method, out of the worker being built, under a name
`except QueueError` never catches. A `lease=60` is the mistake anybody makes once, and what it deserves
is a sentence saying a span is a `timedelta`.

**A span taken off `now()` is measured against what lies behind it**, which is `kept` and which holds
the one span measured in that direction: how long a worker keeps a run that is over. A pruning asks for
everything settled before it, so one past the range names an instant no datetime holds — accepted where
it was written, and then raising inside the pruning on the hour, every hour, while nothing that is over
is ever dropped.

**A span added to `now()` is measured against what is left of the range, and never against the whole of
it.** That is `waited`, and it holds the retry delay, its ceiling and the lease of a worker. An interval
is counted from the epoch, so `WIDEST_SECONDS` is exactly its bound — but a wait is added to the moment
an attempt failed at and a lease to the moment a claim was made at, and the two anchors differ by every
second since the epoch. Measured against the interval's bound, every span in that gap was accepted where
it was written and raised where it was taken: a wait raised while the ending of a run was being written,
which left the run claimed and ran the very same handler again on every lease after that, and a lease
raised inside the claim itself, which is a worker that comes up, logs the pass, waits and asks again,
and never takes anything at all.

A payload that survives is **settled where it is written** rather than refused, because there is
nothing wrong with it: a tuple is read back as a list and a key that is not a string is read back as
one, in every store but the one that keeps the object it was handed. So `as_written` writes the
arguments down and reads them again as the run is built, for the reason the instant is decided there
and never in a store — one value, meaning one thing, wherever the runs live.

**Prose in `docs/`**

Every heading starts with an emoji, and no page uses the same one twice — `tests/test_docs.py`
enforces both, along with every symbol, table name, Redis key family and internal link the prose
names. Keep the voice: plain, concrete, and explaining the reason rather than the mechanism.

**No sentence begins with code.** Put a word in front of it — "The `run` loop polls", never "`run`
polls". A sentence opening on a backtick opens on a lowercase identifier, which reads as a fragment
and breaks the language rather than the code. One word is enough, and the suite checks it.
