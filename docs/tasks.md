# 📝 Tasks

A task is a **name**, a handler and a policy. The name is what travels in the database, so it stays
stable while the function behind it moves, is renamed or changes module.

## 🧭 The four kinds

### ⚡ Run it now

```python
from myapp.queue import app


@app.task("send_email", max_attempts=5)
async def send_email(to: str):
    ...


await app.enqueue("send_email", to="reader@example.com")
```

### 🔁 Run it every N seconds

```python
@app.task("poll_inbox", every=30)
async def poll_inbox():
    ...
```

The `every` is seconds, and a `timedelta` works too. Slots are counted from the unix epoch and never
from
whenever a given process started, so **every worker of every machine names the same slot**. That is
what makes the next line true.

They are counted in whole slots and never in seconds, so the slot a worker is handed is always one
still to come. An interval finer than the microsecond a store keeps an instant to is refused where
it
is written: every slot of it would be the slot before it under another name.

An interval wide enough that its first slot lands past the last instant a datetime holds is refused
there too. Left to the worker it is a pass that raises before it claims anything — on every worker
of
the fleet, for as long as the deployment lives, against a store with nothing wrong with it.

Every span a task is given is a real number, and one that is not is refused in the same place: the
interval, the `timeout`, the `retry_delay` and the `max_retry_delay`. A guard is a comparison, and
`nan` is false against all of them while `infinity` is past every one, so both go straight through and
reach the arithmetic instead — where a span is turned into the instant an attempt is due at, exactly
as the ending of a run is being written. A `retry_delay` of `nan` left the run claimed with nothing
recorded, and the lease ran the very same handler again on every one of them. A wait is measured
against the last instant a datetime holds on top of that — against what is left between now and that
instant, and never against the whole span from the epoch the interval is counted from. A wait is added
to the moment the attempt failed at, so one wider than what is left names an instant nothing holds, and
the two bounds differ by every second since the epoch: measured against the wrong one, such a wait was
accepted where it was declared and raised where it was taken.

> **And a span is a plain number, refused where the task is declared when it is not.** A store keeps
> one as text and reads it back with `float`, exactly as it does a priority, so a boolean lands in
> Redis as `True` and a fraction as `1/3`, and neither of them reads back. What raises on it is never
> that one run but every claim that meets it, on every poll of every worker from then on, while memory
> hands the handler the object it was given and a database column takes the number nearest to it. The
> type is asked for and not the interface, because a boolean is a number to Python and a word to a
> store.

Everything on that list is answered for wherever a task is registered, and not only where the
decorator is written. A `Task` is a dataclass anybody may build and hand to `Queuefy.register`, and a
policy the decorator alone stood for was one a task declared that way carried straight into a worker.

### 📅 Run it once, at a stated time

```python
from datetime import datetime, timezone

await app.enqueue_at("close_campaign", datetime(2026, 8, 1, 10, tzinfo=timezone.utc), campaign_id=7)
```

A one-shot is a run with a future `due_at` and nothing more. If every worker is down when it comes
due, the first one back picks it up — nothing is lost by being late.

> **A datetime with no zone is read as UTC**, and it is settled once, here, rather than by each store
> for itself. That matters because the obvious reading is not the same on both sides: a column with no
> offset can only mean UTC, while `datetime.timestamp()` reads a naive value as the wall clock of
> whichever machine happened to write it. The same value would name two instants depending on the
> store. Pass an aware datetime and none of this is yours to think about.

> **And what is not an instant at all is refused at the call.** A nullable column handed straight to
> `enqueue_at` is the one that reaches here, and it is the one field of a run nothing further down can
> do without: memory holds whatever it is given, and then every claim that meets it raises comparing
> nothing to the moment — not that run alone but every claim of every worker from then on, which is a
> fleet that polls for ever and takes nothing. A database refuses the column and Redis refuses the
> score, so no two stores even name it the same way.

To declare one from code that runs on every boot, give it a key so the tenth boot does not enqueue a
tenth copy:

```python
await app.enqueue_at("close_campaign", when, key="close_campaign:7", campaign_id=7)
```

### ⏰ Run it on a cron expression

```python
@app.task("nightly_report", cron="0 4 * * *")
async def nightly_report():
    ...
```

Standard five POSIX fields, with `*`, numbers, `a-b` ranges, `a,b` lists and `*/n` or `a-b/n`
steps. Sunday is
0
or 7. Day-of-month and day-of-week are joined by **or** when neither is a star, which is what
POSIX
says and what `0 0 1 * 1` — the first of the month *or* any Monday — depends on.

A step walks a range and never a single value, so `0 9/2 * * *` is refused where it is written and
`0 9-23/2 * * *` is what says it. Read as it stands the first of those names nine o'clock and
nothing else, and a task written for every other hour would have run once a day with nothing
anywhere saying so.

> **A star is what the field was written as, and never what the values it named add up to.** POSIX
> joins the two day fields by whether each one is `*`, so `1-31` names every day there is and is
> still a restriction — `0 0 1-31 * 1` runs every day, not only on Mondays — while `*/2` names half
> of them and is still a star, so `0 0 */2 * 1` runs on the Mondays that fall on an odd day and not
> on either of the two. Decided by the parsed set instead, both of those fired on days no other cron
> fires on, and a schedule that is wrong is the one kind of failure nothing anywhere reports.

An expression that cannot mean anything raises where it is declared, not on the night it would first
have run — `0 0 30 2 *` included, because no February has a thirtieth. That one is worth naming: the
search is what would otherwise discover it, and what the search walks before it gives up is **forty
years of days** — on every pass, for as long as the process lives. Forty is what it has to be,
because the furthest slot any expression that does parse ever asks for is the leap day falling on
one named weekday: `0 0 29 2 */7` is February the 29th on a Sunday, and the century that is not a
leap year puts 2088 and 2128 forty years apart.

> **A day nothing has is only fatal while the weekday field is a star.** POSIX joins the two with an
> **or** when neither is one, so `0 0 30 2 5` is a perfectly good expression: every Friday in
> February matches it.

## 🔑 Why ten workers do not make ten runs

A recurring task is not run by a scheduler. Every worker, on every poll, computes the **next slot**
of
every recurring task it knows and writes it down under a key built from the name and that instant:

```
nightly_report@2026-08-01T04:00:00+00:00
```

The key is unique. Ten workers write it, the database keeps one, and the other nine are told the key
is taken and carry on. Then the run is claimed like any other — by exactly one of them.

Nothing elects a leader, because nothing has to.

The name has to leave room for the instant beside it, so a recurring task is measured against the
**longest** key it will ever write and not against the slot it happens to want next: an interval of
half
a second lands on microseconds every other slot, and those are seven characters a whole second does
not
spend. A name that would outgrow the column is refused where the task is declared, because left to
the
worker it is a pass that raises before it claims anything — on every worker, for as long as the
deployment lives.

## 🗝️ Keys

Any run may carry a key, not just a recurring one. A key is an idempotency guarantee: the second
caller is handed the run the first one wrote.

```python
first = await app.enqueue("send_email", key="welcome:42", to="reader@example.com")
again = await app.enqueue("send_email", key="welcome:42", to="somebody@example.com")

assert again.id == first.id
```

A key is refused where it is written if it is longer than the column a store keeps it in, and if it
has
no characters at all. An empty key is the one value the stores cannot agree on: a column takes it as
a
name that can be held once, and Redis reads it as no key at all — so the same call would fold every
enqueue into one row on a database and write a row every time on Redis, with nothing in an
application
to say which of the two it was getting.

**The same key is answered for where a run is looked for by one.** A caller reading a key off a request
that did not carry it asks with nothing at all, and no two stores look for that the same way: memory
compares it to the key of every run and answers with the first written under none, `SqlAlchemyStore`
reads it as `IS NULL` and raises the moment two runs have no key between them, and `RedisStore` goes
looking for a key spelled after it. So `find` refuses it exactly where `enqueue` does.

So is a key no store could write down at all, and so are the name and the queue of a task. A lone
surrogate is what a POSIX path carries when the bytes it was read from were never UTF-8, and SQLite,
MySQL, PostgreSQL and Redis each refuse one in their own way — a NUL byte is one PostgreSQL refuses on
its own. Memory keeps either without a word, so a key built out of a filename works in a test and
raises in production, deep inside a driver that never says which value it was. These three are refused
rather than escaped, unlike a failure message or an answer: what they name is *which run this is*, and
escaping one would quietly fold two callers into a single row. What is not text at all is refused
first, for the reason a cron expression that is not text is: it never reaches the encoding or the
length, it reaches the call behind them and raises about a method instead of about a name.

## 🚚 Queues

A task may name a queue, and a worker names the queues it serves. That is how a slow task is kept
from
sitting in front of a fast one:

```python
from queuefy.worker import Worker


@app.task("transcode", queue="heavy", timeout=3600)
async def transcode(path: str):
    ...


Worker(app, queues=("heavy",), concurrency=2)
Worker(app, queues=("default",), concurrency=32)
```

## 🧩 Handlers

A handler may be `async def`, a plain `def`, a callable object, or something a decorator wrapped. A
plain one runs off the event loop so it never blocks the runs beside it, and one that only *looks*
plain — a wrapper that answers a coroutine — is awaited all the same.

A trigger that is not one is refused there too. What writes the next slot of a task is read on every
pass of every worker, so one that cannot answer raises inside the pass rather than on a run — the pass
carries on, that task never fires, and the slots are written in the order the tasks were declared, so
every recurring task declared after it is skipped along with it on every poll.

A generator is refused where the task is declared, `async def` with a `yield` in it included. Calling
one runs none of its body: the worker would be handed the generator object itself, keep nothing of it
because it is not a mapping, and close the run as done — every run of it, with the work never started
and nothing anywhere saying so. An object is answered for by its `__call__` and never by itself, since
that is what runs when one is called: asked about the instance, `inspect` reads the code of something
that has none, so a class based handler whose `__call__` yields walked straight through a gate written
for the plain function.

The payload arrives as keyword arguments, so it has to be a mapping and it has to survive a trip
through JSON — and one that will not is refused at the call rather than deep inside a driver. Anything
that is not a mapping could never be called at all, and the stores did not even agree on what it was: a
database read an empty list, an empty string and a zero all back as no arguments where memory and Redis
read back what they were handed.

Memory took an object no store could
serialise where every real one refused it, so an application built against memory enqueued it and only
met it in production. A lone surrogate is the same divergence one store further along: it is what a
POSIX path carries when the bytes it was read from were never UTF-8, and MySQL refuses one inside a
JSON value where the other four hold it. So is `nan`, and so is `infinity`: Python writes them as words
no other reader of JSON has, so SQLite and Redis hold what MySQL and PostgreSQL refuse — and an average
of nothing at all is where one comes from.

> **A whole number is bounded, and it is the one divergence that says nothing when it happens.** MySQL
> keeps a whole number inside a JSON value for as long as it fits a 64-bit integer and turns everything
> either side of that into a double, so a payload of `10**40` was read back off it as
> 9.999999999999998e+39 while memory, SQLite, PostgreSQL and Redis every one of them read back the
> number that was written. Nothing refuses it, nothing logs it, and the handler is simply called on a
> value nobody enqueued. What the run is closed with is bounded the same way and for the same reason —
> an answer past it ends the run with `UnwritableAnswer`, because no attempt makes a handler answer a
> different number.

> **What does survive comes back settled, and it is settled once at the call.** A tuple is read back as
> a list and a key that is not a string is read back as one, in every store but the one that keeps the
> object it was handed — so the same call gave a handler a tuple in memory and a list on every real
> store. The arguments are written down and read again where the run is built, for the reason the
> instant is decided there and never in a store: one value, meaning one thing, wherever the runs live.

## 🥇 Priority

A `priority` is served before age. A task declares its own, and a single call may override it:

```python
await app.enqueue("send_email", priority=10, to="reader@example.com")
```

It is served before age **across every queue a worker serves**, and not one queue at a time: a queue
is
where a run waits, and never what orders it. A worker on `("reports", "email")` holding a backlog of
reports still takes the urgent email waiting behind it.

> **A priority names a lane, not a score, and it is bounded for that reason.** Redis keeps one sorted
> set per priority a queue has ever seen and a claim walks every one of them, inside a script that
> holds the server while it runs. A priority drawn from something that keeps changing — a timestamp, an
> id — is a new lane every second for ever, and a claim that grows without end while the queue goes on
> looking like it works. So a priority runs from `-PRIORITY_LIMIT` to `PRIORITY_LIMIT`, refused where
> the task is declared and again where a call overrides it. Single digits are what anybody actually
> needs.

> **And it is a whole number, refused in the same two places when it is not.** A store writes a
> priority down as text and reads it back as a number, so a float lands in Redis as a lane called
> `5.0` and a boolean as one called `True`, neither of which reads back. What raises on it is not that
> one run but the claim that met it, on every poll of every worker from then on — a fleet that stops
> taking anything at all over a single enqueue. The same holds for `max_attempts`.

> **And `max_attempts` is bounded by the column that counts it.** MySQL and PostgreSQL each keep that
> count in four bytes and refuse a write past it outright, while memory, SQLite and Redis every one of
> them take it — so a task allowed more than `ATTEMPT_LIMIT` answered a whole suite against the stores
> a laptop reaches and then raised on every enqueue of it where the runs really live.
