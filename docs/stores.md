# 🗄️ Stores

A store is where runs live. It is the only thing that knows about durability, and it is the seam a
different backend plugs into.

## 🔢 Versions

| Store | Supported from | CI runs |
| --- | --- | --- |
| PostgreSQL | 14 | 14 and 16 |
| MySQL | 8.0 | 8.0 and 8.4 |
| Redis | 7.0 | 7.0 and 7 |
| SQLite | whichever one Python was built with | the runner's |

**Both ends of every range run on every push.** A minimum nobody tests is a number in a table, so
the
oldest supported version of each store answers the whole suite beside the newest.

The floor is where a version is still maintained, and not the oldest one the code could technically
sit on. PostgreSQL 13 went end of life in November 2025 and MySQL 5.7 in October 2023 — a managed
service will not sell you either, and neither has ever answered this suite.

> **MySQL 8.0 is a correctness floor and not a preference.** Before it, InnoDB rebuilt its
> auto-increment counter on startup from the highest id still in the table — so pruning the newest
> settled runs and restarting hands those same ids straight back out. Measured against this schema on
> 5.7.44: the run written after the restart was given the id of the run that had just been pruned,
> where 8.4 gave the next one. That is exactly what `sqlite_autoincrement` exists to prevent, so **an
> id means one run for ever from 8.0 and does not on 5.7**. Prefer 8.4, which is the LTS — 8.0 itself
> reached end of life in April 2026.

## 🐘 SQLAlchemy

```python
from sqlalchemy.ext.asyncio import create_async_engine

from queuefy.store.sqlalchemy import SqlAlchemyStore

store = SqlAlchemyStore(create_async_engine("postgresql+asyncpg://user:pass@host/db"))
await store.setup()
```

Works on PostgreSQL, MySQL and SQLite, where `setup` builds one table, `queuefy_run`, under metadata
of
its own, so it never creates or drops anything of yours.

**The `setup` call survives being made by everybody at once.** `create_all` asks whether the table
is there
and then creates it, which is a question and a statement with a gap in between — ten replicas
booting
together used to leave eight of them dead on that gap, told the table already exists. It asks the
database again rather than reading an error message, so a race carries on and a real refusal still
raises.

Timestamps are held as naive UTC and read back as aware UTC, because MySQL keeps no offset and a
queue that guesses one runs tasks an hour late.

**The three columns that say which run this is are compared code point by code point.** The key, the
queue and the worker are what a claim, a beat and every ending are written against, and MySQL builds a
column under `utf8mb4_0900_ai_ci` unless it is told otherwise — a collation that folds case and accents
away before it compares. Behind the unique index that made `invoice:Bob@shop.com` and
`invoice:bob@shop.com` one occurrence, so the second caller was handed the run of the first and its own
work never happened. A worker serving `mail` claimed and ran what was written to `Mail`, and a worker
named like another closed a run that other one was still holding. They are built as
`utf8mb4_0900_bin` instead, which is what memory, SQLite, PostgreSQL and Redis each already do — a
collation MySQL has had since 8.0.17, which is far below any version still maintained.

The timeout and the two retry delays are held as doubles, because a Python float is one. Held as the
generic float they became a single-precision `FLOAT` on MySQL, where a timeout of 12345.678 came back
12345.7 and one of 3599.999999 came back 3600.0 — while memory, SQLite, PostgreSQL and Redis every one
of them read back the number that was written. Past what a single holds it was not read back changed
but refused outright, which is the same call writing a run on four stores and raising on the fifth.

Three indexes carry the whole load: `(queue, status, priority, due_at)` answers every claim,
`(status, lease_until)` answers every reclaim, and `(status, finished_at)` answers every pruning.

**Everything that writes more than one row reads those rows first and then names them by id.** An
index
is what finds them, and the primary key is what locks them. Naming them inside the write instead let
the database drive the statement off the index the condition reads by, so it locked a secondary
entry
and then reached for the row — while every close of a run locks the row and then reaches for that
same
entry. Two orders around one row is a deadlock, and the one InnoDB rolls back is the close: a run
that
had already finished, left claimed, and its work done all over again a lease later.

**A claim that broke halfway through hands back the runs it had already won.** The batch is taken one
row at a time, so a connection that goes away partway through leaves the worker holding runs whose
attempt is spent and whose lease is running — and it is alive and about to start them. Letting the
database end the pass there drops every one of them: what meets them next is the reclaim their leases
run out into, and `max_attempts` is one by default, so each is failed outright under `LeaseExpired`
with its handler never called and nothing anywhere saying so. A claim that won nothing still raises,
because there is nothing to lose by saying what happened.

**And it holds them against every failure and not only against one the driver raised.** A row is taken
and committed one at a time, so the connection goes back to the pool between two of them — and a pool
with nothing left answers the next one with a `TimeoutError` SQLAlchemy raises itself, which no
`except DBAPIError` ever caught. Measured: nothing is held between two rows of a batch, and a claim
made while the pool is empty raises that refusal — which on a batch of four left two runs claimed with
their attempt spent and handed to nobody. It is an ordinary batch under ordinary load, with nothing
having gone wrong anywhere.

**And a row it could not read is one it never took.** Taking a row and reading it back are one
transaction, so a claim that cannot read what it has just written leaves that row exactly as it found
it. Committed first and read afterwards, the read was a second transaction with the world between the
two — and the row lost in that gap was the same failure worn one row smaller: claimed, its attempt
spent, handed to nobody, and failed a lease later under `LeaseExpired` with its handler never called.

### ⏳ What a production engine needs

```python
create_async_engine(url, pool_pre_ping=True, connect_args={"command_timeout": 15})
```

**A statement that never answers is a worker that never polls again.** The pass is one `await`, so a
connection whose network went away leaves the process alive, holding its runs until their leases run
out — and never coming back on its own. It is the same failure a Redis client without
`socket_timeout`
has, worn differently.

The `pool_pre_ping` option is what replaces a connection the database, a proxy or an idle load
balancer already
dropped, instead of handing it out to fail. On PostgreSQL, asyncpg's `command_timeout` bounds every
statement. On MySQL, aiomysql bounds `connect_timeout` only, so the statement side belongs to the
server — `max_execution_time` covers reads, and a sane `innodb_lock_wait_timeout` covers the rest.

**Size the pool for what a worker does at once, and not for one connection a pass.** A worker beats
for every run it holds, writes an ending for each one as it lands, and polls beside all of that — and
a claim asks the pool again for every row of its batch, because each row is committed on its own and
the connection goes straight back. None of that is a leak and none of it is fixed by waiting: a pool
too small for it hands out `TimeoutError` instead of a connection, which cuts a claim short of what it
was going to take and is logged rather than raised. A pool that comfortably holds the `concurrency` of
each worker in the process, with room over it, is what keeps every pass whole.

### 📁 SQLite across processes

SQLite works with several processes against one file if you ask for it:

```python
engine = create_async_engine(f"sqlite+aiosqlite:///{path}", connect_args={"timeout": 30})

async with engine.begin() as connection:
    await connection.exec_driver_sql("PRAGMA journal_mode=WAL")
```

Without write-ahead logging the second process meets a locked database. With it, this is a fine
setup for a single machine. For several machines, use a database several machines can reach.

The table asks SQLite for a counter that only ever goes up. Left to itself SQLite hands out the
highest
id it can see plus one, so pruning the newest settled run gives that id to the next run written —
and
whoever kept an id from before the pruning goes on to read, and to call off, somebody else's run.
The
sequences of PostgreSQL and the persisted counter of InnoDB never do that, and this is what makes an
id
mean the same thing on all three.

> **An id is the store's own, and it is handed back exactly as it was given.** A database names a run
> with a number and Redis names it with a string, so there is nothing to make uniform without taking
> the meaning out of one of them. Change the type on the way back and the stores stop agreeing: SQLite
> and MySQL quietly read `"41"` as `41` and answer with the run, memory finds nothing and answers with
> nothing, and PostgreSQL refuses the comparison outright — a request handler that read the id off a
> URL works in development and raises in production. Keep it as it came.

## 🔴 Redis

```python
from redis.asyncio import Redis

from queuefy.store.redis import RedisStore

store = RedisStore(Redis.from_url("redis://localhost:6379/0"))
await store.setup()
```

For whoever would rather not put a queue in their database, and `setup` builds nothing — Redis needs
no
schema — and only registers the scripts the store runs.

**The claim, the reclaim and every close are Lua**, so each of them is one atomic step on the
server.
A claim that read, decided and wrote in three round trips would let a second worker in between them,
and two workers would run the same thing.

**A claim answers with the runs themselves and never with their ids.** Reading them back was a round
trip of its own after the script had already taken them, so a connection that went away between the
two left every run of the batch claimed, its attempt spent and its lease running, while the worker
that won them came away holding nothing — and `max_attempts` is one by default, so the reclaim their
leases ran out into failed each of them under `LeaseExpired` with the handler never called. It is the
hole a broken batch opens on the SQL side, met from the other: the step that takes a run is the step
that answers with it, which is one round trip fewer on the path every worker walks on every poll and
nothing at all left in between.

What it keeps:

| Key | What it holds |
| --- | --- |
| `queuefy:run:{id}` | the run itself, as a hash |
| `queuefy:queue:{queue}:{priority}` | one lane per priority, scored by `due_at` |
| `queuefy:priorities:{queue}` | which lanes exist, so a claim walks them highest first |
| `queuefy:leased` | what is running, scored by when its lease runs out |
| `queuefy:settled` | what is over, scored by `finished_at`, so a pruning is a range and not a scan |
| `queuefy:key:{key}` | the reservation that makes an occurrence single |
| `queuefy:sequence` | the id counter |

**One lane per priority is why priority works.** A sorted set orders by one number, and a claim
needs
the highest priority *among what is due* — two questions that one score cannot answer. The lanes are
created on demand, so a deployment that never sets a priority has exactly one.

**A claim gathers the lanes of every queue it serves before it walks any of them.** Priority is what
orders a claim and a queue is only where a run waits, so a worker serving two of them takes the
urgent
run of the second before the ordinary ones of the first. Inside a priority the lanes are merged by
`due_at`, which is the order the other stores read straight out of one index.

**A run is written into the set its own state waits in.** A store is handed whatever run it is given
and every other one reads that state straight back, so a run written as running waits on the leases
and one already over waits among what is pruned. Put in the queue lane whatever it carried, a run its
holder was still working was claimed out from under it by the next worker to ask, and one that was
already over was handed out to be run again and then kept for ever, because a pruning only ever reads
what is settled. This is the write an import, a migration or a fixture makes, and never one an
`enqueue` does.

**Every instant is held as the whole microseconds since the epoch**, which is the finest any store
keeps one to. Held as the seconds a `datetime.timestamp()` hands back it was a double, and a double
stops being able to name a microsecond somewhere past the year 2112: the last instant a datetime holds
came back off it as the first of the year after, which is a refusal raised where a claim builds the
runs it has already taken — the pass ends, everything else in that batch goes back on its lease, and
the run that raised comes round again on every lease from then on. A whole number is exact where
Python reads it and a score wherever Lua sorts it.

The `prefix` renames every key at once, which is what lets this share a Redis with an application
without
ever meeting it.

### 🛠️ What a production client needs

```python
Redis.from_url(url, socket_timeout=15, socket_connect_timeout=5, health_check_interval=30, retry_on_timeout=True)
```

**redis-py waits forever by default.** With no `socket_timeout`, a connection whose network went
away
leaves a worker blocked on a read that never answers — the run stays claimed until its lease expires
and another worker picks it up, but this one is gone and will not come back on its own.

With a timeout the read raises, the worker logs it and the next pass carries on, which is what a
process is supposed to do when a dependency blinks.

The `health_check_interval` is what closes a connection an idle NAT or load balancer already
dropped, and
it is the difference between one failed pass and a worker that answers nothing.

**A client built with `decode_responses` is refused where the store is built.** This store reads
what
Redis answers as bytes, and that setting is one an application sharing its client very often has on.
Nothing below could tell the difference: enqueueing goes on working, so the queue takes everything
it
is given, while every claim of every worker raises — a process that comes up, polls, takes nothing
and
says the same thing once a second as the backlog grows behind it. Give this store a client of its
own,
or one without the setting.

> **Counting is a scan.** Redis has no index over a hash, so `count` walks what the store owns. It is
> for an operator watching depth, not for a hot path.

### ☁️ Redis somebody else runs

Every managed Redis speaks the same protocol, so a url really is the whole of the code — ElastiCache,
Memorystore, Azure Cache, Redis Cloud and the rest, ElastiCache for Valkey included. There are two
settings to get right when you create the instance, and then you never think about it again.

**When you create it, ask for two things.** Turn **cluster mode off**, because sharding is the one
thing this store cannot run on and it is chosen at creation. Set the eviction policy to
**noeviction**, because an eviction takes a run that nothing can give back. On a managed instance that
second one is not a `CONFIG` command, which the provider blocks — it is a field in the parameter group
you attach to the instance.

**Then copy the url.** Take the **primary** endpoint and not the reader one, since every operation here
is a write. With encryption in transit the scheme is `rediss` rather than `redis`, and an auth token
goes where the password goes:

```python
from redis.asyncio import Redis

from queuefy.store.redis import RedisStore

store = RedisStore(Redis.from_url("rediss://:the-auth-token@my-cache.example.com:6379", socket_timeout=15, socket_connect_timeout=5, health_check_interval=30, retry_on_timeout=True))
```

That is all of it. The timeouts are the ones every production client wants and are explained just
above, and nothing else about a managed instance differs from one running on your own machine.

### 🚧 One instance, and never a cluster

> ⚠️ **This store runs on one Redis. Redis Cluster is not supported and will not be.** A replica for
> failover is fine — every write goes to the primary. What is not fine is sharding.

Every script here builds the keys it touches from `ARGV` instead of declaring them in `KEYS`,
because a
claim discovers which run it took only while it runs, and a cluster refuses a script that reaches a
key
it was not given. Slots would scatter the lanes, the leases and the run hashes across nodes, and the
atomic step that makes two workers safe would stop being one.

It is not a port waiting to be written. Every way of forcing the keys into one slot takes a
guarantee
with it:

| If you | Then |
| --- | --- |
| tag every key into one slot | all the data lives on one node anyway, and you have bought a hot slot instead of a shard |
| shard by queue | a claim spanning queues crosses slots, so priority stops being decided across them |
| shard anything | the id counter and the idempotency key are global while the run they name is not, and reserving a key and writing its run stop being one step |

That last one is the end of the argument: a key is what makes an occurrence single, and it cannot be
reserved on one node while the run it names is written on another without a process that dies
between
them poisoning that key for ever.

### 🏢 When one Redis is not enough

Shard where the data already divides — **one queue per tenant, each with its own Redis** — instead
of
asking one queue to span nodes:

```python
from redis.asyncio import Redis

from myapp.settings import REDIS_URLS
from queuefy.app import Queuefy
from queuefy.store.redis import RedisStore


def queue_for(tenant: str) -> Queuefy:
    return Queuefy(RedisStore(Redis.from_url(REDIS_URLS[tenant]), prefix=f"queuefy:{tenant}"))
```

Each tenant gets its own app, its own store and its own workers, and nothing coordinates across them
because nothing has to. Two tenants on one Redis is what `prefix` is for, and two tenants on two
Redis
instances is what this is for. A `Queuefy` is small — the tasks are declared once and each app
registers
the same ones.

If the work does not divide by tenant, that is the signal to put the queue in the database instead:
a
`SqlAlchemyStore` scales on a different axis, and PostgreSQL and MySQL carry a queue far past the
point
where one Redis stops.

> **The reclaim takes a batch, and that is deliberate.** Redis runs a script to the end before it
> answers anybody else, so a pass over an unbounded backlog of expired leases would freeze the server
> for every other client. The `RECLAIM_BATCH` is 500 per pass, and the pass after it takes the next 500.
> **Every store takes the same batch**, for the same reason worn differently: a cluster that died
> holding a hundred thousand runs is one statement over a hundred thousand rows, in one transaction,
> issued by every surviving worker at once.

### 🧨 Nothing may evict a run

**The `maxmemory-policy` has to be `noeviction`** on whatever Redis holds this, or the queue belongs
to a
database of its own. A run is a hash and the queue it waits in is a sorted set, and an eviction
takes
the first without touching the second: the lane keeps handing out an id whose run is gone.

The store steps over that instead of building a hash out of the fields a claim writes — the claim,
the
read that follows it and the reclaim all check the run is still there — so what an eviction costs is
the run itself, silently, which is the part no code on either side can give back.

Setting `allkeys-lru` on a shared Redis is what does this, and it is a common thing to inherit from
whoever set the instance up.

## 🧠 Memory

```python
from queuefy.store.memory import MemoryStore
```

The whole library minus durability: right for tests and for a single process, wrong for two, because
nothing outside the process that owns it can see a thing.

## ✍️ Writing your own

Subclass `queuefy.store.base.Store` and answer fourteen methods. The contract is short, and one rule
runs through all of it:

> **Every method that changes a run is conditional on the state that run was in.**

A `claim` only takes a run that is `pending` and due. Then `complete`, `fail`, `retry_later`,
`release`
and `heartbeat` only touch a run whose `worker` is the caller, whose `attempts` is the attempt the
caller
was handed, and whose status is `running`. And `add` refuses a key that is taken. That is what makes
two
workers safe without a lock anywhere, and a store that answers "changed" for a row it did not change
breaks the guarantee for everybody.

A second rule sits beside it, and it is the one easiest to write a store past:

> **What a call changes, that call answers for — so it changes everything or it changes nothing.**

A run a claim moved to `running` has to be a run that claim handed back. Take the row, commit, and
then go and read it, and the read is a second trip with the world between it and the first: a
connection that went away there, or a pool with nothing left to hand one out, leaves that run
claimed with its attempt spent and its lease running while the worker came away holding nothing. What
comes for it next is the reclaim its lease runs out into, and `max_attempts` is one by default — so
it is failed under `LeaseExpired` with its handler never called and nothing anywhere saying so. The
taking and the answering are therefore one step: one script on the server for Redis, one transaction
for a database, and never a write committed and afterwards read. The same holds for every ending, for
a pruning and for the reservation that makes a key single.

The attempt is asked for beside the name because a name is not enough to say which run this is. A
worker
whose beat could not reach the store meets its own expired lease on its own next pass, takes it back
and
claims the very same run again — under the very same name. From then on the attempt it lost and the
attempt it is running are both the caller, and a condition written on the name alone lets the one
that
lost the run close it, or put it back in the queue while the other is still mid handler. A claim
mints an
attempt nobody else is on, so the two of them together are what name the holder.

The `retry_later` and `release` are the same write with one difference: an attempt that happened
stands,
and one a worker never had anything to try is given back. A store that treats them as the same
method
spends a run's whole budget on a rolling deploy.

A lease is held up to the instant it runs out and not up to the one before it, and a pruning drops a
run that finished **strictly** before the instant it was given. Both are boundaries a store is easy
to
get a microsecond wrong on, and a reclaim a moment early puts a run on a second worker while the
first
is still working it.

The suite in `tests/test_store_contract.py` is written against the interface and parametrized over
every store. Add yours to the fixture and it inherits the whole thing — that is the intended way to
know a new backend is correct.

Beside it, `tests/test_differential.py` runs one seeded script of every operation against your store
and against `MemoryStore`, and compares every field of every run, every answer and every count. A
suite
written by hand only asks the questions somebody thought to ask, and two of the drifts found so far
were
answers nobody had thought to compare.

And `tests/test_interruptions.py` refuses one round trip of one call at a time — a statement or a
commit for a database, a command for Redis — and reads what your store left behind each time. That is
the second rule above, asked at every point a call can stop at rather than at the one somebody chose.
A store that writes a run over two round trips passes the other two suites and fails this one.

Each round trip is refused twice over: once by a connection that went away, which ends the call, and
once by the deadlock a database asks to be asked again after. The second is what reads whether your
rollback left the session usable and whether the statement asked a second time landed once rather
than twice, which is the half of `under_contention` nothing else walks at more than one point.
