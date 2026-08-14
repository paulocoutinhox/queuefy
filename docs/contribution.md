# 🤝 Contribution

Thanks for wanting to help. 🙌

## 🚀 Getting set up

```bash
make install
make test
```

The suite runs against memory and SQLite with nothing else installed. The other three stores take
part
only when their server is reachable, and are simply not collected when it is not:

```bash
make servers
```

That starts a Redis on 6399, a MySQL on 3399 and a PostgreSQL on 5499. Point the suite somewhere else
with `QUEUEFY_REDIS_URL`, `QUEUEFY_MYSQL_URL` and `QUEUEFY_POSTGRES_URL`.

> **Those three belong to the suite and not to the package.** They are read by `tests/conftest.py` and
> by nothing else — `queuefy` itself reads no environment variable anywhere, and is handed an engine or
> a client instead. There are three rather than one because the suite uses all three **at once**: every
> store answers the same contract in the same run, so a single url would quietly leave two of them
> untested.

> **A `make coverage` run needs all three.** The gate is 100%, and a store nobody could reach is a store
> whose lines nobody ran — the gate fails and tells you exactly which. The plain `make test` works without them.

> **One session at a time against a server.** Every suite here owns the whole store: the ordinary one
> drops the table before each test it runs, and the stress one has six machines working it. Two
> sessions against one server therefore leave both of them reading a table the other just took away,
> and what that looks like is a run failing somewhere it never touched — a relation that does not
> exist, or a queue that carried none of the work. It is the one failure here that says nothing at all
> about the code, and the only thing telling it from a real one is remembering what else was running.
> So let `make test` finish before starting `make stress`, or point one of them at a server of its own
> with the three urls above. The pipeline never meets this, because every job there declares its own
> containers.

> **The drivers come with `make install`, and they have to.** A store takes part whenever its port
> answers, and the port answering is the whole test — so with the servers up and a driver missing, every
> test of that store fails on building the engine instead of being quietly left out. That is why
> `aiomysql`, `asyncpg` and the `cryptography` MySQL 8 authenticates with are development tools here
> rather than extras of the package: nothing in `queuefy` imports them, and the suite cannot run without
> them once the servers are up.

**Run the suite against a real MySQL before believing anything about MySQL.** It rounds a `DATETIME`
with no fractional precision, which once stored a run due at `10:00:00.9` as due at `10:00:01` — a
second in the future, where nothing claimed it until the second turned. SQLite says nothing about
that, and neither does PostgreSQL.

## ✅ Before you open a pull request

```bash
make format
make coverage
make lint
```

Three things the pipeline will check anyway, and it is faster to hear it from your own machine.

**Touched a store? Run `make stress` too.** It is many machines against every server that answers,
minutes rather than seconds, which is why it is left out of every ordinary run. What it reaches is
the interleaving, and three of the worst bugs found so far were invisible to a suite already at 100%
without it — the last of them two statements locking one row in two orders, a deadlock no single
connection can ever see. The release runs it before publishing, because a version on PyPI is
permanent.

**The other way past a gate at 100% is already in `make test`.** Load is not the only thing a graded
run never applies — a failure at one named point is the other, and the sweep in
`tests/test_interruptions.py` costs seconds rather than minutes, so it runs on every ordinary pass
instead of waiting for somebody to ask for it.

## 📐 What the project asks of a change

**Coverage stays at 100%, branches included.** It is a gate and not an aspiration. A line nobody
exercises is a line nobody knows the behaviour of.

**A new store answers the same contract.** `tests/test_store_contract.py` is written against the
interface and parametrized over every store — add yours to the fixture in `tests/conftest.py` and it
inherits the whole suite. That is the intended way to know a backend is correct, and a store that
passes it behaves exactly like the others.

**And it answers a whole call or none of one.** The sweep in `tests/test_interruptions.py` cuts one
round trip of one call at a time — a statement or a commit for a database, a command for Redis — and
reads what that call left behind. A store that writes a run over two round trips passes the contract
suite and fails this one, because what it can leave behind is half of a run: a claim that took a row
and handed it to nobody, or a key held by nothing.

**A test never waits without a bound.** Use `wait_until` from `tests/conftest.py`. A loop that spins
until something happens is a pipeline that burns for hours when it does not.

**A name means one thing.** A *task* is what you declare, a *run* is one execution of it, and a
*queue* is a lane. Mixing them is how documentation stops being true.

## 🎨 Style

Both `ruff` and `black` decide formatting, and `make format` runs them in the order that settles:
the
linter first, because it removes imports the formatter already laid out.

Calls stay on one line — the line length is 320 for exactly that reason.

Comments are rare, lowercase, one sentence, and explain **why**. If a comment says what the line
does,
the line should have been clearer instead.

## 🐛 Reporting something

An issue that shows how to reproduce is worth ten that describe. If it is a race, say how many
workers
and which store — those two answer most of it.
