# 🧩 Frameworks

Everything on this page is true of every framework. Each one then has a page of its own, because what
differs between them is not the queue — it is whether their world is synchronous or asynchronous.

## 📚 Pick yours

| Framework | Its world | Where the worker lives |
| --- | --- | --- |
| [FastAPI and Starlette](fastapi.md) | asynchronous | inside the web process, on the lifespan |
| [Django](django.md) | synchronous at heart, asynchronous where you ask | a management command |
| [Flask](flask.md) | synchronous | a process of its own |
| Anything else | either | wherever you can run an asyncio task |

## 🧭 The one rule

A worker is an asyncio task that polls. **Wherever you can run one, you can run the queue** — inside
the web process, beside it, or in a process of its own. Nothing here is framework-aware, and the
framework never learns the queue exists.

The queue gets **an async engine of its own**, pointed at the same database your application already
uses. Handing it the application's engine works too — both were measured — but the default is separate,
because a worker polls every second and holds a connection while it claims, and a pool shared with
request traffic is one where the two starve each other under load.

**The library reads no environment variable, ever.** You build the engine or the Redis client and hand
it over, so configuration stays in the one place your application already keeps it — your settings
module, and never a name this package decided on behind you.

```python
# myapp/queue.py — imported by the web process and by the worker alike, because a name has to mean the same thing on both sides
from sqlalchemy.ext.asyncio import create_async_engine

from myapp.settings import DATABASE_URL
from queuefy.app import Queuefy
from queuefy.store.sqlalchemy import SqlAlchemyStore

app = Queuefy(SqlAlchemyStore(create_async_engine(DATABASE_URL, pool_pre_ping=True)))


@app.task("send_email", max_attempts=5, timeout=30)
async def send_email(to: str, subject: str):
    ...
```

## ⚖️ Synchronous or asynchronous

This is the only question that changes anything, and it is asked twice — once of the handler, and once
of the code that enqueues.

**A handler may be either, and both are first class.** An `async def` runs on the event loop, so
blocking there stalls every run beside it. A plain `def` is handed to a thread, which is exactly where
synchronous libraries belong. Pick the one that matches what the handler actually calls, and the
framework pages say which that is for each of them.

**Enqueueing follows the caller.** Asynchronous code awaits `enqueue` like anything else. Synchronous
code bridges it with `async_to_sync` from asgiref, measured here at fifty calls in a row from one
thread and twelve request threads at once, with no failures and no lost rows.

## 🔌 What a handler may touch

**Anything the rest of your application can.** The two are independent in storage and joined in code:
the queue keeps its runs in `queuefy_run` or in Redis and never looks at your tables, while a handler
is ordinary application code that may use your ORM, your models, your settings and your clients.

The payload carries an **id and never an object**, because it has to survive a trip through JSON. The
handler reads the row itself, which is also what you want: by the time a run is claimed the row may
have moved on, and the run should work on what is true now.

> **A handler runs outside whatever the framework wraps a request in.** There is no request, no session
> scope and no dependency injection around it, so each framework asks for one small thing back. Its page
> says which, and it is one line in every case.

The handler's own commit and the queue's bookkeeping are **two transactions**, so a run whose work
committed and whose outcome then failed to record comes back and runs again. That is the at-least-once
boundary this library is built on, and the answer is always the same: make the handler idempotent, or
give the task `max_attempts=1`.

## 🖥️ Standalone

No framework at all — a process that does nothing but work the queue:

```python
import asyncio

from queuefy.worker import Worker

from myapp.queue import app


async def main():
    await app.setup()
    await Worker(app, concurrency=8).run()


asyncio.run(main())
```

The `run` loop polls until `stop`, and then lets what is in flight land. Ask for `stop` on whatever
signal your process manager sends, because asking is what buys the `grace`: nothing new is claimed,
and what is running has that long to finish before its lease is left to bring it back.

Cancelling the task instead ends the polling just as surely and closes the threads the worker kept
for its plain handlers, but it never waits — a cancel means now, and what was running when it landed
goes back on its lease rather than being given the `grace`. Both are clean shutdowns and neither
loses work, so the choice is only whether a deploy waits for what is in flight.

## ✂️ Separating the worker from the web process

Past a certain size, run the queue where web traffic cannot starve it. Nothing changes but where the
process lives: the web side builds the queue and only enqueues, the worker side builds the same queue
and only works it. Both import the same task declarations, because a name has to mean the same thing on
both sides.

That is also how you scale the two apart — sixteen workers against two web processes, or the reverse,
without either knowing.

## 📈 Watching it

```python
from queuefy.run import RunStatus

depth = {"pending": await app.count(status=RunStatus.PENDING), "running": await app.count(status=RunStatus.RUNNING), "failed": await app.count(status=RunStatus.FAILED)}
```

Pending climbing means not enough workers. Failed climbing means something is broken, and the run
carries the message and the class that broke it.
