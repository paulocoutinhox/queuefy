# 🌐 FastAPI and Starlette

Read [Frameworks](frameworks.md) first for what is true everywhere. This page is only what FastAPI and
Starlette ask of you, and they ask the least of the three.

## ⚡ Asynchronous all the way

Both are asynchronous, and so is the worker, so nothing has to be bridged in either direction. Routes
await `enqueue`, handlers are `async def`, and sessions are the async ones the application already has.

A plain `def` handler still works and still runs off the loop in a thread — reach for it when a task
calls something synchronous, a library with no async client of its own.

## 🤝 A worker beside the api

The helper lives in `queuefy.asgi` and not under any framework's name, because what it honours is the
**ASGI lifespan protocol** — the same one Starlette, Litestar, Quart and BlackSheep speak. Nothing in
it imports FastAPI, so the code below is the whole integration for any of them:

```python
from fastapi import FastAPI

from queuefy.asgi import lifespan_for
from queuefy.worker import Worker

from myapp.queue import app

worker = Worker(app, concurrency=8)
api = FastAPI(lifespan=lifespan_for(worker))
```

Here `app` is the queue and `api` is the web application, and they stay two names because each of them
answers to one thing.

The lifespan builds the store, starts the worker with the process, and on shutdown stops the polling
loop and waits for what is in flight. A deploy loses no work.

## 📨 Enqueueing from a route

A request only writes a row:

```python
from myapp.accounts import SignUpRequest, create_account
from myapp.queue import app

from .main import api


@api.post("/signup")
async def signup(payload: SignUpRequest):
    account = await create_account(payload)
    await app.enqueue("send_email", to=account.email, subject="Welcome")

    return account
```

The response does not wait for the mail server.

A `def` route works too. FastAPI runs those in a threadpool, so there is no loop in that thread and
`async_to_sync` from asgiref bridges it — measured through a real client, answering 200 either way:

```python
from asgiref.sync import async_to_sync


@api.post("/signup-sync")
def signup_sync(payload: SignUpRequest):
    account = create_account(payload)
    async_to_sync(app.enqueue)("send_email", to=account.email, subject="Welcome")

    return account
```

## 🔗 A handler and the database

A handler reaches the database the same way a route does, through the session factory the application
already has:

```python
# myapp/db.py
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from myapp.settings import DATABASE_URL

engine = create_async_engine(DATABASE_URL, pool_pre_ping=True)
sessions = async_sessionmaker(engine, expire_on_commit=False)
```

```python
# myapp/queue.py
from datetime import datetime, timezone

from myapp.db import sessions
from myapp.mail import mailer
from myapp.models import Account


@app.task("send_welcome", max_attempts=5, timeout=30)
async def send_welcome(account_id: int):
    async with sessions() as session:
        account = await session.get(Account, account_id)
        await mailer.send(account.email)

        account.welcomed_at = datetime.now(timezone.utc)
        await session.commit()
```

> ⚠️ **Never put `Depends` on a handler, because it does not fail — it lies.** FastAPI resolves
> dependencies in the routing layer, and a run is claimed by a worker with no request around it. So the
> parameter simply takes its default, and its default **is the `Depends` object itself**. Measured: a
> handler declared `given: str = Depends(a_dependency)` ran, finished green, and received
> `Depends(dependency=<function a_dependency>, use_cache=True, scope=None)` as a string. Nothing
> anywhere says a word.

Take the session factory, the settings object and the client straight from the module that builds them,
which is the same module a dependency would have been reading from anyway.

Measured on this shape: the handler wrote through the application's own session while the queue kept
its runs in its own table, with the two engines separate and again with a single engine shared by both.

## 👥 Several processes

Running `uvicorn --workers 4` gives four processes, each with its own worker. Nothing changes: they
coordinate through the store, so the nightly report still runs once.

## 📊 Watching it

```python
from queuefy.run import RunStatus


@api.get("/queue")
async def depth():
    return {"pending": await app.count(status=RunStatus.PENDING), "running": await app.count(status=RunStatus.RUNNING), "failed": await app.count(status=RunStatus.FAILED)}
```

Pending climbing means not enough workers. Failed climbing means something is broken, and the run
carries the message and the class that broke it.
