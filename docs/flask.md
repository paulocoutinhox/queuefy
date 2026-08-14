# 🍶 Flask

Read [Frameworks](frameworks.md) first for what is true everywhere. Flask is the simplest of the three
to reason about and the one that needs the most said out loud, because none of it is asynchronous.

## 🔁 Synchronous, with no event loop

A WSGI application has no event loop at all, which decides two things and leaves nothing else open.

**The worker does not live in the web process.** There is nothing to attach it to, so it runs as the
[standalone](frameworks.md) process, started by the same supervisor that starts the web one.

**Handlers are plain `def`.** They are handed to a thread, which is where Flask-SQLAlchemy, requests
and every other synchronous library already expect to be. Write `async def` only for a task that
genuinely calls something asynchronous.

## 🚀 The worker is its own process

```python
# worker.py
import asyncio

from queuefy.worker import Worker

from myapp.queue import app


async def main():
    await app.setup()
    await Worker(app, concurrency=8).run()


asyncio.run(main())
```

## 📨 Enqueueing from a view

A view is synchronous code calling an asynchronous queue, so `async_to_sync` bridges it. Unlike
Django, **Flask does not ship asgiref**, so ask for it:

```bash
pip install asgiref
```

```python
from asgiref.sync import async_to_sync
from flask import redirect, request

from myapp.accounts import create_account
from myapp.queue import app
from myapp.web import web


@web.post("/signup")
def signup():
    account = create_account(request.form)
    async_to_sync(app.enqueue)("send_welcome", account_id=account.id)

    return redirect("/")
```

An `async def` view works too, since Flask runs those through the same asgiref you just installed, and
there it awaits `enqueue` directly:

```python
from flask import redirect, request

from myapp.accounts import create_account
from myapp.queue import app
from myapp.web import web


@web.post("/signup-async")
async def signup_async():
    account = create_account(request.form)
    await app.enqueue("send_welcome", account_id=account.id)

    return redirect("/")
```

Both were measured through a real client and answered 200. On the synchronous shape, also fifty
enqueues in a row from one thread and twelve request threads at once, with no failures and no lost
rows — that is what a WSGI server is, so it is what was tested.

## 🧱 A handler and the application context

> ⚠️ **A handler runs outside the application context, and Flask-SQLAlchemy needs one.** There is no
> request around a run, so `db.session` has nothing to bind to. Measured: a handler that touched
> `db.session` without a context failed with `RuntimeError: Working outside of application context`,
> and the same handler inside one read, wrote and committed.

Push a context around the work, which is the same line a Flask CLI command or a script would need:

```python
from myapp.mail import mailer
from myapp.models import Account, db
from myapp.queue import app
from myapp.web import web


@app.task("send_welcome", max_attempts=5)
def send_welcome(account_id: int):
    with web.app_context():
        account = db.session.get(Account, account_id)
        mailer.send(account.email)

        account.state = "welcomed"
        db.session.commit()
```

Anything else the application context carries — `current_app`, the configuration, an extension that
registered itself on it — is available for the same one line.

## ⚖️ What this costs

Nothing that matters, and it is worth saying plainly: the worker holds a thread per running task rather
than a coroutine, so `concurrency` is bounded by threads instead of by memory. For work that waits on a
network — sending mail, calling an api, writing a row — that ceiling is far above what a queue of this
shape ever reaches.
