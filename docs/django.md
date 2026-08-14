# 🎸 Django

Read [Frameworks](frameworks.md) first for what is true everywhere. This page is the part that is
Django's alone, and Django is the framework where the synchronous question actually bites.

## 🔀 Synchronous at heart

Django's ORM is synchronous and the worker is asyncio, so **how a handler is declared decides whether
it may call the ORM at all**. Measured against a real project:

| The handler | What happens |
| --- | --- |
| plain `def` calling the ORM | **works**, because the run is handed to a thread and that is where a synchronous ORM belongs |
| `async def` calling the ORM | **fails** with `SynchronousOnlyOperation`, and the run ends as that failure |
| `async def` wrapping it in `sync_to_async` | works |
| `async def` using the async ORM, `aget` and its friends | works |

**Declare it as a plain `def` and the question never comes up.** That is the shape most Django work
wants anyway, and the worker already runs plain handlers off the loop so they never block the runs
beside them.

## 🛠️ The worker is a management command

```python
# myapp/management/commands/run_worker.py
import asyncio

from django.core.management.base import BaseCommand

from queuefy.worker import Worker

from myapp.queue import app


async def poll():
    await app.setup()
    await Worker(app, concurrency=8).run()


class Command(BaseCommand):
    help = "works the queue until it is told to stop"

    def handle(self, *args, **options):
        asyncio.run(poll())
```

Run it beside your web process, under the same supervisor, and stop it the same way. Measured: the
command's `handle` drove a worker that claimed the runs both kinds of view had written, and each of
them finished green.

## 🧭 The engine reads the settings

Point the queue at the database Django already has, built from the settings rather than written down
twice:

```python
from django.conf import settings


def database_url() -> str:
    wanted = settings.DATABASES["default"]

    return f"postgresql+asyncpg://{wanted['USER']}:{wanted['PASSWORD']}@{wanted['HOST']}:{wanted['PORT']}/{wanted['NAME']}"
```

## 📨 Enqueueing from a view

A plain view is synchronous code calling an asynchronous queue, and `async_to_sync` is what bridges it.
Django ships asgiref, so there is nothing to install:

```python
from asgiref.sync import async_to_sync
from django.shortcuts import redirect

from myapp.accounts import create_account
from myapp.queue import app


def signup(request):
    account = create_account(request.POST)
    async_to_sync(app.enqueue)("send_email", to=account.email, subject="Welcome")

    return redirect("home")
```

An async view awaits it like anything else, with nothing in between:

```python
from django.shortcuts import redirect

from myapp.accounts import create_account
from myapp.queue import app


async def signup(request):
    account = await create_account(request.POST)
    await app.enqueue("send_email", to=account.email, subject="Welcome")

    return redirect("home")
```

Both were measured against a real project, and both wrote the run.

## ⚠️ Transactions do not cover the queue

> **A run enqueued inside `atomic()` is not rolled back with it.** The queue writes on its own
> connection, so the row lands whatever the surrounding transaction goes on to do. Measured: a view
> that enqueued and then raised inside `atomic()` left the run in the queue, and a worker went on to
> run it for an account the rollback had already thrown away.

The `transaction.on_commit` hook is the fix, and it is the same idiom Django already asks for when
talking to anything outside the database:

```python
from asgiref.sync import async_to_sync
from django.db import transaction

from myapp.accounts import create_account
from myapp.queue import app

with transaction.atomic():
    account = create_account(request.POST)
    transaction.on_commit(lambda: async_to_sync(app.enqueue)("send_email", to=account.email))
```

## 🗃️ A handler and the ORM

```python
from django.db import close_old_connections
from django.utils import timezone

from myapp.mail import mailer
from myapp.models import Account
from myapp.queue import app


@app.task("send_welcome", max_attempts=5)
def send_welcome(account_id: int):
    close_old_connections()

    account = Account.objects.get(pk=account_id)
    mailer.send(account.email)

    account.welcomed_at = timezone.now()
    account.save(update_fields=["welcomed_at"])
```

Measured on this shape: the handler read and wrote through Django's own models and its write landed in
Django's own table, while the queue kept its runs in a table it never shares.

## 🧹 Closing the connections nobody else will

Django drops a connection at the end of a request, and a worker has no requests. Measured: forty
handlers ran on eight threads and opened eight connections, one per thread, each reused by every run
after it. They do not multiply, which is the good news — and they do persist, which is why one the
server has since dropped sits waiting for whichever handler lands on that thread next.

Calling `close_old_connections` first is what Celery does, for the same reason.
