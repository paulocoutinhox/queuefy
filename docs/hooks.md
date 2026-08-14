# 🪝 Hooks

A worker tells the application about every run it handles. That is how an audit trail, a metric or an
alert is kept without the queue ever knowing what any of those are.

```python
import logging

from myapp.queue import app
from queuefy.worker import Worker

logger = logging.getLogger(__name__)
worker = Worker(app)


@worker.on_start
async def started(run):
    logger.info("[queue] %s started", run.name)


@worker.on_finish
async def finished(run, result, seconds):
    logger.info("[queue] %s finished in %.2fs", run.name, seconds)


@worker.on_error
async def failed(run, error, seconds, retrying):
    logger.error("[queue] %s broke: %s (%s)", run.name, error, "coming back" if retrying else "given up")
```

| Stage | Arguments |
| --- | --- |
| `on_start` | `run` |
| `on_finish` | `run`, what the handler answered as the store holds it, seconds it took |
| `on_error` | `run`, the exception, seconds it took, whether it comes back for another attempt |

What every stage is handed is the `Run` — one execution, carrying its own `attempts`, `payload` and
`key` — and never the `Task` that declared it. The two are different objects, and only the run knows
which execution this was.

A listener may be a coroutine or a plain function, and every listener of a stage is told in the order
it was registered. Registering hands the listener back, so it works as a decorator or as a plain call.

> **Register an `on_error`, because nothing else logs a handler that raised.** The library says nothing
> when work fails — what broke is written into the `error` and `error_type` of the run and nowhere else,
> which is why the message is sized for being the only record there is. Without a listener a queue that
> is failing every run looks exactly like a queue that is idle, and the two numbers that tell them apart
> are `count(status=RunStatus.FAILED)` and a `count(status=RunStatus.RUNNING)` that never comes down.

## 🛡️ A listener that breaks breaks alone

A listener that raises is logged and nothing else. It never changes the outcome of the run, and it
never stops the listeners after it.

That rule is not politeness — an audit table that is full, or a metrics endpoint that is down, must
not turn a task that worked into a task that failed.

**Alone means alone, and `SystemExit` is not an `Exception`.** A library calling `sys.exit()` somewhere
deep inside a listener is caught here for the same reason it is caught around a handler: announced
before the work starts, it used to end the attempt where it stood — the handler never ran, no outcome
was ever written, and the run sat claimed until a lease closed it as one nobody answered for.

Only `asyncio.CancelledError` still goes through. That is the shutdown asking, and a
listener is not allowed to make a worker nobody can stop.

## 🤐 An ending the store would not take is not announced

A worker whose lease ran out while it was working no longer holds the run — somebody else took it over
and is running it too. When that worker finishes, the store refuses its outcome, and **the listeners
are not told**, whichever way the attempt ended.

Without that, one run writes two lines into an audit trail: one here, for an outcome that was thrown
away, and one under the worker that actually holds it. What the listeners are told is exactly what the
store recorded, and the dropped outcome is a warning in the log instead.

## 📒 Writing an audit trail

```python
from myapp.db import SessionLocal
from myapp.models import AuditLog


@worker.on_finish
async def record(run, result, seconds):
    async with SessionLocal() as session:
        session.add(AuditLog(name=run.name, attempts=run.attempts, seconds=seconds))
        await session.commit()
```

The run carries everything worth writing down: `name`, `queue`, `attempts`, `payload`, `key` and the
timestamps the store filled in.
