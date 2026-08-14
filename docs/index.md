# ⚡ Queuefy

A task queue and a scheduler that are the same thing.

## 💡 The one idea

Everything this library does reduces to a single row: **a run that is due, which exactly one worker
claims**. An immediate task, an interval, a fixed datetime and a cron expression differ only in *when
the next run is written*, and in nothing else.

That is why there is no separate scheduler process, no leader election and no broker:

| Question | Answer |
| --- | --- |
| who runs a due run? | whoever wins a conditional `UPDATE ... WHERE status = 'pending'` — the store decides |
| what stops ten workers writing ten copies of the 04:00 run? | a unique key per occurrence, `name@2026-08-01T04:00:00+00:00` |
| what happens when a worker dies mid-run? | its lease runs out and the run goes back to the queue |
| what elects the scheduler? | nothing. every worker writes the next occurrence, and the key leaves one |

## 🚧 What it does not do

- **It is not a broker.** Delivery is polled, not pushed, so a run becomes due within one poll interval and not within a microsecond.
- **It does not spread a single run across workers.** One run, one worker.
- **It does not keep a result store.** A run holds a small JSON result, and anything bigger belongs where your data lives.

## 🧭 Where to go next

Start at [Getting started](getting-started.md), then read [Tasks](tasks.md) for the four kinds and what
a name means.

After that, in whatever order the question comes up:

| Page | When you want it |
| --- | --- |
| [Workers](workers.md) | concurrency, leases, pruning and many machines |
| [Stores](stores.md) | which database or Redis, what versions, and writing your own |
| [Frameworks](frameworks.md) | running it under FastAPI, Django, Flask or nothing at all |
| [Retries and failures](retries.md) | policies, timeouts and what a failed attempt becomes |
| [Hooks](hooks.md) | being told about every run, for an audit trail or a metric |
| [Contribution](contribution.md) | the suite, the servers it wants, and what a change is asked for |
