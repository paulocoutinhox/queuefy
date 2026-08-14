import json
from datetime import datetime, timedelta

from redis.asyncio import Redis

from queuefy.clock import EPOCH, as_utc
from queuefy.errors import QueueError
from queuefy.retry import RetryPolicy
from queuefy.run import Run, RunStatus
from queuefy.store.base import CLAIM_SPREAD, RECLAIM_BATCH, Store

# every key this store owns starts here, so it shares a database with an application without ever meeting it
PREFIX = "queuefy"

# what an instant is counted in here, because it is the finest every store keeps one to and the widest a whole number of them stays exact past
MICROSECOND = timedelta(microseconds=1)

# what a claim does has to be one step to the server: find the highest priority due run, take it, move it to the leases and read it back — three round trips would let a second worker in between them, and reading the runs afterwards left every one of them claimed when the store stopped answering between the two
CLAIM = """
local prefix = ARGV[6]
local wanted = tonumber(ARGV[4])
local taken = {}

-- the levels of every queue this worker serves, gathered before any of them is walked: a queue is where a run waits and never what orders it, so serving one queue to its end first hands out an ordinary run while an urgent one waits next door
local levels = {}
local seen = {}

for _, queue in ipairs(KEYS) do
  for _, level in ipairs(redis.call('ZREVRANGE', prefix .. ':priorities:' .. queue, 0, -1)) do
    if not seen[level] then
      seen[level] = true
      table.insert(levels, level)
    end
  end
end

table.sort(levels, function(left, right) return tonumber(left) > tonumber(right) end)

for _, level in ipairs(levels) do
  if #taken >= wanted then break end

  -- every lane of this level, merged by when each run came due and then by which was written first, which is the order the other stores read straight out of one index
  local ready = {}

  for _, queue in ipairs(KEYS) do
    local lane = prefix .. ':queue:' .. queue .. ':' .. level
    local due = redis.call('ZRANGEBYSCORE', lane, '-inf', ARGV[1], 'WITHSCORES', 'LIMIT', 0, tonumber(ARGV[5]))

    for index = 1, #due, 2 do
      table.insert(ready, {id = due[index], due_at = tonumber(due[index + 1]), lane = lane})
    end
  end

  table.sort(ready, function(left, right)
    if left.due_at ~= right.due_at then return left.due_at < right.due_at end

    return tonumber(left.id) < tonumber(right.id)
  end)

  for _, entry in ipairs(ready) do
    if #taken >= wanted then break end

    if redis.call('ZREM', entry.lane, entry.id) == 1 then
      local key = prefix .. ':run:' .. entry.id

      -- a run an eviction took leaves its id in the lane, and writing the fields of a claim over nothing would build a hash that holds those fields and no run
      if redis.call('EXISTS', key) == 1 then
        redis.call('HSET', key, 'status', 'running', 'worker', ARGV[2], 'started_at', ARGV[1], 'lease_until', ARGV[3])
        redis.call('HINCRBY', key, 'attempts', 1)
        redis.call('ZADD', prefix .. ':leased', tonumber(ARGV[3]), entry.id)

        -- the run is read where it was taken, so what the claim answers with is what the claim took. read in a round trip of its own, a connection that went away between the two left every run of the batch claimed, its attempt spent and its lease running, with the worker that won them holding nothing — and `max_attempts` at one fails each of them under `LeaseExpired` with the handler never called
        table.insert(taken, redis.call('HGETALL', key))
      end
    end
  end
end

return taken
"""

# a lease nobody renewed belongs to a process that is gone, and what it held goes back or ends here. it takes a batch and not everything: redis runs a script to the end before it answers anybody else, so an outage that expired a hundred thousand leases would freeze the whole server — the rest is picked up by the next pass. the range stops short of the instant itself, because a lease running out exactly now is one every other store still holds — and taking it back a moment early puts the run on a second worker while the first is still working it
RECLAIM = """
local expired = redis.call('ZRANGEBYSCORE', ARGV[2] .. ':leased', '-inf', '(' .. ARGV[1], 'LIMIT', 0, tonumber(ARGV[3]))

for _, id in ipairs(expired) do
  local key = ARGV[2] .. ':run:' .. id
  redis.call('ZREM', ARGV[2] .. ':leased', id)

  local attempts = redis.call('HGET', key, 'attempts')

  -- a run somebody deleted by hand leaves its id behind here, and reading a field it no longer has would end this script for everybody, for good
  if attempts ~= false then
    if tonumber(attempts) >= tonumber(redis.call('HGET', key, 'max_attempts')) then
      redis.call('HSET', key, 'status', 'failed', 'finished_at', ARGV[1], 'error', 'the worker holding this run stopped answering', 'error_type', 'LeaseExpired', 'worker', '', 'lease_until', '', 'started_at', '')
      redis.call('ZADD', ARGV[2] .. ':settled', tonumber(ARGV[1]), id)
    else
      redis.call('HSET', key, 'status', 'pending', 'due_at', ARGV[1], 'worker', '', 'lease_until', '', 'started_at', '')
      redis.call('ZADD', ARGV[2] .. ':queue:' .. redis.call('HGET', key, 'queue') .. ':' .. redis.call('HGET', key, 'priority'), tonumber(ARGV[1]), id)
    end
  end
end

return #expired
"""

# only the attempt that holds a run may close it, and the check and the write are one step so nothing slips between them. the attempt is asked for and not only the worker, because a worker that met its own expired lease claims the very same run again under the very same name — and from then on the attempt it lost and the one it is running are both the caller
SETTLE = """
local key = ARGV[5] .. ':run:' .. ARGV[1]

if redis.call('HGET', key, 'worker') ~= ARGV[2] or redis.call('HGET', key, 'status') ~= 'running' or tonumber(redis.call('HGET', key, 'attempts')) ~= tonumber(ARGV[7]) then return 0 end

redis.call('ZREM', ARGV[5] .. ':leased', ARGV[1])
redis.call('HSET', key, unpack(cjson.decode(ARGV[3])))

-- what the attempt just made counts for: nothing when it happened, and one back for a worker that never had anything to try
redis.call('HINCRBY', key, 'attempts', ARGV[6])

if ARGV[4] ~= '' then
  redis.call('ZADD', ARGV[5] .. ':queue:' .. redis.call('HGET', key, 'queue') .. ':' .. redis.call('HGET', key, 'priority'), tonumber(ARGV[4]), ARGV[1])
else
  redis.call('ZADD', ARGV[5] .. ':settled', tonumber(redis.call('HGET', key, 'finished_at')), ARGV[1])
end

return 1
"""

# the lease moves only while the attempt that took it is the one asking
HEARTBEAT = """
local key = ARGV[4] .. ':run:' .. ARGV[1]

if redis.call('HGET', key, 'worker') ~= ARGV[2] or redis.call('HGET', key, 'status') ~= 'running' or tonumber(redis.call('HGET', key, 'attempts')) ~= tonumber(ARGV[5]) then return 0 end

redis.call('HSET', key, 'lease_until', ARGV[3])
redis.call('ZADD', ARGV[4] .. ':leased', tonumber(ARGV[3]), ARGV[1])

return 1
"""

# the whole of writing a run: reserve the key, take an id, write the hash and put it where its state waits. in pieces, a process that dies between two of them leaves a key reserved for a run that was never written, and that occurrence can never be written again
ADD = """
local prefix = ARGV[1]

if ARGV[2] ~= '' and redis.call('SET', prefix .. ':key:' .. ARGV[2], '', 'NX') == false then return nil end

local id = tostring(redis.call('INCR', prefix .. ':sequence'))
local fields = cjson.decode(ARGV[3])

table.insert(fields, 'id')
table.insert(fields, id)

redis.call('HSET', prefix .. ':run:' .. id, unpack(fields))

-- the lane is listed whatever state the run was written in, because a claim only ever walks the priorities its queue has listed — and a run reclaimed into a lane nobody listed is one no worker ever sees again
redis.call('ZADD', prefix .. ':priorities:' .. ARGV[4], tonumber(ARGV[5]), ARGV[5])

-- and the run itself waits where its own state waits, which is the one set that state is read out of. an instant that set orders by and the run does not carry is a run none of them holds, exactly as a lease that is not there is one no reclaim takes back
if ARGV[7] ~= '' then redis.call('ZADD', ARGV[6], tonumber(ARGV[7]), id) end

if ARGV[2] ~= '' then redis.call('SET', prefix .. ':key:' .. ARGV[2], id) end

return id
"""


# a run that never started may be called off, and one already going may not
CANCEL = """
local key = ARGV[3] .. ':run:' .. ARGV[1]

if redis.call('HGET', key, 'status') ~= 'pending' then return 0 end

redis.call('ZREM', ARGV[3] .. ':queue:' .. redis.call('HGET', key, 'queue') .. ':' .. redis.call('HGET', key, 'priority'), ARGV[1])
redis.call('HSET', key, 'status', 'canceled', 'finished_at', ARGV[2])
redis.call('ZADD', ARGV[3] .. ':settled', tonumber(ARGV[2]), ARGV[1])

return 1
"""


# what a run over is still doing here is being held in memory, and a batch at a time because a script holds the whole server while it works. the range stops short of the instant itself, because what every other store drops is a run that finished strictly before it
PURGE = """
local gone = redis.call('ZRANGEBYSCORE', ARGV[2] .. ':settled', '-inf', '(' .. ARGV[1], 'LIMIT', 0, tonumber(ARGV[3]))

for _, id in ipairs(gone) do
  local key = ARGV[2] .. ':run:' .. id
  local named = redis.call('HGET', key, 'key')

  -- the reservation goes with the run it named, in every store: what is left of a run that was dropped is a key nothing can ever write again
  if named and named ~= '' then redis.call('DEL', ARGV[2] .. ':key:' .. named) end

  redis.call('DEL', key)
  redis.call('ZREM', ARGV[2] .. ':settled', id)
end

return #gone
"""


def stamp(moment: datetime | None) -> str:
    """an instant as the whole microseconds since the epoch, which is the finest any store keeps one to. a naive datetime is the utc instant it reads as, which is what every other store already means by one: `timestamp()` on its own reads it as the wall clock of whatever machine wrote it, and the same value would name two instants depending on where the runs live. what it hands back is a double as well, and a double stops being able to name a microsecond somewhere past the year 2112 — the last instant a datetime holds came back off it as the first of the year after, which is a `ValueError` raised where a claim builds the runs it has already taken. that pass ends, the runs beside it in the batch go back on their leases, and the one that raised comes round again on every lease from then on. a whole number is exact where python reads it and a score wherever lua sorts it"""
    return "" if moment is None else str((as_utc(moment) - EPOCH) // MICROSECOND)


def fields_of(pairs) -> dict:
    """every field of a hash and what it holds, as text. redis answers both as bytes, and this store reads them that way because a client told to decode for itself is one an application sharing it very often has"""
    return {name.decode(): value.decode() for name, value in pairs}


def paired(flat: list) -> dict:
    """the same, out of a script, which answers a hash with one flat array of every field followed by what it holds"""
    return fields_of(zip(flat[::2], flat[1::2]))


def moment_of(value: str) -> datetime | None:
    """counted forward from the epoch and never off the clock of the machine: `fromtimestamp` with no zone answers local wall time, and calling that utc moves every instant this store reads by the offset of wherever it happens to run"""
    return None if not value else EPOCH + timedelta(microseconds=int(value))


class RedisStore(Store):
    """runs held in redis, for whoever would rather not put a queue in their database. the claim, the reclaim and every close are lua, so each of them is one atomic step on the server"""

    def __init__(self, client: Redis, *, prefix: str = PREFIX) -> None:
        if client.connection_pool.connection_kwargs.get("decode_responses"):
            raise QueueError("the client was built with decode_responses, and this store reads what redis answers as bytes — every claim of every worker would raise while the enqueueing beside it went on working")

        self.client = client
        self.prefix = prefix
        self.scripts: dict[str, object] = {}

    async def setup(self) -> None:
        """redis builds nothing, so this only registers the scripts the store runs"""
        self.scripts = {name: self.client.register_script(body) for name, body in (("add", ADD), ("claim", CLAIM), ("reclaim", RECLAIM), ("settle", SETTLE), ("heartbeat", HEARTBEAT), ("cancel", CANCEL), ("purge", PURGE))}

    def run_key(self, run_id) -> str:
        return f"{self.prefix}:run:{run_id}"

    def to_hash(self, run: Run) -> dict:
        return {
            "id": str(run.id),
            "key": run.key or "",
            "name": run.name,
            "queue": run.queue,
            "payload": json.dumps(run.payload),
            "status": run.status.value,
            "priority": str(run.priority),
            "due_at": stamp(run.due_at),
            "attempts": str(run.attempts),
            "max_attempts": str(run.max_attempts),
            "timeout": "" if run.timeout is None else str(run.timeout),
            "retry_policy": run.retry_policy.value,
            "retry_delay": str(run.retry_delay),
            "max_retry_delay": str(run.max_retry_delay),
            "worker": run.worker or "",
            "lease_until": stamp(run.lease_until),
            "created_at": stamp(run.created_at),
            "started_at": stamp(run.started_at),
            "finished_at": stamp(run.finished_at),
            "result": "" if run.result is None else json.dumps(run.result),
            "error": run.error or "",
            "error_type": run.error_type or "",
        }

    def to_run(self, stored: dict) -> Run:
        return Run(
            id=stored["id"],
            key=stored["key"] or None,
            name=stored["name"],
            queue=stored["queue"],
            payload=json.loads(stored["payload"]),
            status=RunStatus(stored["status"]),
            priority=int(stored["priority"]),
            due_at=moment_of(stored["due_at"]),
            attempts=int(stored["attempts"]),
            max_attempts=int(stored["max_attempts"]),
            timeout=None if not stored["timeout"] else float(stored["timeout"]),
            retry_policy=RetryPolicy(stored["retry_policy"]),
            retry_delay=float(stored["retry_delay"]),
            max_retry_delay=float(stored["max_retry_delay"]),
            worker=stored["worker"] or None,
            lease_until=moment_of(stored["lease_until"]),
            created_at=moment_of(stored["created_at"]),
            started_at=moment_of(stored["started_at"]),
            finished_at=moment_of(stored["finished_at"]),
            result=None if not stored["result"] else json.loads(stored["result"]),
            error=stored["error"] or None,
            error_type=stored["error_type"] or None,
        )

    def waiting_in(self, run: Run) -> tuple[str, str]:
        """the set a run written from outside belongs in, and the instant that set orders it by. a store is handed whatever run it is given and every other one reads that state straight back, so a run written as running has to wait on the leases and one already over among what is pruned. put in the queue lane whatever it was, a run its holder was still working was claimed out from under it by the next worker to ask, and one that was already over was run again and then kept for ever, because nothing prunes what never reached the settled set"""
        if run.status == RunStatus.PENDING:
            return f"{self.prefix}:queue:{run.queue}:{run.priority}", stamp(run.due_at)

        if run.status == RunStatus.RUNNING:
            return f"{self.prefix}:leased", stamp(run.lease_until)

        return f"{self.prefix}:settled", stamp(run.finished_at)

    async def add(self, run: Run) -> Run | None:
        """one step on the server: the key is reserved and the run is written together, so nothing can die between the two and leave a key nobody can ever use again"""
        fields = [piece for name, value in self.to_hash(run).items() if name != "id" for piece in (name, value)]
        written = await self.scripts["add"](keys=[], args=[self.prefix, run.key or "", json.dumps(fields), run.queue, str(run.priority), *self.waiting_in(run)])

        if written is None:
            return None

        run.id = written.decode()

        return run

    async def claim(self, worker: str, queues: tuple[str, ...], limit: int, lease: timedelta, moment: datetime) -> list[Run]:
        """one step on the server, which is the whole of a claim: taking the runs and reading them back are the same step, so nothing between the two can leave a run claimed that this worker never came away holding"""
        taken = await self.scripts["claim"](keys=list(queues), args=[stamp(moment), worker, stamp(moment + lease), limit, limit * CLAIM_SPREAD, self.prefix])

        return [self.to_run(paired(stored)) for stored in taken]

    async def settle(self, run_id, worker: str, attempt: int, values: dict, due_at: datetime | None, counted: int) -> bool:
        """closes or requeues the run this attempt holds, with what the attempt just made counts for: nothing for one that happened, and one back for a worker that never had anything to try"""
        flat = [piece for pair in values.items() for piece in pair]

        return await self.scripts["settle"](keys=[], args=[run_id, worker, json.dumps(flat), stamp(due_at), self.prefix, counted, attempt]) == 1

    async def heartbeat(self, run_id, worker: str, attempt: int, lease: timedelta, moment: datetime) -> bool:
        return await self.scripts["heartbeat"](keys=[], args=[run_id, worker, stamp(moment + lease), self.prefix, attempt]) == 1

    async def complete(self, run_id, worker: str, attempt: int, moment: datetime, result: dict | None) -> bool:
        return await self.settle(run_id, worker, attempt, {"status": "done", "finished_at": stamp(moment), "result": "" if result is None else json.dumps(result), "error": "", "error_type": "", "worker": "", "lease_until": ""}, None, 0)

    async def fail(self, run_id, worker: str, attempt: int, moment: datetime, error: str, error_type: str) -> bool:
        return await self.settle(run_id, worker, attempt, {"status": "failed", "finished_at": stamp(moment), "error": error, "error_type": error_type, "worker": "", "lease_until": ""}, None, 0)

    def requeued(self, due_at: datetime, error: str, error_type: str) -> dict:
        return {"status": "pending", "due_at": stamp(due_at), "error": error, "error_type": error_type, "worker": "", "lease_until": "", "started_at": ""}

    async def retry_later(self, run_id, worker: str, attempt: int, due_at: datetime, error: str, error_type: str) -> bool:
        return await self.settle(run_id, worker, attempt, self.requeued(due_at, error, error_type), due_at, 0)

    async def release(self, run_id, worker: str, attempt: int, due_at: datetime, error: str, error_type: str) -> bool:
        return await self.settle(run_id, worker, attempt, self.requeued(due_at, error, error_type), due_at, -1)

    async def reclaim(self, moment: datetime) -> int:
        return await self.scripts["reclaim"](keys=[], args=[stamp(moment), self.prefix, RECLAIM_BATCH])

    async def purge(self, before: datetime, limit: int) -> int:
        return await self.scripts["purge"](keys=[], args=[stamp(before), self.prefix, limit])

    async def cancel(self, run_id, moment: datetime) -> bool:
        return await self.scripts["cancel"](keys=[], args=[run_id, stamp(moment), self.prefix]) == 1

    async def get(self, run_id) -> Run | None:
        stored = await self.client.hgetall(self.run_key(run_id))

        return self.to_run(fields_of(stored.items())) if stored else None

    async def find(self, key: str) -> Run | None:
        run_id = await self.client.get(f"{self.prefix}:key:{key}")

        return await self.get(run_id.decode()) if run_id else None

    async def count(self, status: RunStatus | None = None, queue: str | None = None) -> int:
        """redis has no index over a hash, so the depth is counted by walking what this store owns"""
        found = 0

        async for name in self.client.scan_iter(match=f"{self.prefix}:run:*"):
            held, queued = await self.client.hmget(name, "status", "queue")

            # a pruning walking beside this drops the hash while the scan still names it, and a run that is already gone is not depth
            if held is None:
                continue

            if (status is None or held.decode() == status.value) and (queue is None or queued.decode() == queue):
                found += 1

        return found
