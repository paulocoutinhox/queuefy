from datetime import datetime, timedelta, timezone
from math import isfinite

from queuefy.errors import QueueError

EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)

# the last instant a datetime holds, which a slot key and the span of an interval are both measured against
WIDEST_SLOT = datetime(9999, 12, 31, 23, 59, 59, 999999, tzinfo=timezone.utc)

# and the first one, which is what a span measured backwards from now is bounded by
EARLIEST_SLOT = datetime(1, 1, 1, tzinfo=timezone.utc)

# the widest span anything here may be given, counted in whole seconds because that is the unit every span is given in — and because `total_seconds()` rounds the last microsecond of the range back up over it
WIDEST_SECONDS = (WIDEST_SLOT - EPOCH) // timedelta(seconds=1)


def now() -> datetime:
    """every instant this library writes down is utc, so two machines in two zones agree on when a run is due"""
    return datetime.now(timezone.utc)


def real(value: float, what: str) -> None:
    """what a number has to be for a store to write it down and for an instant to be worked out from it. the type is asked for and not the interface, for the very reason a count asks for one: redis keeps a span as text and reads it back with `float`, so a boolean is written down as 'True' and a fraction as '1/3', and neither of them reads back — what raises on it is never the run but every claim that meets it, on every poll of every worker from then on, while memory hands the handler the object it was given and a `Double` column takes the number nearest to it. `nan` is false against every comparison there is and `infinity` is past every one of them, so both pass every guard written as a comparison and reach the arithmetic instead — where `timedelta` refuses them while the ending of a run is being written, and an ending that never reaches the store is a run left claimed and the very same handler run again on every lease after that"""
    if type(value) not in (int, float):
        raise QueueError(f"{what} is {value!r}, and a store keeps a span as text it reads back with `float` — a boolean is written down as 'True', which memory works with and every claim that meets it raises on instead of taking anything")

    if not isfinite(value):
        raise QueueError(f"{what} is {value}, and what it has to be is a real number — `nan` is false against every comparison there is and `infinity` is past every one of them, so neither is refused by any of them and both raise where the ending of a run is being written")


def spanned(value, what: str) -> float:
    """the seconds of a span, which is how every span this library is handed arrives. the type is asked for because nothing below it ever could: a plain number written where a `timedelta` belongs reaches `total_seconds()` instead of a guard, and what comes out is an `AttributeError` about a method — raised where the worker is being built, under a name `except QueueError` never catches, and saying nothing at all about the sixty that was meant to be a minute"""
    if not isinstance(value, timedelta):
        raise QueueError(f"{what} is {value!r}, and a span is a timedelta — a plain number handed where one belongs raises from inside the worker being built, under a name nothing here answers for")

    return value.total_seconds()


def waited(seconds: float, what: str) -> None:
    """what a span has to be for the instant it names to exist. every span measured here is added to `now()` — a retry wait to the moment the attempt failed at, a lease to the moment the claim was made at — so what bounds one is what is left of the range and never the whole of it that an interval, counted from the epoch, is measured against. the two differ by every second since the epoch, and a span in that gap is accepted where it is written and raises where it is taken: a wait raises while the ending of a run is being written, which leaves the run claimed and runs the very same handler again on every lease after that, and a lease raises inside every claim, which is a worker that polls for ever and takes nothing"""
    real(seconds, what)

    widest = (WIDEST_SLOT - now()) // timedelta(seconds=1)

    if seconds > widest:
        raise QueueError(f"{what} is {seconds}s, and what is left between now and the last instant a datetime holds is {widest}s — so what it names is an instant no datetime holds, and the write that would have said so is one nothing ever takes")


def kept(seconds: float, what: str) -> None:
    """what a span has to be for the instant behind it to exist. `waited` bounds a span added to `now()` and this bounds one taken off it, which is the only other direction anything here measures in: a pruning asks for everything settled before the span a run is kept for, so one past the range names an instant no datetime holds. left where it is written it is accepted, and then raises on the hour inside the pruning, every hour, for as long as the process lives — a queue that grows for ever while nothing it holds is ever dropped"""
    real(seconds, what)

    widest = (now() - EARLIEST_SLOT) // timedelta(seconds=1)

    if seconds > widest:
        raise QueueError(f"{what} is {seconds}s, and what lies between the first instant a datetime holds and now is {widest}s — so what it names is an instant no datetime holds, and the pruning it is asked for raises on the hour instead of dropping anything")


def as_utc(value: datetime | None) -> datetime | None:
    """a naive value is read as utc, because that is the only thing a column without an offset can mean"""
    if value is None:
        return None

    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)

    return value.astimezone(timezone.utc)


def naive_utc(value: datetime | None) -> datetime | None:
    """mysql holds no offset, so what reaches a driver is always the naive utc instant"""
    converted = as_utc(value)

    return None if converted is None else converted.replace(tzinfo=None)
