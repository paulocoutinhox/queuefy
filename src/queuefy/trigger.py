from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timedelta

from queuefy import cron
from queuefy.clock import EPOCH, WIDEST_SECONDS, real
from queuefy.errors import CronError, QueueError


class Trigger(ABC):
    """what turns an instant into the next instant a recurring task is due"""

    @abstractmethod
    def next_after(self, moment: datetime) -> datetime:
        """the first instant after this one that the trigger names, and never the one it was asked about"""


# the finest an instant is ever kept, in every store and in the column each of them holds one in. anything under it names one slot twice and never two of them
RESOLUTION = timedelta(microseconds=1)


@dataclass(frozen=True)
class Interval(Trigger):
    """every n seconds, counted from the unix epoch so every worker of every machine names the same slot"""

    seconds: float

    def __post_init__(self) -> None:
        real(self.seconds, "an interval")

        if self.seconds <= 0:
            raise QueueError(f"an interval of {self.seconds}s never advances")

        # measured before the span is built, because a number this far out is one a timedelta will not hold either
        if self.seconds > WIDEST_SECONDS:
            raise QueueError(f"an interval of {self.seconds}s names its first slot past the last instant a datetime holds, so every pass of every worker raises on it before anything is ever claimed")

        if self.span < RESOLUTION:
            raise QueueError(f"an interval of {self.seconds}s is finer than the microsecond a store keeps an instant to, so every slot of it is the slot before it under another name")

    @property
    def span(self) -> timedelta:
        return timedelta(seconds=self.seconds)

    def next_after(self, moment: datetime) -> datetime:
        """counted in whole slots and never in seconds: a float carries the answer back through a multiplication it cannot undo, and the slot that came out of it was the instant it was asked after — a task writing the occurrence it had already written, and never the one after"""
        return EPOCH + self.span * ((moment - EPOCH) // self.span + 1)


@dataclass(frozen=True)
class Cron(Trigger):
    """a posix expression, rounded to the minute like every cron is"""

    expression: str

    def __post_init__(self) -> None:
        # an interval answers for the type it was handed and this never did, so anything but text reached the split and raised an `AttributeError` out of the parser — the one way of getting a trigger wrong that `except QueueError` does not catch
        if not isinstance(self.expression, str):
            raise CronError(f"a cron expression is {self.expression!r}, and what the five fields are read out of is text — anything else raises from inside the parser, under a name nothing here answers for")

        cron.parse(self.expression)

    def next_after(self, moment: datetime) -> datetime:
        return cron.next_after(self.expression, moment)
