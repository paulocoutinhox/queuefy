class QueueError(Exception):
    """what this library raises, so a caller can tell its failures from the ones of the code it runs"""


class UnknownTask(QueueError):
    """a name nobody registered, which is a typo or a worker running older code than whoever enqueued"""


class PermanentError(QueueError):
    """a handler saying this attempt is the last one: a malformed payload never gets better by being retried"""


class CronError(QueueError):
    """an expression that does not parse, raised where it is written instead of on the night it would first run"""


class WorkNeverStarted(QueueError):
    """a timeout that passed while the work was still queued for a thread, so no line of the handler ever ran — the run goes back with its attempt given back, because an attempt that never happened is not one the run has spent"""


class UnwritableAnswer(QueueError):
    """a handler that answered something no store could write down, which is the end of the run however many attempts were allowed — the very same handler answers the very same thing on every one of them"""
