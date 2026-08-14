from datetime import datetime, timedelta

from queuefy.errors import CronError

# minute, hour, day of month, month, day of week
FIELD_RANGES = ((0, 59), (0, 23), (1, 31), (1, 12), (0, 7))

# the most days each month ever has, february counted as 29 because a leap year is one of the years an expression lives through
MONTH_LENGTHS = (31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31)

# forty-one years of days: what is furthest away is the leap day falling on one named weekday, which a weekday field written as a star always asks for because such a field always holds sunday — '*/7' names sunday alone, so '0 0 29 2 */7' is february the 29th on a sunday. the century that is not a leap year stretches that gap to forty years, 2088 to 2128, where the first of a month on a named weekday is twelve and the leap day on any weekday is eight
HORIZON = 41 * 366


def number_of(value: str) -> int:
    try:
        return int(value)
    except ValueError as error:
        raise CronError(f"'{value}' is not a number") from error


def values_of(token: str, low: int, high: int) -> set[int]:
    base, sliced, step_spec = token.partition("/")
    step = number_of(step_spec) if sliced else 1

    if step <= 0:
        raise CronError(f"'{token}' steps by {step}, which never advances")

    if base == "*":
        start, end = low, high
    elif "-" in base:
        start_spec, _, end_spec = base.partition("-")
        start, end = number_of(start_spec), number_of(end_spec)
    else:
        start = end = number_of(base)

        if sliced:
            raise CronError(f"'{token}' steps over the single value {base}, and a step is what walks a range — read as it is written the field names {base} and nothing else, which is a schedule that is wrong and that nothing anywhere reports. '{base}-{high}/{step}' is what says the rest of it")

    if start < low or end > high or start > end:
        raise CronError(f"'{token}' falls outside [{low}, {high}]")

    return set(range(start, end + 1, step))


def field_of(spec: str, low: int, high: int) -> set[int]:
    values: set[int] = set()

    for token in spec.split(","):
        values.update(values_of(token, low, high))

    return values


def starred(spec: str) -> bool:
    """whether a day field was written as a star, which is what posix joins the two of them by — and never whether the values it named happen to cover the range. '1-31' names every day there is and is still a restriction, so an expression naming a weekday beside it matches either of the two, while '*/2' names half of them and is still a star, so one naming a weekday beside it matches both. read off the parsed set instead, each of those fired on days no other cron fires on, and a schedule that is wrong is one nothing anywhere reports"""
    return spec.startswith("*")


def reachable(days: set[int], months: set[int], weekday_starred: bool) -> bool:
    """posix joins the two day fields with an or when neither is a star, so a day none of the named months ever has is only fatal while the weekday field is one"""
    if not weekday_starred:
        return True

    return any(day <= MONTH_LENGTHS[month - 1] for month in months for day in days)


def parse(expression: str) -> tuple[tuple[set[int], ...], bool, bool]:
    """the five fields of a posix expression, each read into the set of values it stands for, and whether each of the two day fields was written as a star"""
    fields = expression.split()

    if len(fields) != 5:
        raise CronError(f"a cron expression has 5 fields and '{expression}' has {len(fields)}")

    parsed = [field_of(field, low, high) for field, (low, high) in zip(fields, FIELD_RANGES)]

    # posix writes sunday as 0 or 7, and the matcher only speaks 0
    if 7 in parsed[4]:
        parsed[4] = (parsed[4] - {7}) | {0}

    # refused where it is written: nothing else here can tell that february has no thirtieth, and the search would walk a year of minutes to find that out on every pass for as long as the process lives
    if not reachable(parsed[2], parsed[3], starred(fields[4])):
        raise CronError(f"'{expression}' asks for a day that none of the months it names ever has")

    return tuple(parsed), starred(fields[2]), starred(fields[4])


def on_day(moment: datetime, days, months, weekdays, day_starred: bool, weekday_starred: bool) -> bool:
    """posix joins day of month and day of week with an or when neither was written as a star, and with an and when either was"""
    if moment.month not in months:
        return False

    day_match = moment.day in days
    weekday_match = (moment.weekday() + 1) % 7 in weekdays

    if day_starred or weekday_starred:
        return day_match and weekday_match

    return day_match or weekday_match


def time_on(moment: datetime, minutes, hours) -> datetime | None:
    """the first minute of this day the expression matches that is not before `moment`, and nothing once the day is past all of them"""
    for hour in sorted(hours):
        if hour < moment.hour:
            continue

        for minute in sorted(minutes):
            if hour > moment.hour or minute >= moment.minute:
                return moment.replace(hour=hour, minute=minute)

    return None


def next_after(expression: str, moment: datetime) -> datetime:
    """the first minute strictly after `moment` that the expression matches. a day that cannot match is skipped whole, because walking it minute by minute is half a million steps to answer what one comparison answers — and a leap day, which is forty years out at worst, is unreachable at any cost per step"""
    fields, day_starred, weekday_starred = parse(expression)
    minutes, hours, days, months, weekdays = fields

    candidate = (moment + timedelta(minutes=1)).replace(second=0, microsecond=0)

    for _ in range(HORIZON):
        if on_day(candidate, days, months, weekdays, day_starred, weekday_starred):
            found = time_on(candidate, minutes, hours)

            if found is not None:
                return found

        candidate = (candidate + timedelta(days=1)).replace(hour=0, minute=0)

    raise CronError(f"'{expression}' matches nothing in the {HORIZON} days after {moment}")
