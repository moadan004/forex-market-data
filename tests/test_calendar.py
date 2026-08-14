from datetime import UTC, date, datetime, time, timedelta

import pytest

from marketdata.calendar import (
    AlwaysOpenCalendar,
    ForexCalendar,
    MarketCalendar,
    get_calendar,
)
from marketdata.calendar.base import merge_intervals

MINUTE = timedelta(minutes=1)
HOUR = timedelta(hours=1)

# 2026-08-14 is a Friday, so this week runs Monday 10th to Sunday 16th.
MONDAY = datetime(2026, 8, 10, 0, 0, tzinfo=UTC)
FRIDAY_NOON = datetime(2026, 8, 14, 12, 0, tzinfo=UTC)
FRIDAY_CLOSE = datetime(2026, 8, 14, 21, 0, tzinfo=UTC)
SATURDAY = datetime(2026, 8, 15, 0, 0, tzinfo=UTC)
SUNDAY = datetime(2026, 8, 16, 0, 0, tzinfo=UTC)
SUNDAY_OPEN = datetime(2026, 8, 16, 22, 0, tzinfo=UTC)
NEXT_MONDAY = datetime(2026, 8, 17, 0, 0, tzinfo=UTC)


@pytest.fixture
def calendar() -> ForexCalendar:
    return ForexCalendar()


def test_calendar_is_registered_by_name():
    assert isinstance(get_calendar("forex"), ForexCalendar)
    assert isinstance(get_calendar("24x7"), AlwaysOpenCalendar)


def test_unknown_calendar_is_rejected():
    with pytest.raises(ValueError, match="Unknown calendar"):
        get_calendar("nyse")


def test_abstract_calendar_cannot_be_instantiated():
    with pytest.raises(TypeError):
        MarketCalendar()


def test_weekday_trading_is_open(calendar):
    assert calendar.is_open(MONDAY)
    assert calendar.is_open(FRIDAY_NOON)
    assert calendar.is_open(FRIDAY_CLOSE - MINUTE)
    assert calendar.is_open(NEXT_MONDAY)


def test_saturday_is_closed_all_day(calendar):
    moment = SATURDAY

    while moment < SUNDAY:
        assert not calendar.is_open(moment), moment
        moment += HOUR


def test_sunday_is_closed_until_the_weekly_open(calendar):
    moment = SUNDAY

    while moment < SUNDAY_OPEN:
        assert not calendar.is_open(moment), moment
        moment += HOUR

    assert calendar.is_open(SUNDAY_OPEN)


def test_weekly_closure_boundaries(calendar):
    assert calendar.is_open(FRIDAY_CLOSE - MINUTE)
    assert not calendar.is_open(FRIDAY_CLOSE)
    assert not calendar.is_open(SUNDAY_OPEN - MINUTE)
    assert calendar.is_open(SUNDAY_OPEN)


def test_closed_intervals_cover_the_whole_weekend(calendar):
    intervals = calendar.closed_intervals(MONDAY, NEXT_MONDAY)

    assert len(intervals) == 1
    assert intervals[0].start == FRIDAY_CLOSE
    assert intervals[0].end == SUNDAY_OPEN
    assert intervals[0].reason == "weekend"


def test_closed_intervals_are_clipped_to_the_query_window(calendar):
    intervals = calendar.closed_intervals(SATURDAY, SUNDAY)

    assert len(intervals) == 1
    assert intervals[0].start == SATURDAY
    assert intervals[0].end == SUNDAY


def test_closure_spanning_the_window_start_is_not_missed(calendar):
    """A window opening mid-weekend still sees the closure it sits inside."""
    intervals = calendar.closed_intervals(SUNDAY, SUNDAY + HOUR)

    assert len(intervals) == 1
    assert intervals[0].start == SUNDAY
    assert intervals[0].end == SUNDAY + HOUR


def test_multiple_weekends_are_reported(calendar):
    intervals = calendar.closed_intervals(MONDAY, MONDAY + timedelta(days=21))

    assert len(intervals) == 3
    assert [interval.start.date() for interval in intervals] == [
        date(2026, 8, 14),
        date(2026, 8, 21),
        date(2026, 8, 28),
    ]


def test_open_intervals_split_around_the_weekend(calendar):
    intervals = calendar.open_intervals(MONDAY, NEXT_MONDAY)

    assert intervals == [
        (MONDAY, FRIDAY_CLOSE),
        (SUNDAY_OPEN, NEXT_MONDAY),
    ]


def test_open_intervals_of_a_fully_closed_window(calendar):
    assert calendar.open_intervals(SATURDAY, SUNDAY) == []


def test_open_intervals_of_an_empty_window(calendar):
    assert calendar.open_intervals(MONDAY, MONDAY) == []


def test_expected_candle_count_excludes_the_weekend(calendar):
    minutes_open = (FRIDAY_CLOSE - MONDAY) + (NEXT_MONDAY - SUNDAY_OPEN)

    assert calendar.expected_candle_count(MONDAY, NEXT_MONDAY, MINUTE) == (
        minutes_open // MINUTE
    )


def test_expected_candle_count_of_a_trading_hour(calendar):
    assert calendar.expected_candle_count(FRIDAY_NOON, FRIDAY_NOON + HOUR, MINUTE) == 60


def test_expected_candle_count_is_zero_over_a_closure(calendar):
    assert calendar.expected_candle_count(SATURDAY, SUNDAY, MINUTE) == 0


def test_expected_candle_count_is_anchored_to_the_epoch(calendar):
    """An unaligned window counts the slots that fall inside it, not a ratio."""
    start = FRIDAY_NOON + timedelta(seconds=30)

    assert calendar.expected_candle_count(start, start + HOUR, MINUTE) == 60
    assert calendar.expected_candle_count(start, FRIDAY_NOON + HOUR, MINUTE) == 59


def test_expected_candle_count_rejects_a_zero_cadence(calendar):
    with pytest.raises(ValueError, match="cadence must be positive"):
        calendar.expected_candle_count(MONDAY, NEXT_MONDAY, timedelta(0))


def test_holidays_close_the_whole_utc_day():
    calendar = ForexCalendar(holidays=[date(2026, 8, 12)])

    assert calendar.is_open(datetime(2026, 8, 11, 23, 59, tzinfo=UTC))
    assert not calendar.is_open(datetime(2026, 8, 12, 0, 0, tzinfo=UTC))
    assert not calendar.is_open(datetime(2026, 8, 12, 23, 59, tzinfo=UTC))
    assert calendar.is_open(datetime(2026, 8, 13, 0, 0, tzinfo=UTC))


def test_holiday_inside_the_weekend_is_absorbed():
    calendar = ForexCalendar(holidays=[date(2026, 8, 15)])

    intervals = calendar.closed_intervals(MONDAY, NEXT_MONDAY)

    assert len(intervals) == 1
    assert intervals[0].start == FRIDAY_CLOSE
    assert intervals[0].end == SUNDAY_OPEN


def test_holiday_overlapping_the_weekly_open_extends_the_closure():
    """The Sunday 22:00 open falls inside a Sunday holiday."""
    calendar = ForexCalendar(holidays=[date(2026, 8, 16)])

    intervals = calendar.closed_intervals(MONDAY, NEXT_MONDAY)

    assert len(intervals) == 1
    assert intervals[0].start == FRIDAY_CLOSE
    assert intervals[0].end == NEXT_MONDAY
    assert intervals[0].reason == "market closure"


def test_holiday_after_the_weekly_open_stays_a_separate_closure():
    """Sunday 22:00 to Monday 00:00 is tradeable, so the closures differ."""
    calendar = ForexCalendar(holidays=[date(2026, 8, 17)])

    intervals = calendar.closed_intervals(MONDAY, MONDAY + timedelta(days=8))

    assert [(interval.start, interval.end) for interval in intervals] == [
        (FRIDAY_CLOSE, SUNDAY_OPEN),
        (NEXT_MONDAY, datetime(2026, 8, 18, 0, 0, tzinfo=UTC)),
    ]
    assert calendar.open_intervals(SUNDAY_OPEN, NEXT_MONDAY) == [
        (SUNDAY_OPEN, NEXT_MONDAY)
    ]


def test_holidays_reduce_the_expected_candle_count():
    plain = ForexCalendar()
    with_holiday = ForexCalendar(holidays=[date(2026, 8, 12)])

    difference = plain.expected_candle_count(
        MONDAY, NEXT_MONDAY, MINUTE
    ) - with_holiday.expected_candle_count(MONDAY, NEXT_MONDAY, MINUTE)

    assert difference == 24 * 60


def test_session_boundaries_are_configurable():
    calendar = ForexCalendar(
        close_time=time(22, 0),
        open_time=time(21, 0),
    )

    assert calendar.is_open(datetime(2026, 8, 14, 21, 30, tzinfo=UTC))
    assert not calendar.is_open(datetime(2026, 8, 14, 22, 30, tzinfo=UTC))
    assert calendar.is_open(datetime(2026, 8, 16, 21, 30, tzinfo=UTC))


def test_invalid_weekday_is_rejected():
    with pytest.raises(ValueError, match="between 0"):
        ForexCalendar(close_weekday=7)


def test_naive_datetimes_are_rejected(calendar):
    with pytest.raises(ValueError, match="timezone-aware"):
        calendar.is_open(datetime(2026, 8, 14, 12, 0))  # noqa: DTZ001

    with pytest.raises(ValueError, match="timezone-aware"):
        calendar.closed_intervals(
            datetime(2026, 8, 14, 12, 0),  # noqa: DTZ001
            NEXT_MONDAY,
        )


def test_always_open_calendar_never_closes():
    calendar = AlwaysOpenCalendar()

    assert calendar.name == "24x7"
    assert calendar.is_open(SATURDAY)
    assert calendar.closed_intervals(MONDAY, NEXT_MONDAY) == []
    assert calendar.open_intervals(MONDAY, NEXT_MONDAY) == [(MONDAY, NEXT_MONDAY)]
    assert calendar.expected_candle_count(SATURDAY, SUNDAY, MINUTE) == 24 * 60


def test_merge_intervals_orders_and_collapses():
    from marketdata.calendar.base import ClosedInterval

    merged = merge_intervals(
        [
            ClosedInterval(start=SUNDAY, end=SUNDAY_OPEN, reason="weekend"),
            ClosedInterval(start=FRIDAY_CLOSE, end=SATURDAY, reason="weekend"),
            ClosedInterval(start=SATURDAY, end=SUNDAY, reason="weekend"),
            ClosedInterval(start=MONDAY, end=MONDAY, reason="empty"),
        ]
    )

    assert len(merged) == 1
    assert merged[0].start == FRIDAY_CLOSE
    assert merged[0].end == SUNDAY_OPEN
