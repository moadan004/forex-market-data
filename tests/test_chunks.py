from datetime import UTC, datetime, timedelta
from itertools import pairwise

import pytest

from marketdata.downloader.chunks import (
    DurationChunkSize,
    MonthlyChunkSize,
    parse_chunk_size,
    plan_chunks,
)

MONTH = MonthlyChunkSize()


def utc(*args) -> datetime:
    return datetime(*args, tzinfo=UTC)


def assert_covers_range(chunks, start, end):
    """Chunks must tile the requested range exactly."""
    assert chunks[0].start == start
    assert chunks[-1].end == end

    for previous, current in pairwise(chunks):
        assert previous.end == current.start

    assert [chunk.index for chunk in chunks] == list(range(len(chunks)))
    assert all(chunk.start < chunk.end for chunk in chunks)
    assert sum((chunk.duration for chunk in chunks), timedelta(0)) == end - start


def test_parse_month_sizes():
    assert parse_chunk_size("1month") == MonthlyChunkSize(months=1)
    assert parse_chunk_size("3months") == MonthlyChunkSize(months=3)
    assert parse_chunk_size(" 6 MONTHS ") == MonthlyChunkSize(months=6)


def test_parse_duration_sizes():
    assert parse_chunk_size("7d") == DurationChunkSize(amount=7, unit="d")
    assert parse_chunk_size("12h") == DurationChunkSize(amount=12, unit="h")
    assert parse_chunk_size("30min") == DurationChunkSize(amount=30, unit="min")
    assert parse_chunk_size("2w") == DurationChunkSize(amount=2, unit="w")


def test_chunk_size_labels_round_trip():
    for text in ("1month", "3months", "7d", "12h", "30min", "2w"):
        assert parse_chunk_size(text).label == text


@pytest.mark.parametrize("text", ["", "month", "7", "7y", "-1d", "1.5d", "d7"])
def test_invalid_chunk_sizes_are_rejected(text):
    with pytest.raises(ValueError, match="Invalid chunk size"):
        parse_chunk_size(text)


def test_zero_months_is_rejected():
    with pytest.raises(ValueError, match="at least 1"):
        MonthlyChunkSize(months=0)


def test_zero_duration_is_rejected():
    with pytest.raises(ValueError, match="at least 1"):
        DurationChunkSize(amount=0, unit="d")


def test_unknown_duration_unit_is_rejected():
    with pytest.raises(ValueError, match="Unknown chunk unit"):
        DurationChunkSize(amount=1, unit="fortnight")


def test_monthly_chunks_break_on_month_starts():
    start = utc(2019, 1, 1)
    end = utc(2019, 4, 1)

    chunks = plan_chunks(start, end, MONTH)

    assert [(chunk.start, chunk.end) for chunk in chunks] == [
        (utc(2019, 1, 1), utc(2019, 2, 1)),
        (utc(2019, 2, 1), utc(2019, 3, 1)),
        (utc(2019, 3, 1), utc(2019, 4, 1)),
    ]
    assert_covers_range(chunks, start, end)


def test_monthly_chunks_cross_a_year_boundary():
    start = utc(2019, 11, 15)
    end = utc(2020, 2, 10)

    chunks = plan_chunks(start, end, MONTH)

    assert [(chunk.start, chunk.end) for chunk in chunks] == [
        (utc(2019, 11, 15), utc(2019, 12, 1)),
        (utc(2019, 12, 1), utc(2020, 1, 1)),
        (utc(2020, 1, 1), utc(2020, 2, 1)),
        (utc(2020, 2, 1), utc(2020, 2, 10)),
    ]
    assert_covers_range(chunks, start, end)


def test_multi_month_chunks_align_from_january():
    start = utc(2019, 2, 15)
    end = utc(2019, 11, 1)

    chunks = plan_chunks(start, end, MonthlyChunkSize(months=3))

    assert [(chunk.start, chunk.end) for chunk in chunks] == [
        (utc(2019, 2, 15), utc(2019, 4, 1)),
        (utc(2019, 4, 1), utc(2019, 7, 1)),
        (utc(2019, 7, 1), utc(2019, 10, 1)),
        (utc(2019, 10, 1), utc(2019, 11, 1)),
    ]
    assert_covers_range(chunks, start, end)


def test_duration_chunks_are_anchored_to_the_epoch():
    start = utc(2019, 1, 1, 5, 0)
    end = utc(2019, 1, 3, 5, 0)

    chunks = plan_chunks(start, end, DurationChunkSize(amount=1, unit="d"))

    assert [(chunk.start, chunk.end) for chunk in chunks] == [
        (utc(2019, 1, 1, 5, 0), utc(2019, 1, 2)),
        (utc(2019, 1, 2), utc(2019, 1, 3)),
        (utc(2019, 1, 3), utc(2019, 1, 3, 5, 0)),
    ]
    assert_covers_range(chunks, start, end)


def test_hourly_chunks():
    start = utc(2026, 8, 14, 12, 30)
    end = utc(2026, 8, 14, 15, 0)

    chunks = plan_chunks(start, end, DurationChunkSize(amount=1, unit="h"))

    assert len(chunks) == 3
    assert_covers_range(chunks, start, end)


def test_range_shorter_than_a_chunk_is_a_single_chunk():
    start = utc(2026, 8, 14, 12, 0)
    end = utc(2026, 8, 14, 13, 0)

    chunks = plan_chunks(start, end, MONTH)

    assert len(chunks) == 1
    assert chunks[0].start == start
    assert chunks[0].end == end


def test_range_ending_exactly_on_a_boundary():
    start = utc(2019, 1, 15)
    end = utc(2019, 2, 1)

    chunks = plan_chunks(start, end, MONTH)

    assert len(chunks) == 1
    assert_covers_range(chunks, start, end)


def test_seven_years_of_monthly_chunks():
    start = utc(2019, 1, 1)
    end = utc(2026, 1, 1)

    chunks = plan_chunks(start, end, MONTH)

    assert len(chunks) == 7 * 12
    assert_covers_range(chunks, start, end)


def test_planning_is_deterministic():
    start = utc(2019, 3, 7, 4, 5)
    end = utc(2020, 8, 21, 6, 7)

    first = plan_chunks(start, end, MONTH)
    second = plan_chunks(start, end, MONTH)

    assert first == second


def test_boundaries_do_not_depend_on_the_requested_start():
    """Two requests ending together share every boundary they both span."""
    end = utc(2019, 6, 1)
    late_start = utc(2019, 3, 20)

    early = plan_chunks(utc(2019, 1, 10), end, MONTH)
    late = plan_chunks(late_start, end, MONTH)

    interior = {chunk.start for chunk in late} - {late_start}

    assert interior == {utc(2019, 4, 1), utc(2019, 5, 1)}
    assert interior <= {chunk.start for chunk in early}
    assert early[-1].start == late[-1].start


def test_naive_datetimes_are_rejected():
    with pytest.raises(ValueError, match="timezone-aware"):
        plan_chunks(
            datetime(2019, 1, 1),  # noqa: DTZ001
            utc(2019, 2, 1),
            MONTH,
        )


def test_inverted_range_is_rejected():
    with pytest.raises(ValueError, match="start must be before end"):
        plan_chunks(utc(2019, 2, 1), utc(2019, 1, 1), MONTH)


def test_empty_range_is_rejected():
    with pytest.raises(ValueError, match="start must be before end"):
        plan_chunks(utc(2019, 1, 1), utc(2019, 1, 1), MONTH)
