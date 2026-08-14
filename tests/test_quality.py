from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from marketdata.models.candle import Candle
from marketdata.models.timeframe import timeframe_cadence
from marketdata.quality.gaps import (
    expected_candle_count,
    find_missing_intervals,
)
from marketdata.quality.report import (
    QualityStatus,
    build_quality_report,
    write_quality_report,
)
from marketdata.validation.candles import CandleViolation

START = datetime(2026, 8, 14, 12, 0, tzinfo=UTC)
END = datetime(2026, 8, 14, 13, 0, tzinfo=UTC)
MINUTE = timedelta(minutes=1)


def make_candle(timestamp: datetime) -> Candle:
    return Candle(
        timestamp=timestamp,
        symbol="EUR/USD",
        open=Decimal("1.1700"),
        high=Decimal("1.1710"),
        low=Decimal("1.1690"),
        close=Decimal("1.1705"),
        volume=Decimal(100),
    )


def build_report(candles, **overrides):
    kwargs = {
        "provider": "fake",
        "symbol": "EUR/USD",
        "timeframe": "1min",
        "requested_start": START,
        "requested_end": END,
        "candles": candles,
        "downloaded_rows": len(candles),
        "duplicates_removed": 0,
        "out_of_range_rows": 0,
        "violations": [],
    }
    kwargs.update(overrides)
    return build_quality_report(**kwargs)


def test_timeframe_cadence_known_and_unknown():
    assert timeframe_cadence("1min") == MINUTE
    assert timeframe_cadence("1hour") == timedelta(hours=1)
    assert timeframe_cadence("1day_eet") is None
    assert timeframe_cadence("unknown") is None


def test_expected_candle_count():
    assert expected_candle_count(START, END, MINUTE) == 60
    assert expected_candle_count(START, START, MINUTE) == 0
    assert expected_candle_count(END, START, MINUTE) == 0


def test_no_gaps_for_contiguous_series():
    timestamps = [START + MINUTE * index for index in range(60)]

    assert find_missing_intervals(timestamps, MINUTE, start=START, end=END) == []


def test_interior_gap_is_detected():
    timestamps = [START, START + MINUTE, START + MINUTE * 5]

    intervals = find_missing_intervals(timestamps, MINUTE)

    assert len(intervals) == 1
    assert intervals[0].start == START + MINUTE * 2
    assert intervals[0].end == START + MINUTE * 5
    assert intervals[0].missing_candles == 3


def test_leading_and_trailing_gaps_are_detected():
    timestamps = [START + MINUTE * 2, START + MINUTE * 3]

    intervals = find_missing_intervals(timestamps, MINUTE, start=START, end=END)

    assert [interval.missing_candles for interval in intervals] == [2, 56]
    assert intervals[0].start == START
    assert intervals[-1].end == END


def test_empty_series_reports_whole_range_missing():
    intervals = find_missing_intervals([], MINUTE, start=START, end=END)

    assert len(intervals) == 1
    assert intervals[0].missing_candles == 60


def test_non_positive_cadence_is_rejected():
    with pytest.raises(ValueError, match="cadence must be positive"):
        find_missing_intervals([START], timedelta(0))


def test_report_is_ok_for_complete_dataset():
    candles = [make_candle(START + MINUTE * index) for index in range(60)]

    report = build_report(candles)

    assert report.status is QualityStatus.OK
    assert report.final_rows == 60
    assert report.expected_rows == 60
    assert report.missing_candles == 0
    assert report.actual_start == START
    assert report.actual_end == END - MINUTE
    assert report.cadence_seconds == 60


def test_report_flags_incomplete_dataset():
    candles = [make_candle(START + MINUTE * index) for index in range(10)]

    report = build_report(candles)

    assert report.status is QualityStatus.INCOMPLETE
    assert report.missing_candles == 50


def test_report_flags_invalid_rows():
    candles = [make_candle(START + MINUTE * index) for index in range(60)]
    violation = CandleViolation(
        timestamp=START,
        symbol="EUR/USD",
        reason="high cannot be below low",
    )

    report = build_report(candles, violations=[violation], downloaded_rows=61)

    assert report.status is QualityStatus.INVALID
    assert report.invalid_rows == 1
    assert report.violations[0].reason == "high cannot be below low"
    assert report.violations_truncated is False


def test_report_flags_empty_dataset():
    report = build_report([])

    assert report.status is QualityStatus.EMPTY
    assert report.actual_start is None
    assert report.actual_end is None


def test_report_skips_gap_detection_for_variable_cadence():
    candles = [make_candle(START)]

    report = build_report(candles, timeframe="1day_eet")

    assert report.cadence_seconds is None
    assert report.expected_rows is None
    assert report.missing_intervals == []
    assert report.status is QualityStatus.OK


def test_report_truncates_violation_samples():
    violations = [
        CandleViolation(
            timestamp=START + MINUTE * index,
            symbol="EUR/USD",
            reason="high cannot be below low",
        )
        for index in range(25)
    ]

    report = build_report([], violations=violations)

    assert report.invalid_rows == 25
    assert len(report.violations) == 20
    assert report.violations_truncated is True


def test_report_is_machine_readable(tmp_path):
    import json

    candles = [make_candle(START + MINUTE * index) for index in range(3)]

    report = build_report(candles)
    path = write_quality_report(report, tmp_path / "report.json")

    payload = json.loads(path.read_text())

    assert payload["symbol"] == "EUR/USD"
    assert payload["status"] == "incomplete"
    assert payload["downloaded_rows"] == 3
    assert payload["missing_intervals"][0]["missing_candles"] == 57
