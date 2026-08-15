"""The offline CSV provider, against the MarketDataProvider contract."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from marketdata.models.candle import Candle
from marketdata.providers.base import MarketDataProvider
from marketdata.providers.csv import (
    REQUIRED_COLUMNS,
    CsvMarketDataProvider,
    CsvProviderError,
)
from marketdata.providers.errors import PermanentProviderError, ProviderError

FIXTURES = Path(__file__).parent / "fixtures" / "csv"

# The fixtures start on a Monday, so the forex calendar is open throughout.
START = datetime(2026, 8, 10, 0, 0, tzinfo=UTC)
MINUTE = timedelta(minutes=1)
HOUR = timedelta(hours=1)


def provider(name: str = "dataset") -> CsvMarketDataProvider:
    return CsvMarketDataProvider(FIXTURES / name)


def fetch(source, symbol="EUR/USD", start=START, end=None, **kwargs):
    return CsvMarketDataProvider(source).fetch_candles(
        symbol,
        start,
        end or START + HOUR,
        **kwargs,
    )


# --------------------------------------------------------------------------
# Contract
# --------------------------------------------------------------------------


def test_it_implements_the_provider_interface():
    assert issubclass(CsvMarketDataProvider, MarketDataProvider)
    assert isinstance(provider(), MarketDataProvider)


def test_it_names_itself():
    assert provider().name == "csv"
    assert CsvMarketDataProvider(FIXTURES, name="fixtures").name == "fixtures"


def test_it_returns_canonical_candles():
    candles = fetch(FIXTURES / "dataset")

    assert candles
    assert all(isinstance(candle, Candle) for candle in candles)
    assert candles[0].symbol == "EUR/USD"
    assert candles[0].open == Decimal("1.17000")
    assert candles[0].high == Decimal("1.17020")
    assert candles[0].low == Decimal("1.16990")
    assert candles[0].close == Decimal("1.17005")
    assert candles[0].volume == Decimal(100)


def test_health_check_follows_the_source():
    assert provider().health_check() is True
    assert CsvMarketDataProvider(FIXTURES / "no_volume.csv").health_check() is True
    assert CsvMarketDataProvider(FIXTURES / "nowhere.csv").health_check() is False


def test_errors_are_permanent_provider_errors():
    """Nothing about a bad file improves on a retry."""
    assert issubclass(CsvProviderError, PermanentProviderError)
    assert issubclass(CsvProviderError, ProviderError)
    assert CsvProviderError("x").retryable is False


# --------------------------------------------------------------------------
# Source resolution
# --------------------------------------------------------------------------


def test_a_directory_is_searched_by_symbol_and_timeframe():
    candles = fetch(FIXTURES / "dataset", "GBP/USD")

    assert len(candles) == 30
    assert candles[0].symbol == "GBP/USD"


def test_a_single_file_serves_the_symbol_it_is_asked_for():
    candles = fetch(FIXTURES / "no_volume.csv")

    assert len(candles) == 2
    assert candles[0].symbol == "EUR/USD"


def test_supported_symbols_come_from_the_directory():
    assert provider().get_supported_symbols() == ["EUR/USD", "GBP/USD"]


def test_supported_symbols_come_from_a_symbol_column():
    source = CsvMarketDataProvider(FIXTURES / "multi_symbol_1min.csv")

    assert source.get_supported_symbols() == ["EUR/USD", "GBP/USD"]


def test_a_file_without_a_symbol_column_advertises_nothing():
    source = CsvMarketDataProvider(FIXTURES / "no_volume.csv")

    assert source.get_supported_symbols() == []


def test_a_missing_symbol_says_where_it_looked():
    with pytest.raises(CsvProviderError, match="Looked for"):
        fetch(FIXTURES / "dataset", "USD/JPY")


def test_a_missing_source_is_rejected():
    with pytest.raises(CsvProviderError, match="does not exist"):
        fetch(FIXTURES / "nowhere")

    with pytest.raises(CsvProviderError, match="does not exist"):
        CsvMarketDataProvider(FIXTURES / "nowhere").get_supported_symbols()


# --------------------------------------------------------------------------
# Range handling
# --------------------------------------------------------------------------


def test_only_the_requested_range_is_returned():
    candles = fetch(
        FIXTURES / "dataset",
        start=START + MINUTE * 10,
        end=START + MINUTE * 20,
    )

    assert len(candles) == 10
    assert candles[0].timestamp == START + MINUTE * 10
    assert candles[-1].timestamp == START + MINUTE * 19


def test_the_range_is_half_open():
    candles = fetch(FIXTURES / "dataset", start=START, end=START + MINUTE)

    assert [candle.timestamp for candle in candles] == [START]


def test_rows_outside_the_range_are_not_returned():
    """The messy fixture holds rows on both sides of the window."""
    candles = fetch(FIXTURES / "messy_1min.csv", end=START + MINUTE * 10)

    assert all(START <= candle.timestamp < START + MINUTE * 10 for candle in candles)
    assert START - MINUTE not in [candle.timestamp for candle in candles]


def test_a_range_with_no_rows_returns_nothing():
    assert (
        fetch(FIXTURES / "dataset", start=START + HOUR * 5, end=START + HOUR * 6) == []
    )


def test_naive_datetimes_are_rejected():
    with pytest.raises(ValueError, match="timezone-aware"):
        fetch(FIXTURES / "dataset", start=datetime(2026, 8, 10))  # noqa: DTZ001


def test_an_inverted_range_is_rejected():
    with pytest.raises(ValueError, match="start must be before end"):
        fetch(FIXTURES / "dataset", start=START + HOUR, end=START)


def test_a_non_utc_request_window_is_honoured():
    offset = timezone_window()

    candles = fetch(FIXTURES / "dataset", start=offset[0], end=offset[1])

    assert len(candles) == 60
    assert candles[0].timestamp == START


def timezone_window():
    from datetime import timezone

    plus_three = timezone(timedelta(hours=3))

    return (
        START.astimezone(plus_three),
        (START + HOUR).astimezone(plus_three),
    )


# --------------------------------------------------------------------------
# Faithful reading
# --------------------------------------------------------------------------


def test_duplicates_and_invalid_rows_are_passed_through():
    """The provider reports what the file says; the pipeline judges it."""
    candles = fetch(FIXTURES / "messy_1min.csv")

    timestamps = [candle.timestamp for candle in candles]

    assert timestamps.count(START + MINUTE) == 2
    assert any(candle.high < candle.low for candle in candles)


def test_no_candle_is_invented_for_a_gap():
    candles = fetch(FIXTURES / "messy_1min.csv", end=START + MINUTE * 10)

    timestamps = [candle.timestamp for candle in candles]

    assert START + MINUTE * 4 not in timestamps
    assert START + MINUTE * 5 not in timestamps
    assert START + MINUTE * 6 in timestamps


def test_out_of_order_rows_are_returned_chronologically():
    candles = fetch(FIXTURES / "messy_1min.csv")

    timestamps = [candle.timestamp for candle in candles]

    assert timestamps == sorted(timestamps)


def test_reading_is_deterministic():
    first = fetch(FIXTURES / "messy_1min.csv")
    second = fetch(FIXTURES / "messy_1min.csv")

    assert first == second


def test_a_duplicate_keeps_its_file_order():
    """Sorting is stable, so the later row stays later and dedup is stable."""
    candles = [
        candle
        for candle in fetch(FIXTURES / "messy_1min.csv")
        if candle.timestamp == START + MINUTE
    ]

    assert [candle.close for candle in candles] == [
        Decimal("1.17006"),
        Decimal("1.17010"),
    ]


# --------------------------------------------------------------------------
# Timestamps
# --------------------------------------------------------------------------


def test_offsets_are_normalized_to_utc():
    candles = fetch(FIXTURES / "timezones_1min.csv")

    assert [candle.timestamp for candle in candles] == [
        START + MINUTE * index for index in range(4)
    ]
    assert all(candle.timestamp.utcoffset().total_seconds() == 0 for candle in candles)


def test_a_naive_timestamp_in_the_file_is_rejected():
    with pytest.raises(CsvProviderError, match="no timezone"):
        fetch(FIXTURES / "naive_timestamps.csv")


def test_an_unparseable_timestamp_names_the_line():
    with pytest.raises(CsvProviderError, match="line 3: invalid ISO-8601"):
        fetch(FIXTURES / "malformed_timestamp.csv")


# --------------------------------------------------------------------------
# Malformed input
# --------------------------------------------------------------------------


def test_a_non_numeric_price_names_the_line_and_field():
    with pytest.raises(CsvProviderError, match="line 3: high 'not-a-price'"):
        fetch(FIXTURES / "malformed_number.csv")


def test_missing_columns_are_reported_with_what_was_expected():
    with pytest.raises(CsvProviderError, match="missing required columns: high, low"):
        fetch(FIXTURES / "missing_columns.csv")

    assert REQUIRED_COLUMNS == ("timestamp", "open", "high", "low", "close")


def test_a_row_with_too_many_values_is_rejected():
    with pytest.raises(CsvProviderError, match="more values than the header"):
        fetch(FIXTURES / "ragged_row.csv")


def test_an_empty_file_is_rejected():
    with pytest.raises(CsvProviderError, match="is empty"):
        fetch(FIXTURES / "empty.csv")


def test_a_header_without_rows_returns_nothing():
    assert fetch(FIXTURES / "header_only.csv") == []


def test_volume_defaults_to_zero_when_the_column_is_absent():
    candles = fetch(FIXTURES / "no_volume.csv")

    assert all(candle.volume == Decimal(0) for candle in candles)


# --------------------------------------------------------------------------
# Multi-symbol files
# --------------------------------------------------------------------------


def test_a_symbol_column_selects_the_rows():
    euro = fetch(FIXTURES / "multi_symbol_1min.csv", "EUR/USD")
    sterling = fetch(FIXTURES / "multi_symbol_1min.csv", "GBP/USD")

    assert len(euro) == len(sterling) == 2
    assert all(candle.symbol == "EUR/USD" for candle in euro)
    assert all(candle.symbol == "GBP/USD" for candle in sterling)
    assert euro[0].open == Decimal("1.17000")
    assert sterling[0].open == Decimal("1.29000")


def test_an_absent_symbol_in_a_multi_symbol_file_returns_nothing():
    assert fetch(FIXTURES / "multi_symbol_1min.csv", "USD/JPY") == []
