"""End-to-end coverage of the real ingestion path.

The provider is driven through a mock HTTP transport rather than a stub, so
the request construction, pagination and payload parsing in
:mod:`marketdata.providers.dukascopy` all run for real. Only the network hop
is replaced.
"""

import json
from datetime import UTC, datetime, timedelta

import httpx
import pytest

from marketdata.cli import main
from marketdata.providers.dukascopy import DukascopyProvider
from marketdata.storage.parquet import CANDLE_SCHEMA, ParquetStorage

START = datetime(2026, 8, 14, 12, 0, tzinfo=UTC)
END = datetime(2026, 8, 14, 13, 0, tzinfo=UTC)
MINUTE = timedelta(minutes=1)
PAGE_SIZE = 30
INSTRUMENT_ID = 1


def to_ms(value: datetime) -> int:
    return int(value.timestamp() * 1000)


def build_candle(index: int) -> dict:
    """Build a payload candle with the float noise a JSON API really returns."""
    base = 1.17 + index * 0.0001

    return {
        "timestamp": to_ms(START + MINUTE * index),
        "bid_open": base,
        "bid_high": base + 0.0002,
        "bid_low": base - 0.0001,
        "bid_close": base + 0.00005,
        "volume": index * 0.1,
    }


class DukascopyStubApi:
    """Serve the hour of one-minute candles in pages, newest page first."""

    def __init__(self) -> None:
        self.requests: list[httpx.QueryParams] = []

    @property
    def price_requests(self) -> list[httpx.QueryParams]:
        return [
            params
            for params in self.requests
            if params["path"] == "api/historicalPrices"
        ]

    def handler(self, request: httpx.Request) -> httpx.Response:
        params = request.url.params
        self.requests.append(params)

        if params["path"] == "api/instrumentList":
            return httpx.Response(
                200,
                json=[
                    {
                        "id": INSTRUMENT_ID,
                        "name": "EUR/USD",
                        "pipValue": 0.0001,
                        "nameLong": "Euro/US Dollar",
                    },
                    {"id": 2, "name": "GBP/USD", "pipValue": 0.0001},
                ],
            )

        if params["path"] == "api/historicalPrices":
            start_ms = int(params["start"])
            end_ms = int(params["end"])

            available = [
                candle
                for candle in (build_candle(index) for index in range(60))
                if start_ms <= candle["timestamp"] <= end_ms
            ]

            return httpx.Response(200, json={"candles": available[-PAGE_SIZE:]})

        return httpx.Response(404)


@pytest.fixture
def api() -> DukascopyStubApi:
    return DukascopyStubApi()


@pytest.fixture
def provider_factory(api):
    def factory() -> DukascopyProvider:
        return DukascopyProvider(
            client=httpx.Client(transport=httpx.MockTransport(api.handler))
        )

    return factory


def test_provider_paginates_backwards_until_the_range_is_covered(provider_factory, api):
    with provider_factory() as provider:
        candles = provider.fetch_candles("EUR/USD", START, END)

    assert len(api.price_requests) == 2
    assert len(candles) == 60
    assert candles[0].timestamp == START
    assert candles[-1].timestamp == END - MINUTE
    assert all(START <= candle.timestamp < END for candle in candles)


def test_provider_requests_utc_bid_candles(provider_factory, api):
    with provider_factory() as provider:
        provider.fetch_candles("EUR/USD", START, END)

    first = api.price_requests[0]

    assert first["instrument"] == str(INSTRUMENT_ID)
    assert first["timeFrame"] == "1min"
    assert first["dayStartTime"] == "UTC"
    assert first["offerSide"] == "B"
    assert int(first["start"]) == to_ms(START)
    assert int(first["end"]) == to_ms(END)


def test_download_command_produces_a_readable_dataset(
    tmp_path, provider_factory, capsys
):
    exit_code = main(
        [
            "download",
            "--symbol",
            "EUR/USD",
            "--start",
            "2026-08-14T12:00:00Z",
            "--end",
            "2026-08-14T13:00:00Z",
            "--output-root",
            str(tmp_path / "processed"),
            "--manifest-root",
            str(tmp_path / "manifests"),
            "--quality-root",
            str(tmp_path / "quality"),
            "--checkpoint-root",
            str(tmp_path / "checkpoints"),
        ],
        provider_factory=provider_factory,
    )

    assert exit_code == 0

    output = capsys.readouterr().out

    assert "Provider:          dukascopy" in output
    assert "Rows retained:     60" in output
    assert "Quality status:    ok" in output

    # Stored under the intended partitioning scheme.
    parquet = tmp_path / (
        "processed/EUR_USD/timeframe=1min/year=2026/month=08/candles.parquet"
    )
    assert parquet.exists()

    # Readable back through PyArrow.
    storage = ParquetStorage(tmp_path / "processed")
    table = storage.read_table(symbol="EUR/USD", timeframe="1min")

    assert table.num_rows == 60
    assert table.column("timestamp")[0].as_py() == START

    candles = storage.read_candles(symbol="EUR/USD", timeframe="1min")

    assert len(candles) == 60
    assert candles[0].symbol == "EUR/USD"
    assert all(candle.timestamp.tzinfo is not None for candle in candles)
    assert all(candle.high >= candle.low for candle in candles)

    for name in CANDLE_SCHEMA.names:
        assert name in table.column_names


def test_download_command_records_the_run(tmp_path, provider_factory):
    main(
        [
            "download",
            "--symbol",
            "EUR/USD",
            "--start",
            "2026-08-14T12:00:00Z",
            "--end",
            "2026-08-14T13:00:00Z",
            "--output-root",
            str(tmp_path / "processed"),
            "--manifest-root",
            str(tmp_path / "manifests"),
            "--quality-root",
            str(tmp_path / "quality"),
            "--checkpoint-root",
            str(tmp_path / "checkpoints"),
        ],
        provider_factory=provider_factory,
    )

    manifest_path = (
        tmp_path / "manifests/EUR_USD/1min_20260814T120000Z_20260814T130000Z.json"
    )
    report_path = (
        tmp_path / "quality/EUR_USD/1min_20260814T120000Z_20260814T130000Z.json"
    )

    manifest = json.loads(manifest_path.read_text())
    report = json.loads(report_path.read_text())

    assert manifest["provider"] == "dukascopy"
    assert manifest["row_count"] == 60
    assert manifest["quality_status"] == "ok"
    assert manifest["quality_report"] == str(report_path)

    assert report["downloaded_rows"] == 60
    assert report["retained_rows"] == 60
    assert report["expected_rows"] == 60
    assert report["duplicates_removed"] == 0
    assert report["invalid_rows"] == 0
    assert report["missing_intervals"] == []
    assert report["status"] == "ok"


def download(tmp_path, provider_factory, *extra):
    return main(
        [
            "download",
            "--symbol",
            "EUR/USD",
            "--start",
            "2026-08-14T12:00:00Z",
            "--end",
            "2026-08-14T13:00:00Z",
            "--data-root",
            str(tmp_path / "processed"),
            "--manifest-root",
            str(tmp_path / "manifests"),
            "--quality-root",
            str(tmp_path / "quality"),
            "--checkpoint-root",
            str(tmp_path / "checkpoints"),
            *extra,
        ],
        provider_factory=provider_factory,
    )


def test_chunked_download_covers_the_range_once(tmp_path, provider_factory, api):
    exit_code = download(tmp_path, provider_factory, "--chunk-size", "30min")

    assert exit_code == 0

    windows = [
        (int(params["start"]), int(params["end"])) for params in api.price_requests
    ]

    # Each chunk opens with a request for its own window, and pagination
    # inside a chunk never reaches outside the requested range.
    assert windows[0] == (to_ms(START), to_ms(START + MINUTE * 30))
    assert (to_ms(START + MINUTE * 30), to_ms(END)) in windows
    assert all(
        to_ms(START) <= window[0] < window[1] <= to_ms(END) for window in windows
    )

    storage = ParquetStorage(tmp_path / "processed")
    timestamps = storage.read_timestamps(symbol="EUR/USD", timeframe="1min")

    assert len(timestamps) == 60
    assert len(set(timestamps)) == 60
    assert timestamps[0] == START
    assert timestamps[-1] == END - MINUTE


def test_a_failed_chunk_is_recovered_by_rerunning(tmp_path, api, capsys):
    failing = {"active": True}

    def handler(request: httpx.Request) -> httpx.Response:
        params = request.url.params

        if (
            failing["active"]
            and params["path"] == "api/historicalPrices"
            and int(params["start"]) >= to_ms(START + MINUTE * 30)
        ):
            return httpx.Response(503)

        return api.handler(request)

    def provider_factory() -> DukascopyProvider:
        return DukascopyProvider(
            client=httpx.Client(transport=httpx.MockTransport(handler))
        )

    assert download(tmp_path, provider_factory, "--chunk-size", "30min") == 1

    output = capsys.readouterr().out

    assert "Quality status:    failed" in output
    assert "Failed chunks:" in output

    storage = ParquetStorage(tmp_path / "processed")

    assert len(storage.read_timestamps(symbol="EUR/USD", timeframe="1min")) == 30

    failing["active"] = False

    assert download(tmp_path, provider_factory, "--chunk-size", "30min") == 0

    output = capsys.readouterr().out

    assert "1 already done" in output
    assert "Quality status:    ok" in output

    timestamps = storage.read_timestamps(symbol="EUR/USD", timeframe="1min")

    assert len(timestamps) == 60
    assert len(set(timestamps)) == 60
