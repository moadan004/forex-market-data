import json
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal

import pytest

from marketdata.downloader.pipeline import DownloadPipeline
from marketdata.models.candle import Candle
from marketdata.providers.base import MarketDataProvider
from marketdata.quality.report import QualityStatus
from marketdata.storage.parquet import ParquetStorage
from marketdata.validation.candles import CandleValidationError

START = datetime(2026, 8, 14, 12, 0, tzinfo=UTC)
END = datetime(2026, 8, 14, 13, 0, tzinfo=UTC)
MINUTE = timedelta(minutes=1)
UTC_PLUS_3 = timezone(timedelta(hours=3))


def make_candle(
    timestamp: datetime,
    *,
    close: str = "1.1705",
    high: str = "1.1710",
    low: str = "1.1690",
) -> Candle:
    return Candle(
        timestamp=timestamp,
        symbol="EUR/USD",
        open=Decimal("1.1700"),
        high=Decimal(high),
        low=Decimal(low),
        close=Decimal(close),
        volume=Decimal(100),
    )


class StubProvider(MarketDataProvider):
    """Provider returning a fixed payload, recording the call it received."""

    def __init__(self, candles: list[Candle]) -> None:
        self._candles = candles
        self.calls: list[dict] = []
        self.closed = False

    @property
    def name(self) -> str:
        return "stub"

    def get_supported_symbols(self) -> list[str]:
        return ["EUR/USD"]

    def fetch_candles(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        timeframe: str = "1min",
    ) -> list[Candle]:
        self.calls.append(
            {
                "symbol": symbol,
                "start": start,
                "end": end,
                "timeframe": timeframe,
            }
        )
        return list(self._candles)

    def health_check(self) -> bool:
        return True

    def close(self) -> None:
        self.closed = True


def messy_payload() -> list[Candle]:
    return [
        # Out of range: before the requested window.
        make_candle(START - MINUTE),
        # Not UTC: 15:00+03:00 is 12:00Z and must normalize into the window.
        make_candle(datetime(2026, 8, 14, 15, 0, tzinfo=UTC_PLUS_3)),
        make_candle(START + MINUTE, close="1.1705"),
        # Duplicate timestamp; the last occurrence wins.
        make_candle(START + MINUTE, close="1.1708"),
        # Invalid: high below low.
        make_candle(START + MINUTE * 2, high="1.1600", low="1.1690"),
        # Out of range: beyond the requested window.
        make_candle(END + MINUTE * 30),
    ]


def run_pipeline(tmp_path, candles, **kwargs):
    provider = StubProvider(candles)

    pipeline = DownloadPipeline(
        provider,
        output_root=tmp_path / "processed",
        manifest_root=tmp_path / "manifests",
        quality_root=tmp_path / "quality",
        checkpoint_root=tmp_path / "checkpoints",
        **kwargs,
    )

    result = pipeline.run(
        symbol="EUR/USD",
        start=START,
        end=END,
        timeframe="1min",
    )

    return provider, result


def test_pipeline_applies_every_stage(tmp_path):
    _, result = run_pipeline(tmp_path, messy_payload())

    assert result.downloaded_count == 6
    assert result.out_of_range_count == 2
    assert result.invalid_count == 1
    assert result.duplicate_count == 1
    assert result.retained_count == 2

    # The row arithmetic must account for every downloaded row.
    assert (
        result.downloaded_count
        - result.out_of_range_count
        - result.invalid_count
        - result.duplicate_count
        == result.retained_count
    )


def test_pipeline_normalizes_to_utc(tmp_path):
    _, result = run_pipeline(tmp_path, messy_payload())

    storage = ParquetStorage(tmp_path / "processed")
    stored = storage.read_candles(symbol="EUR/USD", timeframe="1min")

    assert [candle.timestamp for candle in stored] == [START, START + MINUTE]
    assert result.actual_start == START
    assert result.actual_end == START + MINUTE


def test_pipeline_keeps_the_last_duplicate(tmp_path):
    run_pipeline(tmp_path, messy_payload())

    storage = ParquetStorage(tmp_path / "processed")
    stored = storage.read_candles(symbol="EUR/USD", timeframe="1min")

    assert stored[1].close == Decimal("1.17080000")


def test_pipeline_excludes_data_outside_the_requested_range(tmp_path):
    run_pipeline(tmp_path, messy_payload())

    storage = ParquetStorage(tmp_path / "processed")
    stored = storage.read_candles(symbol="EUR/USD", timeframe="1min")

    assert all(START <= candle.timestamp < END for candle in stored)


def test_pipeline_writes_manifest_and_quality_report(tmp_path):
    _, result = run_pipeline(tmp_path, messy_payload())

    manifest = json.loads(result.manifest.read_text())

    assert manifest["symbol"] == "EUR/USD"
    assert manifest["timeframe"] == "1min"
    assert manifest["provider"] == "stub"
    assert manifest["row_count"] == 2
    assert manifest["quality_status"] == "invalid"
    assert manifest["quality_report"] == str(result.quality_report_path)
    assert len(manifest["files"]) == len(result.files) == 1

    report = json.loads(result.quality_report_path.read_text())

    assert report["provider"] == "stub"
    assert report["requested_start"].startswith("2026-08-14T12:00:00")
    assert report["actual_start"].startswith("2026-08-14T12:00:00")
    assert report["downloaded_rows"] == 6
    assert report["retained_rows"] == 2
    assert report["duplicates_removed"] == 1
    assert report["invalid_rows"] == 1
    assert report["expected_rows"] == 60
    assert report["missing_candles"] == 58
    assert report["status"] == "invalid"


def test_pipeline_reports_ok_for_a_complete_hour(tmp_path):
    candles = [make_candle(START + MINUTE * index) for index in range(60)]

    _, result = run_pipeline(tmp_path, candles)

    assert result.quality.status is QualityStatus.OK
    assert result.retained_count == 60
    assert result.quality.missing_candles == 0


def test_pipeline_handles_an_empty_provider_response(tmp_path):
    _, result = run_pipeline(tmp_path, [])

    assert result.retained_count == 0
    assert result.files == []
    assert result.quality.status is QualityStatus.EMPTY
    assert result.manifest.exists()
    assert result.quality_report_path.exists()


def test_strict_mode_rejects_invalid_candles(tmp_path):
    with pytest.raises(CandleValidationError, match="high cannot be below low"):
        run_pipeline(tmp_path, messy_payload(), strict=True)


def test_pipeline_passes_the_requested_window_to_the_provider(tmp_path):
    provider, _ = run_pipeline(tmp_path, [])

    assert provider.calls == [
        {
            "symbol": "EUR/USD",
            "start": START,
            "end": END,
            "timeframe": "1min",
        }
    ]


def test_pipeline_rejects_an_inverted_range(tmp_path):
    provider = StubProvider([])

    pipeline = DownloadPipeline(
        provider,
        output_root=tmp_path / "processed",
        manifest_root=tmp_path / "manifests",
        quality_root=tmp_path / "quality",
        checkpoint_root=tmp_path / "checkpoints",
    )

    with pytest.raises(ValueError, match="start must be before end"):
        pipeline.run(symbol="EUR/USD", start=END, end=START)


def test_pipeline_normalizes_a_non_utc_request_window(tmp_path):
    provider = StubProvider([])

    pipeline = DownloadPipeline(
        provider,
        output_root=tmp_path / "processed",
        manifest_root=tmp_path / "manifests",
        quality_root=tmp_path / "quality",
        checkpoint_root=tmp_path / "checkpoints",
    )

    result = pipeline.run(
        symbol="EUR/USD",
        start=datetime(2026, 8, 14, 15, 0, tzinfo=UTC_PLUS_3),
        end=datetime(2026, 8, 14, 16, 0, tzinfo=UTC_PLUS_3),
    )

    assert result.requested_start == START
    assert result.requested_end == END
    assert provider.calls[0]["start"] == START
