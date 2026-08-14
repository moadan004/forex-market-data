"""Chunked, resumable downloads through the full pipeline."""

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from marketdata.calendar import AlwaysOpenCalendar, ForexCalendar
from marketdata.downloader.checkpoint import CheckpointStore, ChunkStatus
from marketdata.downloader.chunks import DurationChunkSize, MonthlyChunkSize
from marketdata.downloader.pipeline import DownloadPipeline
from marketdata.models.candle import Candle
from marketdata.providers.base import MarketDataProvider
from marketdata.quality.report import QualityStatus
from marketdata.storage.parquet import ParquetStorage

# A Monday, so the default forex calendar is open through the week.
START = datetime(2026, 8, 10, 0, 0, tzinfo=UTC)
HOUR = timedelta(hours=1)
MINUTE = timedelta(minutes=1)
END = START + HOUR * 4


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


class WindowProvider(MarketDataProvider):
    """Serves one candle a minute, and can be told to fail on some windows."""

    def __init__(self, *, fail_from: datetime | None = None) -> None:
        self.fail_from = fail_from
        self.windows: list[tuple[datetime, datetime]] = []

    @property
    def name(self) -> str:
        return "window"

    def get_supported_symbols(self) -> list[str]:
        return ["EUR/USD"]

    def fetch_candles(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        timeframe: str = "1min",
    ) -> list[Candle]:
        self.windows.append((start, end))

        if self.fail_from is not None and start >= self.fail_from:
            raise RuntimeError("provider unavailable")

        candles = []
        moment = start

        while moment < end:
            candles.append(make_candle(moment))
            moment += MINUTE

        return candles

    def health_check(self) -> bool:
        return True


def build_pipeline(tmp_path, provider, **kwargs):
    kwargs.setdefault("chunk_size", DurationChunkSize(amount=1, unit="h"))
    kwargs.setdefault("calendar", AlwaysOpenCalendar())

    return DownloadPipeline(
        provider,
        output_root=tmp_path / "processed",
        manifest_root=tmp_path / "manifests",
        quality_root=tmp_path / "quality",
        checkpoint_root=tmp_path / "checkpoints",
        **kwargs,
    )


def run(pipeline, **kwargs):
    return pipeline.run(symbol="EUR/USD", start=START, end=END, **kwargs)


def stored_timestamps(tmp_path) -> list[datetime]:
    return ParquetStorage(tmp_path / "processed").read_timestamps(
        symbol="EUR/USD",
        timeframe="1min",
    )


def checkpoint_for(tmp_path):
    return CheckpointStore(tmp_path / "checkpoints").load(
        symbol="EUR/USD",
        timeframe="1min",
        start=START,
        end=END,
    )


def test_fresh_run_downloads_every_chunk(tmp_path):
    provider = WindowProvider()

    result = run(build_pipeline(tmp_path, provider))

    assert provider.windows == [
        (START + HOUR * index, START + HOUR * (index + 1)) for index in range(4)
    ]
    assert result.chunks_total == 4
    assert result.chunks_completed == 4
    assert result.chunks_failed == 0
    assert result.chunks_skipped == 0
    assert result.retained_count == 240
    assert result.quality.status is QualityStatus.OK
    assert len(stored_timestamps(tmp_path)) == 240


def test_chunks_are_recorded_as_completed(tmp_path):
    run(build_pipeline(tmp_path, WindowProvider()))

    checkpoint = checkpoint_for(tmp_path)

    assert checkpoint is not None
    assert checkpoint.is_complete
    assert [record.status for record in checkpoint.chunks] == [
        ChunkStatus.COMPLETED
    ] * 4
    assert [record.row_count for record in checkpoint.chunks] == [60] * 4
    assert all(record.files for record in checkpoint.chunks)


def test_a_failing_chunk_does_not_corrupt_the_completed_ones(tmp_path):
    provider = WindowProvider(fail_from=START + HOUR * 2)

    result = run(build_pipeline(tmp_path, provider))

    assert result.chunks_completed == 2
    assert result.chunks_failed == 2
    assert result.quality.status is QualityStatus.FAILED
    assert len(result.failures) == 2
    assert "provider unavailable" in result.failures[0]

    # The two chunks that succeeded are stored and intact.
    timestamps = stored_timestamps(tmp_path)

    assert len(timestamps) == 120
    assert timestamps[0] == START
    assert timestamps[-1] == START + HOUR * 2 - MINUTE


def test_a_failed_run_reports_the_hole_it_left(tmp_path):
    result = run(build_pipeline(tmp_path, WindowProvider(fail_from=START + HOUR * 2)))

    report = result.quality

    assert report.missing_candles == 120
    assert len(report.missing_intervals) == 1
    assert report.missing_intervals[0].start == START + HOUR * 2
    assert report.missing_intervals[0].end == END


def test_resuming_downloads_only_the_missing_chunks(tmp_path):
    run(build_pipeline(tmp_path, WindowProvider(fail_from=START + HOUR * 2)))

    provider = WindowProvider()
    result = run(build_pipeline(tmp_path, provider))

    assert provider.windows == [
        (START + HOUR * 2, START + HOUR * 3),
        (START + HOUR * 3, END),
    ]
    assert result.chunks_skipped == 2
    assert result.downloaded_count == 120
    assert result.retained_count == 240
    assert result.quality.status is QualityStatus.OK
    assert len(stored_timestamps(tmp_path)) == 240


def test_rerunning_a_completed_download_fetches_nothing(tmp_path):
    run(build_pipeline(tmp_path, WindowProvider()))

    provider = WindowProvider()
    result = run(build_pipeline(tmp_path, provider))

    assert provider.windows == []
    assert result.chunks_skipped == 4
    assert result.downloaded_count == 0
    assert result.retained_count == 240
    assert result.quality.status is QualityStatus.OK


def test_rerunning_does_not_duplicate_candles(tmp_path):
    run(build_pipeline(tmp_path, WindowProvider()))
    run(build_pipeline(tmp_path, WindowProvider()), resume=False)
    run(build_pipeline(tmp_path, WindowProvider()), resume=False)

    timestamps = stored_timestamps(tmp_path)

    assert len(timestamps) == 240
    assert len(set(timestamps)) == 240
    assert timestamps == sorted(timestamps)


def test_disabling_resume_downloads_everything_again(tmp_path):
    run(build_pipeline(tmp_path, WindowProvider()))

    provider = WindowProvider()
    result = run(build_pipeline(tmp_path, provider), resume=False)

    assert len(provider.windows) == 4
    assert result.chunks_skipped == 0
    assert result.downloaded_count == 240
    assert result.retained_count == 240


def test_a_retried_chunk_records_its_attempts(tmp_path):
    run(build_pipeline(tmp_path, WindowProvider(fail_from=START + HOUR * 3)))
    run(build_pipeline(tmp_path, WindowProvider(fail_from=START + HOUR * 3)))

    checkpoint = checkpoint_for(tmp_path)

    assert checkpoint.chunks[3].status is ChunkStatus.FAILED
    assert checkpoint.chunks[3].attempts == 2
    assert checkpoint.chunks[0].attempts == 1


def test_strict_mode_stops_at_the_first_failure(tmp_path):
    provider = WindowProvider(fail_from=START + HOUR * 2)

    with pytest.raises(RuntimeError, match="provider unavailable"):
        run(build_pipeline(tmp_path, provider, strict=True))

    checkpoint = checkpoint_for(tmp_path)

    assert checkpoint.completed_chunks == 2
    assert checkpoint.chunks[2].status is ChunkStatus.FAILED
    assert checkpoint.chunks[3].status is ChunkStatus.PENDING
    assert len(stored_timestamps(tmp_path)) == 120


def test_a_strict_failure_can_be_resumed_without_strict_mode(tmp_path):
    with pytest.raises(RuntimeError):
        run(
            build_pipeline(
                tmp_path,
                WindowProvider(fail_from=START + HOUR * 2),
                strict=True,
            )
        )

    result = run(build_pipeline(tmp_path, WindowProvider()))

    assert result.chunks_skipped == 2
    assert result.retained_count == 240
    assert result.quality.status is QualityStatus.OK


def test_monthly_chunks_span_partitions(tmp_path):
    provider = WindowProvider()

    pipeline = DownloadPipeline(
        provider,
        output_root=tmp_path / "processed",
        manifest_root=tmp_path / "manifests",
        quality_root=tmp_path / "quality",
        checkpoint_root=tmp_path / "checkpoints",
        chunk_size=MonthlyChunkSize(),
        calendar=AlwaysOpenCalendar(),
    )

    result = pipeline.run(
        symbol="EUR/USD",
        start=datetime(2026, 8, 31, 23, 0, tzinfo=UTC),
        end=datetime(2026, 9, 1, 1, 0, tzinfo=UTC),
        timeframe="1min",
    )

    assert result.chunks_total == 2
    assert result.retained_count == 120
    assert len(result.files) == 2
    assert {path.parent.name for path in result.files} == {"month=08", "month=09"}


def test_the_manifest_links_the_checkpoint(tmp_path):
    result = run(build_pipeline(tmp_path, WindowProvider()))

    manifest = json.loads(result.manifest.read_text())

    assert manifest["checkpoint"] == str(result.checkpoint_path)
    assert manifest["row_count"] == 240
    assert manifest["quality_status"] == "ok"


def test_the_report_counts_chunks(tmp_path):
    result = run(build_pipeline(tmp_path, WindowProvider(fail_from=START + HOUR * 3)))

    report = json.loads(result.quality_report_path.read_text())

    assert report["chunks_total"] == 4
    assert report["chunks_completed"] == 3
    assert report["chunks_failed"] == 1
    assert report["status"] == "failed"
    assert report["calendar"] == "24x7"


def test_a_weekend_download_is_complete_with_no_data(tmp_path):
    """The forex calendar expects nothing over the weekend."""
    provider = WindowProvider(fail_from=START)

    pipeline = DownloadPipeline(
        provider,
        output_root=tmp_path / "processed",
        manifest_root=tmp_path / "manifests",
        quality_root=tmp_path / "quality",
        checkpoint_root=tmp_path / "checkpoints",
        calendar=ForexCalendar(),
    )

    result = pipeline.run(
        symbol="EUR/USD",
        start=datetime(2026, 8, 15, 0, 0, tzinfo=UTC),
        end=datetime(2026, 8, 16, 0, 0, tzinfo=UTC),
        timeframe="1min",
    )

    assert result.quality.expected_rows == 0
    assert result.quality.missing_candles == 0
    assert len(result.quality.market_closed_intervals) == 1
