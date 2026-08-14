"""Retry behaviour as seen by the pipeline, the checkpoint and the CLI."""

import json
from datetime import UTC, datetime, timedelta

import httpx
import pytest

from marketdata.calendar import AlwaysOpenCalendar
from marketdata.cli import build_parser, main, retry_policy_from_args
from marketdata.downloader.checkpoint import CheckpointStore, ChunkStatus
from marketdata.downloader.chunks import DurationChunkSize
from marketdata.downloader.pipeline import DownloadPipeline
from marketdata.providers.dukascopy import DukascopyProvider
from marketdata.providers.errors import ProviderAuthError, ProviderServerError
from marketdata.providers.retry import RetryPolicy

# A Monday, so the calendar choice never hides a chunk.
START = datetime(2026, 8, 10, 0, 0, tzinfo=UTC)
END = START + timedelta(hours=2)
MINUTE = timedelta(minutes=1)

INSTANT = RetryPolicy(attempts=3, backoff_seconds=0)
INSTRUMENTS = [{"id": 1, "name": "EUR/USD", "pipValue": 0.0001}]


class FlakyApi:
    """Dukascopy stub that can fail price requests a fixed number of times."""

    def __init__(self, *, failures: int = 0, status: int = 503) -> None:
        self.remaining_failures = failures
        self.status = status
        self.price_calls = 0

    def handler(self, request: httpx.Request) -> httpx.Response:
        params = request.url.params

        if params["path"] == "api/instrumentList":
            return httpx.Response(200, json=INSTRUMENTS)

        self.price_calls += 1

        if self.remaining_failures > 0:
            self.remaining_failures -= 1
            return httpx.Response(self.status)

        start_ms = int(params["start"])
        end_ms = int(params["end"])

        candles = []
        moment = START

        while moment < END:
            stamp = int(moment.timestamp() * 1000)

            if start_ms <= stamp <= end_ms:
                candles.append(
                    {
                        "timestamp": stamp,
                        "bid_open": 1.17,
                        "bid_high": 1.1710,
                        "bid_low": 1.1690,
                        "bid_close": 1.1705,
                    }
                )

            moment += MINUTE

        return httpx.Response(200, json={"candles": candles})


def build_pipeline(tmp_path, api, *, policy: RetryPolicy = INSTANT, **kwargs):
    provider = DukascopyProvider(
        client=httpx.Client(transport=httpx.MockTransport(api.handler)),
        retry_policy=policy,
    )

    return DownloadPipeline(
        provider,
        output_root=tmp_path / "processed",
        manifest_root=tmp_path / "manifests",
        quality_root=tmp_path / "quality",
        checkpoint_root=tmp_path / "checkpoints",
        chunk_size=DurationChunkSize(amount=1, unit="h"),
        calendar=AlwaysOpenCalendar(),
        **kwargs,
    )


def run(pipeline, **kwargs):
    return pipeline.run(symbol="EUR/USD", start=START, end=END, **kwargs)


def checkpoint_for(tmp_path):
    return CheckpointStore(tmp_path / "checkpoints").load(
        symbol="EUR/USD",
        timeframe="1min",
        start=START,
        end=END,
    )


def test_a_transient_failure_is_absorbed_without_failing_the_chunk(tmp_path):
    api = FlakyApi(failures=1)

    result = run(build_pipeline(tmp_path, api))

    assert result.chunks_failed == 0
    assert result.chunks_completed == 2
    assert result.retries == 1
    assert result.retained_count == 120


def test_successful_retries_are_recorded_in_the_checkpoint(tmp_path):
    run(build_pipeline(tmp_path, FlakyApi(failures=2)))

    checkpoint = checkpoint_for(tmp_path)
    first = checkpoint.chunks[0]

    assert first.status is ChunkStatus.COMPLETED
    assert first.attempts == 1
    assert first.retries == 2
    assert first.error is None
    assert first.retryable is None


def test_retries_are_counted_per_chunk(tmp_path):
    """A chunk's record shows its own cost, not the run's running total."""
    api = FlakyApi(failures=1)

    run(build_pipeline(tmp_path, api))

    checkpoint = checkpoint_for(tmp_path)

    assert [record.retries for record in checkpoint.chunks] == [1, 0]


def test_exhausted_retries_leave_the_chunk_failed(tmp_path):
    result = run(build_pipeline(tmp_path, FlakyApi(failures=100)))

    assert result.chunks_failed == 2
    assert result.chunks_completed == 0
    assert result.quality.status.value == "failed"
    assert "ProviderServerError" in result.failures[0]


def test_exhaustion_records_the_final_failure_state(tmp_path):
    run(build_pipeline(tmp_path, FlakyApi(failures=100)))

    record = checkpoint_for(tmp_path).chunks[0]

    assert record.status is ChunkStatus.FAILED
    assert record.attempts == 1
    assert record.retries == INSTANT.attempts - 1
    assert record.error_kind == "ProviderServerError"
    assert record.retryable is True
    assert "HTTP 503" in record.error


def test_a_permanent_failure_fails_fast_and_is_marked_unretryable(tmp_path):
    run(build_pipeline(tmp_path, FlakyApi(failures=100, status=403)))

    record = checkpoint_for(tmp_path).chunks[0]

    assert record.status is ChunkStatus.FAILED
    assert record.retries == 0
    assert record.error_kind == "ProviderAuthError"
    assert record.retryable is False


def test_retry_metadata_survives_a_round_trip_through_disk(tmp_path):
    result = run(build_pipeline(tmp_path, FlakyApi(failures=100)))

    payload = json.loads(result.checkpoint_path.read_text())["chunks"][0]

    assert payload["status"] == "failed"
    assert payload["retries"] == INSTANT.attempts - 1
    assert payload["error_kind"] == "ProviderServerError"
    assert payload["retryable"] is True
    assert "HTTP 503" in payload["error"]


def test_a_chunk_that_exhausted_retries_is_retried_on_a_later_run(tmp_path):
    failing = FlakyApi(failures=100)

    first = run(build_pipeline(tmp_path, failing))

    assert first.chunks_failed == 2
    assert first.retained_count == 0

    # The provider recovers; the same command must finish the job.
    second = run(build_pipeline(tmp_path, FlakyApi(failures=0)))

    assert second.chunks_failed == 0
    assert second.chunks_completed == 2
    assert second.retained_count == 120
    assert second.quality.status.value == "ok"


def test_a_resumed_chunk_keeps_its_earlier_retry_history(tmp_path):
    run(build_pipeline(tmp_path, FlakyApi(failures=100)))
    run(build_pipeline(tmp_path, FlakyApi(failures=0)))

    record = checkpoint_for(tmp_path).chunks[0]

    assert record.status is ChunkStatus.COMPLETED
    assert record.attempts == 2
    assert record.retries == INSTANT.attempts - 1
    assert record.error is None


def test_completed_chunks_are_not_re_fetched_after_a_partial_failure(tmp_path):
    # Two chunks, and only enough failures to exhaust the first one.
    api = FlakyApi(failures=INSTANT.attempts)

    first = run(build_pipeline(tmp_path, api))

    assert first.chunks_failed == 1
    assert first.chunks_completed == 1

    recovered = FlakyApi(failures=0)
    second = run(build_pipeline(tmp_path, recovered))

    assert second.chunks_skipped == 1
    assert recovered.price_calls == 1
    assert second.retained_count == 120


def test_retries_can_be_switched_off(tmp_path):
    api = FlakyApi(failures=1)

    result = run(
        build_pipeline(tmp_path, api, policy=RetryPolicy(attempts=1)),
    )

    assert result.chunks_failed == 1
    assert result.retries == 0


def test_strict_mode_still_retries_before_stopping(tmp_path):
    """--strict is about giving up on a chunk, not about skipping retries."""
    api = FlakyApi(failures=1)

    result = run(build_pipeline(tmp_path, api, strict=True))

    assert result.chunks_failed == 0
    assert result.retries == 1


def test_strict_mode_raises_once_retries_are_exhausted(tmp_path):
    with pytest.raises(ProviderServerError):
        run(build_pipeline(tmp_path, FlakyApi(failures=100), strict=True))

    record = checkpoint_for(tmp_path).chunks[0]

    assert record.status is ChunkStatus.FAILED
    assert record.retries == INSTANT.attempts - 1


def test_strict_mode_raises_immediately_on_a_permanent_failure(tmp_path):
    api = FlakyApi(failures=100, status=401)

    with pytest.raises(ProviderAuthError):
        run(build_pipeline(tmp_path, api, strict=True))

    assert api.price_calls == 1


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------


def parse(*extra):
    return build_parser().parse_args(
        [
            "download",
            "--symbol",
            "EUR/USD",
            "--start",
            "2026-08-10T00:00:00Z",
            "--end",
            "2026-08-10T02:00:00Z",
            *extra,
        ]
    )


def test_retry_defaults_are_bounded():
    policy = retry_policy_from_args(parse())

    assert policy.attempts > 1
    assert policy.enabled is True
    assert policy.backoff_seconds > 0
    assert policy.max_backoff_seconds >= policy.backoff_seconds


def test_retry_settings_are_configurable():
    policy = retry_policy_from_args(
        parse(
            "--retry-attempts",
            "7",
            "--retry-backoff",
            "0.5",
            "--retry-max-backoff",
            "9",
        )
    )

    assert policy.attempts == 7
    assert policy.backoff_seconds == 0.5
    assert policy.max_backoff_seconds == 9


def test_retries_can_be_disabled_from_the_command_line():
    assert retry_policy_from_args(parse("--retry-attempts", "1")).enabled is False


def test_invalid_retry_settings_are_rejected():
    with pytest.raises(ValueError, match="attempts must be at least 1"):
        retry_policy_from_args(parse("--retry-attempts", "0"))


def test_the_summary_reports_provider_retries(tmp_path, capsys):
    api = FlakyApi(failures=2)

    def provider_factory() -> DukascopyProvider:
        return DukascopyProvider(
            client=httpx.Client(transport=httpx.MockTransport(api.handler)),
            retry_policy=INSTANT,
        )

    exit_code = main(
        [
            "download",
            "--symbol",
            "EUR/USD",
            "--start",
            "2026-08-10T00:00:00Z",
            "--end",
            "2026-08-10T02:00:00Z",
            "--chunk-size",
            "1h",
            "--calendar",
            "24x7",
            "--data-root",
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

    output = capsys.readouterr().out

    assert exit_code == 0
    assert "Provider retries:  2" in output
    assert "Quality status:    ok" in output
