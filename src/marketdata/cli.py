from __future__ import annotations

import argparse
from collections.abc import Callable, Sequence
from datetime import UTC, datetime

from marketdata.calendar import CALENDARS, get_calendar
from marketdata.downloader.chunks import ChunkSize, parse_chunk_size
from marketdata.downloader.pipeline import DownloadPipeline, DownloadResult
from marketdata.providers.base import MarketDataProvider
from marketdata.providers.dukascopy import DukascopyProvider
from marketdata.providers.errors import ProviderError
from marketdata.providers.retry import (
    DEFAULT_ATTEMPTS,
    DEFAULT_BACKOFF_SECONDS,
    DEFAULT_MAX_BACKOFF_SECONDS,
    RetryPolicy,
)

ProviderFactory = Callable[[], MarketDataProvider]


def parse_datetime(value: str) -> datetime:
    """Parse an ISO-8601 timestamp and require timezone information."""
    normalized = value.replace("Z", "+00:00")

    try:
        result = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid ISO-8601 datetime: {value}") from exc

    if result.tzinfo is None:
        raise argparse.ArgumentTypeError("datetime must include timezone information")

    return result.astimezone(UTC)


def chunk_size(value: str) -> ChunkSize:
    """Parse a chunk size, reporting failures as an argument error."""
    try:
        return parse_chunk_size(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="marketdata",
        description="Forex historical market-data pipeline.",
    )

    subparsers = parser.add_subparsers(
        dest="command",
        required=True,
    )

    download = subparsers.add_parser(
        "download",
        help="Download and process historical market data.",
    )

    download.add_argument(
        "--symbol",
        required=True,
        help="Provider symbol, for example EUR/USD.",
    )
    download.add_argument(
        "--start",
        required=True,
        type=parse_datetime,
        help="UTC start time in ISO-8601 format.",
    )
    download.add_argument(
        "--end",
        required=True,
        type=parse_datetime,
        help="UTC end time in ISO-8601 format (exclusive).",
    )
    download.add_argument(
        "--timeframe",
        default="1min",
        help="Dukascopy timeframe. Default: 1min.",
    )
    download.add_argument(
        "--chunk-size",
        default="1month",
        type=chunk_size,
        help=(
            "Size of each provider request, for example 1month, 3months, "
            "7d, 12h or 30min. Default: 1month."
        ),
    )
    download.add_argument(
        "--calendar",
        default="forex",
        choices=sorted(CALENDARS),
        help=(
            "Trading calendar used to tell an expected candle from a market "
            "closure. Default: forex."
        ),
    )
    download.add_argument(
        "--no-resume",
        action="store_false",
        dest="resume",
        help="Ignore saved progress and download every chunk again.",
    )
    download.add_argument(
        "--data-root",
        "--output-root",
        default="data/processed",
        dest="data_root",
        help="Root directory for Parquet output. Default: data/processed.",
    )
    download.add_argument(
        "--manifest-root",
        default="data/manifests",
        help="Root directory for manifests. Default: data/manifests.",
    )
    download.add_argument(
        "--quality-root",
        default="data/quality",
        help="Root directory for quality reports. Default: data/quality.",
    )
    download.add_argument(
        "--checkpoint-root",
        default="data/checkpoints",
        help="Root directory for resume checkpoints. Default: data/checkpoints.",
    )
    download.add_argument(
        "--retry-attempts",
        type=int,
        default=DEFAULT_ATTEMPTS,
        help=(
            "Total attempts per provider request, including the first. "
            f"1 disables retries. Default: {DEFAULT_ATTEMPTS}."
        ),
    )
    download.add_argument(
        "--retry-backoff",
        type=float,
        default=DEFAULT_BACKOFF_SECONDS,
        help=(
            "Seconds to wait before the first retry, doubling thereafter. "
            f"Default: {DEFAULT_BACKOFF_SECONDS}."
        ),
    )
    download.add_argument(
        "--retry-max-backoff",
        type=float,
        default=DEFAULT_MAX_BACKOFF_SECONDS,
        help=(
            "Upper bound on the wait between retries. "
            f"Default: {DEFAULT_MAX_BACKOFF_SECONDS}."
        ),
    )
    download.add_argument(
        "--strict",
        action="store_true",
        help=(
            "Stop on the first chunk that fails or contains candles breaking "
            "OHLC invariants, instead of recording it and continuing."
        ),
    )
    download.add_argument(
        "--json",
        action="store_true",
        dest="as_json",
        help="Print the quality report as JSON instead of a text summary.",
    )

    return parser


def _format_timestamp(value: datetime | None) -> str:
    if value is None:
        return "-"

    return f"{value:%Y-%m-%dT%H:%M:%SZ}"


def format_summary(result: DownloadResult) -> str:
    """Render a human-readable summary of one download."""
    report = result.quality

    lines = [
        f"Provider:          {result.provider}",
        f"Symbol:            {result.symbol}",
        f"Timeframe:         {result.timeframe}",
        f"Calendar:          {report.calendar}",
        (
            f"Requested range:   {_format_timestamp(result.requested_start)}"
            f" -> {_format_timestamp(result.requested_end)}"
        ),
        (
            f"Actual range:      {_format_timestamp(result.actual_start)}"
            f" -> {_format_timestamp(result.actual_end)}"
        ),
        (
            f"Chunks:            {result.chunks_completed}/{result.chunks_total}"
            f" completed, {result.chunks_failed} failed,"
            f" {result.chunks_skipped} already done"
        ),
        f"Rows downloaded:   {result.downloaded_count}",
        f"Rows retained:     {result.retained_count}",
        f"Duplicates:        {result.duplicate_count}",
        f"Invalid rows:      {result.invalid_count}",
        f"Out of range rows: {result.out_of_range_count}",
        f"Provider retries:  {result.retries}",
        f"Missing candles:   {report.missing_candles}",
        f"Market closures:   {len(report.market_closed_intervals)}",
        f"Quality status:    {report.status.value}",
    ]

    if report.expected_rows is not None:
        lines.append(f"Expected rows:     {report.expected_rows}")

    lines.append(f"Parquet files:     {len(result.files)}")
    lines.extend(f"  - {path}" for path in result.files)
    lines.append(f"Manifest:          {result.manifest}")
    lines.append(f"Quality report:    {result.quality_report_path}")
    lines.append(f"Checkpoint:        {result.checkpoint_path}")

    if result.failures:
        lines.append("Failed chunks:")
        lines.extend(f"  - {failure}" for failure in result.failures)
        lines.append("Rerun the same command to retry the failed chunks.")

    return "\n".join(lines)


def retry_policy_from_args(args: argparse.Namespace) -> RetryPolicy:
    """Build the retry policy the provider should use."""
    return RetryPolicy(
        attempts=args.retry_attempts,
        backoff_seconds=args.retry_backoff,
        max_backoff_seconds=args.retry_max_backoff,
    )


def run_download(
    args: argparse.Namespace,
    *,
    provider_factory: ProviderFactory | None = None,
) -> int:
    if provider_factory is None:

        def provider_factory() -> MarketDataProvider:
            return DukascopyProvider(retry_policy=retry_policy_from_args(args))

    provider = provider_factory()

    try:
        pipeline = DownloadPipeline(
            provider,
            output_root=args.data_root,
            manifest_root=args.manifest_root,
            quality_root=args.quality_root,
            checkpoint_root=args.checkpoint_root,
            calendar=get_calendar(args.calendar),
            chunk_size=args.chunk_size,
            strict=args.strict,
        )

        result = pipeline.run(
            symbol=args.symbol,
            start=args.start,
            end=args.end,
            timeframe=args.timeframe,
            resume=args.resume,
        )
    finally:
        close = getattr(provider, "close", None)

        if callable(close):
            close()

    if args.as_json:
        print(result.quality.model_dump_json(indent=2))
    else:
        print(format_summary(result))

    # A failed chunk leaves a hole in the dataset; the run is not a success.
    return 1 if result.chunks_failed else 0


def main(
    argv: Sequence[str] | None = None,
    *,
    provider_factory: ProviderFactory | None = None,
) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command != "download":
        parser.error("unknown command")
        return 2

    try:
        return run_download(args, provider_factory=provider_factory)
    except (ProviderError, ValueError) as exc:
        print(f"error: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
