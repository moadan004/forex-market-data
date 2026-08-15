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
from marketdata.providers.rate_limit import DEFAULT_REQUESTS_PER_SECOND, RateLimit
from marketdata.providers.retry import (
    DEFAULT_ATTEMPTS,
    DEFAULT_BACKOFF_SECONDS,
    DEFAULT_MAX_BACKOFF_SECONDS,
    RetryPolicy,
)
from marketdata.quality.dataset import DatasetValidationReport, validate_dataset
from marketdata.quality.report import QualityStatus

ProviderFactory = Callable[[], MarketDataProvider]

MAX_LISTED_GAPS = 10


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
        "--rate-limit",
        type=float,
        default=DEFAULT_REQUESTS_PER_SECOND,
        metavar="REQUESTS_PER_SECOND",
        help=(
            "Maximum outbound provider requests per second, applied to every "
            "request including retries. Equivalent to a minimum interval of "
            "1/RATE seconds between requests. There is no unlimited setting. "
            f"Default: {DEFAULT_REQUESTS_PER_SECOND:g}."
        ),
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

    validate = subparsers.add_parser(
        "validate",
        help="Check an already-downloaded dataset without contacting a provider.",
    )

    validate.add_argument(
        "--symbol",
        required=True,
        help="Symbol to inspect, for example EUR/USD.",
    )
    validate.add_argument(
        "--timeframe",
        default="1min",
        help="Timeframe to inspect. Default: 1min.",
    )
    validate.add_argument(
        "--start",
        type=parse_datetime,
        help=(
            "UTC start of the window to judge the dataset against. "
            "Defaults to the range its manifests claim."
        ),
    )
    validate.add_argument(
        "--end",
        type=parse_datetime,
        help="UTC end of the window to judge the dataset against (exclusive).",
    )
    validate.add_argument(
        "--calendar",
        default="forex",
        choices=sorted(CALENDARS),
        help=(
            "Trading calendar used to tell an expected candle from a market "
            "closure. Default: forex."
        ),
    )
    validate.add_argument(
        "--data-root",
        default="data/processed",
        dest="data_root",
        help="Root directory holding the Parquet dataset. Default: data/processed.",
    )
    validate.add_argument(
        "--manifest-root",
        default="data/manifests",
        help="Root directory holding manifests. Default: data/manifests.",
    )
    validate.add_argument(
        "--no-manifests",
        action="store_false",
        dest="use_manifests",
        help="Judge the dataset on its own extent, ignoring any manifests.",
    )
    validate.add_argument(
        "--allow-incomplete",
        action="store_true",
        help=(
            "Exit 0 when the only finding is missing candles, for datasets "
            "with known provider or holiday gaps."
        ),
    )
    validate.add_argument(
        "--json",
        action="store_true",
        dest="as_json",
        help="Print the validation report as JSON instead of a text summary.",
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
        f"Rate limit:        {result.rate_limit}",
        f"Throttled:         {result.throttled_seconds:.1f}s",
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


def format_validation(report: DatasetValidationReport) -> str:
    """Render a human-readable summary of one dataset validation."""
    lines = [
        f"Symbol:            {report.symbol}",
        f"Timeframe:         {report.timeframe}",
        f"Calendar:          {report.calendar}",
        (
            f"Checked range:     {_format_timestamp(report.range_start)}"
            f" -> {_format_timestamp(report.range_end)}"
            f" (from {report.range_source})"
        ),
        (
            f"Actual range:      {_format_timestamp(report.actual_start)}"
            f" -> {_format_timestamp(report.actual_end)}"
        ),
        f"Candles:           {report.candles}",
    ]

    if report.expected_candles is not None:
        lines.append(f"Expected candles:  {report.expected_candles}")

    lines.extend(
        [
            f"Missing candles:   {report.missing_candles}",
            f"Detected gaps:     {len(report.missing_intervals)}",
            f"Market closures:   {len(report.market_closed_intervals)}",
            f"Duplicate candles: {report.duplicate_candles}",
            f"Invalid OHLC rows: {report.invalid_rows}",
            f"Out of range rows: {report.out_of_range_rows}",
            f"Unordered rows:    {report.unordered_rows}",
            f"Parquet files:     {report.files}",
            f"Schema consistent: {'yes' if report.schema_consistent else 'no'}",
            f"Manifests:         {len(report.manifests)}",
        ]
    )

    for check in report.manifests:
        verdict = "agrees" if check.agrees else "DISAGREES"
        lines.append(
            f"  - {check.path}: {verdict} "
            f"({check.claimed_rows} claimed, {check.stored_rows} stored)"
        )

    lines.append(f"Status:            {report.status.value}")

    if report.problems:
        lines.append("Problems:")
        lines.extend(f"  - {problem}" for problem in report.problems)

    for interval in report.missing_intervals[:MAX_LISTED_GAPS]:
        lines.append(
            f"  gap {interval.start:%Y-%m-%dT%H:%M:%SZ}"
            f" -> {interval.end:%Y-%m-%dT%H:%M:%SZ}"
            f" ({interval.missing_candles} candles)"
        )

    if len(report.missing_intervals) > MAX_LISTED_GAPS:
        lines.append(
            f"  ... and {len(report.missing_intervals) - MAX_LISTED_GAPS} more gaps"
        )

    return "\n".join(lines)


def run_validate(args: argparse.Namespace) -> int:
    report = validate_dataset(
        symbol=args.symbol,
        timeframe=args.timeframe,
        data_root=args.data_root,
        manifest_root=args.manifest_root if args.use_manifests else None,
        calendar=get_calendar(args.calendar),
        start=args.start,
        end=args.end,
    )

    if args.as_json:
        print(report.model_dump_json(indent=2))
    else:
        print(format_validation(report))

    if report.ok:
        return 0

    if args.allow_incomplete and report.status is QualityStatus.INCOMPLETE:
        return 0

    return 1


def rate_limit_from_args(args: argparse.Namespace) -> RateLimit:
    """Build the rate limit the provider should honour."""
    return RateLimit(requests_per_second=args.rate_limit)


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
            return DukascopyProvider(
                retry_policy=retry_policy_from_args(args),
                rate_limit=rate_limit_from_args(args),
            )

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

    try:
        if args.command == "download":
            return run_download(args, provider_factory=provider_factory)

        if args.command == "validate":
            return run_validate(args)
    except (ProviderError, ValueError) as exc:
        print(f"error: {exc}")
        return 1

    parser.error(f"unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
