from __future__ import annotations

import argparse
from collections.abc import Callable, Sequence
from datetime import UTC, datetime

from marketdata.downloader.pipeline import DownloadPipeline, DownloadResult
from marketdata.providers.base import MarketDataProvider
from marketdata.providers.dukascopy import DukascopyError, DukascopyProvider

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
        "--output-root",
        default="data/processed",
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
        "--strict",
        action="store_true",
        help="Fail instead of dropping candles that break OHLC invariants.",
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
        (
            f"Requested range:   {_format_timestamp(result.requested_start)}"
            f" -> {_format_timestamp(result.requested_end)}"
        ),
        (
            f"Actual range:      {_format_timestamp(result.actual_start)}"
            f" -> {_format_timestamp(result.actual_end)}"
        ),
        f"Rows downloaded:   {result.downloaded_count}",
        f"Rows retained:     {result.final_count}",
        f"Duplicates:        {result.duplicate_count}",
        f"Invalid rows:      {result.invalid_count}",
        f"Out of range rows: {result.out_of_range_count}",
        f"Missing candles:   {report.missing_candles}",
        f"Quality status:    {report.status.value}",
    ]

    if report.expected_rows is not None:
        lines.append(f"Expected rows:     {report.expected_rows}")

    lines.append(f"Parquet files:     {len(result.files)}")
    lines.extend(f"  - {path}" for path in result.files)
    lines.append(f"Manifest:          {result.manifest}")
    lines.append(f"Quality report:    {result.quality_report_path}")

    return "\n".join(lines)


def run_download(
    args: argparse.Namespace,
    *,
    provider_factory: ProviderFactory = DukascopyProvider,
) -> int:
    provider = provider_factory()

    try:
        pipeline = DownloadPipeline(
            provider,
            output_root=args.output_root,
            manifest_root=args.manifest_root,
            quality_root=args.quality_root,
            strict=args.strict,
        )

        result = pipeline.run(
            symbol=args.symbol,
            start=args.start,
            end=args.end,
            timeframe=args.timeframe,
        )
    finally:
        close = getattr(provider, "close", None)

        if callable(close):
            close()

    if args.as_json:
        print(result.quality.model_dump_json(indent=2))
    else:
        print(format_summary(result))

    return 0


def main(
    argv: Sequence[str] | None = None,
    *,
    provider_factory: ProviderFactory = DukascopyProvider,
) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command != "download":
        parser.error("unknown command")
        return 2

    try:
        return run_download(args, provider_factory=provider_factory)
    except (DukascopyError, ValueError) as exc:
        print(f"error: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
