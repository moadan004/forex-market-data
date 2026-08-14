from __future__ import annotations

import argparse
from datetime import UTC, datetime


def parse_datetime(value: str) -> datetime:
    """Parse an ISO-8601 timestamp and require timezone information."""
    normalized = value.replace("Z", "+00:00")
    result = datetime.fromisoformat(normalized)

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
        help="UTC end time in ISO-8601 format.",
    )
    download.add_argument(
        "--timeframe",
        default="1min",
        help="Dukascopy timeframe. Default: 1min.",
    )

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "download":
        from marketdata.downloader.pipeline import DownloadPipeline
        from marketdata.providers.dukascopy import DukascopyProvider

        provider = DukascopyProvider()

        try:
            pipeline = DownloadPipeline(provider)

            result = pipeline.run(
                symbol=args.symbol,
                start=args.start,
                end=args.end,
                timeframe=args.timeframe,
            )
        finally:
            provider.close()

        print(f"Provider:       {result.provider}")
        print(f"Symbol:         {result.symbol}")
        print(f"Timeframe:      {result.timeframe}")
        print(f"Downloaded:     {result.downloaded_count}")
        print(f"Final candles:  {result.final_count}")
        print(f"Duplicates:     {result.duplicate_count}")
        print(f"Manifest:       {result.manifest}")

        for file in result.files:
            print(f"Parquet:        {file}")

        return 0

    parser.error("unknown command")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
