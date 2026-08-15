from __future__ import annotations

import argparse
import json
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation

from marketdata.calendar import CALENDARS, get_calendar
from marketdata.downloader.chunks import ChunkSize, parse_chunk_size
from marketdata.downloader.pipeline import DownloadPipeline, DownloadResult
from marketdata.providers.base import MarketDataProvider
from marketdata.providers.csv import CsvMarketDataProvider
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
from marketdata.verification.comparison import (
    DEFAULT_PRICE_MISMATCH_FAIL_RATIO,
    DEFAULT_PRICE_MISMATCH_WARN_RATIO,
    DEFAULT_PRICE_TOLERANCE,
    DEFAULT_VOLUME_MISMATCH_FAIL_RATIO,
    DEFAULT_VOLUME_MISMATCH_WARN_RATIO,
    DEFAULT_VOLUME_TOLERANCE,
    ComparisonReport,
    ComparisonThresholds,
    compare_datasets,
)
from marketdata.verification.comparison_records import (
    ComparisonStore,
    build_comparison_record,
)
from marketdata.verification.guard import check_download
from marketdata.verification.preflight import PreflightReport, check_provider
from marketdata.verification.records import VerificationStore
from marketdata.verification.runner import StageOutcome, run_stage
from marketdata.verification.stages import STAGE_DESCRIPTIONS, Stage
from marketdata.verification.status import (
    DEFAULT_MISSING_FAIL_RATIO,
    DEFAULT_MISSING_WARN_RATIO,
    QualityThresholds,
    VerificationStatus,
)

ProviderFactory = Callable[[], MarketDataProvider]

PROVIDERS = ("dukascopy", "csv")

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


def tolerance(value: str) -> Decimal:
    """
    Parse a relative tolerance as a Decimal.

    Deliberately not a float: tolerances are compared against decimal prices,
    and a binary approximation of 0.0001 would make the bound depend on the
    magnitude of the price it is applied to.
    """
    try:
        parsed = Decimal(value)
    except InvalidOperation as exc:
        raise argparse.ArgumentTypeError(f"invalid decimal tolerance: {value}") from exc

    if parsed < 0 or not parsed.is_finite():
        raise argparse.ArgumentTypeError("tolerance must be a finite, positive decimal")

    return parsed


def chunk_size(value: str) -> ChunkSize:
    """Parse a chunk size, reporting failures as an argument error."""
    try:
        return parse_chunk_size(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


def _add_storage_options(parser: argparse.ArgumentParser) -> None:
    """Add the directories every artifact-writing command shares."""
    parser.add_argument(
        "--data-root",
        "--output-root",
        default="data/processed",
        dest="data_root",
        help="Root directory for Parquet output. Default: data/processed.",
    )
    parser.add_argument(
        "--manifest-root",
        default="data/manifests",
        help="Root directory for manifests. Default: data/manifests.",
    )
    parser.add_argument(
        "--quality-root",
        default="data/quality",
        help="Root directory for quality reports. Default: data/quality.",
    )
    parser.add_argument(
        "--checkpoint-root",
        default="data/checkpoints",
        help="Root directory for resume checkpoints. Default: data/checkpoints.",
    )
    parser.add_argument(
        "--verification-root",
        default="data/verification",
        help="Root directory for verification records. Default: data/verification.",
    )


def _add_rate_limit_option(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
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


def _add_retry_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--retry-attempts",
        type=int,
        default=DEFAULT_ATTEMPTS,
        help=(
            "Total attempts per provider request, including the first. "
            f"1 disables retries. Default: {DEFAULT_ATTEMPTS}."
        ),
    )
    parser.add_argument(
        "--retry-backoff",
        type=float,
        default=DEFAULT_BACKOFF_SECONDS,
        help=(
            "Seconds to wait before the first retry, doubling thereafter. "
            f"Default: {DEFAULT_BACKOFF_SECONDS}."
        ),
    )
    parser.add_argument(
        "--retry-max-backoff",
        type=float,
        default=DEFAULT_MAX_BACKOFF_SECONDS,
        help=(
            "Upper bound on the wait between retries. "
            f"Default: {DEFAULT_MAX_BACKOFF_SECONDS}."
        ),
    )


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
        "--provider",
        default="dukascopy",
        choices=PROVIDERS,
        help=(
            "Where candles come from: dukascopy over the network, or csv "
            "from local files. Default: dukascopy."
        ),
    )
    download.add_argument(
        "--source",
        help=(
            "Path to a CSV file or a directory of them. Required for "
            "--provider csv, ignored otherwise."
        ),
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
    _add_storage_options(download)
    _add_rate_limit_option(download)
    _add_retry_options(download)
    download.add_argument(
        "--force-unverified",
        action="store_true",
        help=(
            "Run a historical acquisition whose smaller stages are unproven. "
            "Recorded in the dataset provenance so the data carries the fact."
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

    compare = subparsers.add_parser(
        "compare",
        help="Compare two stored datasets against each other, offline.",
        description=(
            "Compare two already-stored datasets covering the same symbol and "
            "timeframe. Reads both, writes to neither, and never decides that "
            "one provider is right: disagreements are reported, not resolved."
        ),
    )

    compare.add_argument(
        "--left",
        required=True,
        help="Root directory of the first Parquet dataset.",
    )
    compare.add_argument(
        "--right",
        required=True,
        help="Root directory of the second Parquet dataset.",
    )
    compare.add_argument(
        "--symbol",
        required=True,
        help="Symbol to compare, for example EUR/USD.",
    )
    compare.add_argument(
        "--timeframe",
        default="1min",
        help="Timeframe to compare. Default: 1min.",
    )
    compare.add_argument(
        "--start",
        type=parse_datetime,
        help="UTC start of the window to compare. Defaults to everything stored.",
    )
    compare.add_argument(
        "--end",
        type=parse_datetime,
        help="UTC end of the window to compare (exclusive).",
    )
    compare.add_argument(
        "--left-manifest-root",
        help=(
            "Manifests describing the first dataset, used to identify which "
            "provider produced it."
        ),
    )
    compare.add_argument(
        "--right-manifest-root",
        help="Manifests describing the second dataset.",
    )
    compare.add_argument(
        "--price-tolerance",
        type=tolerance,
        default=DEFAULT_PRICE_TOLERANCE,
        help=(
            "Relative difference two prices may show and still agree, as a "
            "decimal fraction. Two feeds quoting the same instrument differ by "
            f"about a spread. Default: {DEFAULT_PRICE_TOLERANCE} (~1 pip on "
            "EUR/USD)."
        ),
    )
    compare.add_argument(
        "--volume-tolerance",
        type=tolerance,
        default=DEFAULT_VOLUME_TOLERANCE,
        help=(
            "Relative difference two volumes may show and still agree. FX has "
            "no consolidated tape, so volumes are provider-specific tick "
            f"counts. Default: {DEFAULT_VOLUME_TOLERANCE}."
        ),
    )
    compare.add_argument(
        "--price-mismatch-warn-ratio",
        type=float,
        default=DEFAULT_PRICE_MISMATCH_WARN_RATIO,
        help=(
            "Fraction of compared candles that may disagree on price before "
            f"warning. Default: {DEFAULT_PRICE_MISMATCH_WARN_RATIO}."
        ),
    )
    compare.add_argument(
        "--price-mismatch-fail-ratio",
        type=float,
        default=DEFAULT_PRICE_MISMATCH_FAIL_RATIO,
        help=(
            "Fraction of compared candles that may disagree on price before "
            f"failing. Default: {DEFAULT_PRICE_MISMATCH_FAIL_RATIO}."
        ),
    )
    compare.add_argument(
        "--volume-mismatch-warn-ratio",
        type=float,
        default=DEFAULT_VOLUME_MISMATCH_WARN_RATIO,
        help=(
            "Fraction of compared candles that may disagree on volume before "
            f"warning. Default: {DEFAULT_VOLUME_MISMATCH_WARN_RATIO}."
        ),
    )
    compare.add_argument(
        "--volume-mismatch-fail-ratio",
        type=float,
        default=DEFAULT_VOLUME_MISMATCH_FAIL_RATIO,
        help=(
            "Fraction of compared candles that may disagree on volume before "
            "failing. The default of "
            f"{DEFAULT_VOLUME_MISMATCH_FAIL_RATIO} is unreachable, which is "
            "how 'report volume differences but never fail on them' is said."
        ),
    )
    compare.add_argument(
        "--missing-warn-ratio",
        type=float,
        default=DEFAULT_MISSING_WARN_RATIO,
        help=(
            "Fraction of the combined candles that may be present on only one "
            f"side before warning. Default: {DEFAULT_MISSING_WARN_RATIO}."
        ),
    )
    compare.add_argument(
        "--missing-fail-ratio",
        type=float,
        default=DEFAULT_MISSING_FAIL_RATIO,
        help=(
            "Fraction of the combined candles that may be present on only one "
            f"side before failing. Default: {DEFAULT_MISSING_FAIL_RATIO}."
        ),
    )
    compare.add_argument(
        "--record-root",
        help=(
            "Write a durable comparison record under this directory. The "
            "record expires by itself when either dataset, either provider "
            "configuration, the range or the thresholds change."
        ),
    )
    compare.add_argument(
        "--json",
        action="store_true",
        dest="as_json",
        help="Print the comparison report as JSON instead of a text summary.",
    )

    preflight = subparsers.add_parser(
        "provider-check",
        help="Ask a provider whether it can supply usable data at all.",
    )

    preflight.add_argument(
        "--provider",
        default="dukascopy",
        choices=PROVIDERS,
        help="Provider to question. Default: dukascopy.",
    )
    preflight.add_argument(
        "--source",
        help="CSV file or directory; required for --provider csv.",
    )
    preflight.add_argument("--symbol", required=True, help="Symbol to ask for.")
    preflight.add_argument(
        "--timeframe",
        default="1min",
        help="Timeframe to ask for. Default: 1min.",
    )
    preflight.add_argument(
        "--start",
        type=parse_datetime,
        help="UTC start of the window to sample. Defaults to a recent window.",
    )
    preflight.add_argument(
        "--end",
        type=parse_datetime,
        help="UTC end of the window to sample (exclusive).",
    )
    _add_retry_options(preflight)
    _add_rate_limit_option(preflight)
    preflight.add_argument(
        "--json",
        action="store_true",
        dest="as_json",
        help="Print the preflight report as JSON instead of a text summary.",
    )

    verify = subparsers.add_parser(
        "verify-stage",
        help="Acquire one staged range through the pipeline and verify it.",
    )

    verify.add_argument(
        "--stage",
        required=True,
        choices=[stage.value for stage in Stage],
        help="; ".join(
            f"{stage.value}: {description}"
            for stage, description in STAGE_DESCRIPTIONS.items()
        ),
    )
    verify.add_argument(
        "--provider",
        default="dukascopy",
        choices=PROVIDERS,
        help="Provider to acquire from. Default: dukascopy.",
    )
    verify.add_argument(
        "--source",
        help="CSV file or directory; required for --provider csv.",
    )
    verify.add_argument("--symbol", required=True, help="Symbol to acquire.")
    verify.add_argument(
        "--timeframe",
        default="1min",
        help="Timeframe to acquire. Default: 1min.",
    )
    verify.add_argument(
        "--start",
        required=True,
        type=parse_datetime,
        help="UTC start of the stage window.",
    )
    verify.add_argument(
        "--end",
        type=parse_datetime,
        help=(
            "UTC end, exclusive. Only for --stage historical; the smaller "
            "stages decide their own end."
        ),
    )
    verify.add_argument(
        "--chunk-size",
        default="1month",
        type=chunk_size,
        help="Size of each provider request. Default: 1month.",
    )
    verify.add_argument(
        "--calendar",
        default="forex",
        choices=sorted(CALENDARS),
        help="Trading calendar. Default: forex.",
    )
    verify.add_argument(
        "--missing-warn-ratio",
        type=float,
        default=DEFAULT_MISSING_WARN_RATIO,
        help=(
            "Fraction of expected candles that may be missing before warning. "
            f"Default: {DEFAULT_MISSING_WARN_RATIO}."
        ),
    )
    verify.add_argument(
        "--missing-fail-ratio",
        type=float,
        default=DEFAULT_MISSING_FAIL_RATIO,
        help=(
            "Fraction of expected candles that may be missing before failing. "
            f"Default: {DEFAULT_MISSING_FAIL_RATIO}."
        ),
    )
    verify.add_argument(
        "--force-unverified",
        action="store_true",
        help=(
            "Run a stage whose prerequisites are unproven. Recorded in the "
            "dataset provenance so the data carries the fact."
        ),
    )
    _add_retry_options(verify)
    _add_rate_limit_option(verify)
    _add_storage_options(verify)
    verify.add_argument(
        "--json",
        action="store_true",
        dest="as_json",
        help="Print the verification record as JSON instead of a text summary.",
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
            f"Detected gaps:     {report.missing_interval_count}",
            f"Market closures:   {len(report.market_closed_intervals)}",
            f"Duplicate candles: {report.duplicate_candles}",
            f"Invalid OHLC rows: {report.invalid_rows}",
            f"Out of range rows: {report.out_of_range_rows}",
            f"Unordered rows:    {report.unordered_rows}",
            f"Misfiled rows:     {report.misfiled_rows}",
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

    if report.missing_interval_count > MAX_LISTED_GAPS:
        lines.append(
            f"  ... and {report.missing_interval_count - MAX_LISTED_GAPS} more gaps"
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


def comparison_thresholds_from_args(
    args: argparse.Namespace,
) -> ComparisonThresholds:
    return ComparisonThresholds(
        price_tolerance=args.price_tolerance,
        volume_tolerance=args.volume_tolerance,
        price_mismatch_warn_ratio=args.price_mismatch_warn_ratio,
        price_mismatch_fail_ratio=args.price_mismatch_fail_ratio,
        volume_mismatch_warn_ratio=args.volume_mismatch_warn_ratio,
        volume_mismatch_fail_ratio=args.volume_mismatch_fail_ratio,
        missing_warn_ratio=args.missing_warn_ratio,
        missing_fail_ratio=args.missing_fail_ratio,
    )


def format_comparison(report: ComparisonReport) -> str:
    """Render a human-readable summary of one cross-dataset comparison."""
    lines = [
        f"Symbol:            {report.symbol}",
        f"Timeframe:         {report.timeframe}",
        f"Left dataset:      {report.left.root}",
        f"Left provider:     {report.left.provider} ({report.left.dataset_fingerprint})",
        f"Right dataset:     {report.right.root}",
        (
            f"Right provider:    {report.right.provider} "
            f"({report.right.dataset_fingerprint})"
        ),
        (
            f"Requested range:   {_format_timestamp(report.requested_start)}"
            f" -> {_format_timestamp(report.requested_end)}"
        ),
        (
            f"Left range:        {_format_timestamp(report.left_start)}"
            f" -> {_format_timestamp(report.left_end)}"
        ),
        (
            f"Right range:       {_format_timestamp(report.right_start)}"
            f" -> {_format_timestamp(report.right_end)}"
        ),
        f"Ranges match:      {'yes' if report.ranges_match else 'no'}",
        f"Partitions:        {report.partitions_compared}",
        f"Candles compared:  {report.candles_compared}",
        f"Matching:          {report.matching_candles}",
        f"Mismatching:       {report.mismatching_candles}",
        f"  price:           {report.price_mismatches}",
        f"  volume:          {report.volume_mismatches}",
        f"Missing in left:   {report.missing_from_left}",
        f"Missing in right:  {report.missing_from_right}",
        f"Duplicates left:   {report.duplicate_timestamps_left}",
        f"Duplicates right:  {report.duplicate_timestamps_right}",
        (
            f"Max price diff:    {report.max_price_difference}"
            f" at {_format_timestamp(report.max_price_difference_at)}"
            f" ({report.max_price_relative_difference:.8f} relative)"
        ),
        (
            f"Max volume diff:   {report.max_volume_difference}"
            f" at {_format_timestamp(report.max_volume_difference_at)}"
        ),
        f"Gaps left/right:   {report.gaps_left}/{report.gaps_right}",
        (
            f"Gaps on one side:  {report.gaps_only_left} left only, "
            f"{report.gaps_only_right} right only"
        ),
        f"Thresholds:        {report.thresholds_description}",
    ]

    if not report.volume_available:
        lines.append(
            "NOTE: neither dataset reports a non-zero volume, so the volume "
            "comparison proves nothing."
        )

    if report.problems:
        lines.append("Problems:")
        lines.extend(f"  - {problem}" for problem in report.problems)

    for difference in report.differences[:MAX_LISTED_GAPS]:
        lines.append(
            f"  {difference.timestamp:%Y-%m-%dT%H:%M:%SZ} {difference.field}:"
            f" left {difference.left} vs right {difference.right}"
            f" ({difference.relative_difference} relative)"
        )

    if report.differences_truncated:
        lines.append("  ... further differences omitted")

    for gap in report.gap_differences[:MAX_LISTED_GAPS]:
        lines.append(
            f"  gap in {gap.side} only {gap.start:%Y-%m-%dT%H:%M:%SZ}"
            f" -> {gap.end:%Y-%m-%dT%H:%M:%SZ}"
            f" ({gap.missing_candles} candles)"
        )

    lines.append(f"Verdict:           {report.status.value.upper()}")

    if report.blocked:
        lines.append(
            "BLOCKED is not agreement and not a failure: there was nothing to "
            "compare, so nothing has been verified."
        )

    return "\n".join(lines)


def run_compare(args: argparse.Namespace) -> int:
    thresholds = comparison_thresholds_from_args(args)

    report = compare_datasets(
        symbol=args.symbol,
        timeframe=args.timeframe,
        left_root=args.left,
        right_root=args.right,
        left_manifest_root=args.left_manifest_root,
        right_manifest_root=args.right_manifest_root,
        start=args.start,
        end=args.end,
        thresholds=thresholds,
    )

    record_path = None

    if args.record_root:
        record_path = ComparisonStore(args.record_root).save(
            build_comparison_record(report, thresholds)
        )

    if args.as_json:
        print(report.model_dump_json(indent=2))
    else:
        print(format_comparison(report))

        if record_path is not None:
            print(f"Record:            {record_path}")

    return 0 if report.verified else 1


def format_preflight(report: PreflightReport, *, live: bool) -> str:
    """Render a human-readable preflight summary."""
    lines = [
        f"Provider:          {report.provider}",
        f"Symbol:            {report.symbol}",
        f"Timeframe:         {report.timeframe}",
        (
            f"Sampled range:     {_format_timestamp(report.start)}"
            f" -> {_format_timestamp(report.end)}"
        ),
        f"Candles:           {report.candles}",
        "Checks:",
    ]

    lines.extend(
        f"  {check.status.value.upper():<8} {check.name}: {check.detail}"
        for check in report.checks
    )

    lines.append(f"Result:            {report.status.value.upper()}")

    if report.blocked:
        lines.append(
            "The provider could not be reached, which is not the same as the "
            "provider being wrong. Nothing has been verified."
        )

    if not live:
        lines.append(
            "NOTE: this ran against a stub transport, so it proves the client "
            "code and not the provider."
        )

    return "\n".join(lines)


def run_provider_check(
    args: argparse.Namespace,
    *,
    provider_factory: ProviderFactory | None = None,
) -> int:
    live = provider_factory is None
    provider = (provider_factory or (lambda: provider_from_args(args)))()

    try:
        report = check_provider(
            provider,
            symbol=args.symbol,
            timeframe=args.timeframe,
            start=args.start,
            end=args.end,
        )
    finally:
        close = getattr(provider, "close", None)

        if callable(close):
            close()

    if args.as_json:
        print(report.model_dump_json(indent=2))
    else:
        print(format_preflight(report, live=live))

    return 0 if report.passed else 1


def format_stage(outcome: StageOutcome) -> str:
    """Render a human-readable summary of one staged verification."""
    record = outcome.record
    validation = outcome.validation
    result = outcome.result

    lines = [
        f"Provider:          {result.provider if result else '-'}",
        f"Symbol:            {record.symbol if record else '-'}",
        f"Timeframe:         {record.timeframe if record else '-'}",
        f"Stage:             {outcome.stage.value}",
    ]

    if result is not None:
        lines.extend(
            [
                (
                    f"Requested range:   "
                    f"{_format_timestamp(result.requested_start)}"
                    f" -> {_format_timestamp(result.requested_end)}"
                ),
                (
                    f"Actual range:      {_format_timestamp(result.actual_start)}"
                    f" -> {_format_timestamp(result.actual_end)}"
                ),
                f"Rows:              {result.retained_count}",
                f"Quality status:    {result.quality.status.value}",
            ]
        )

    if validation is not None:
        lines.extend(
            [
                f"Expected rows:     {validation.expected_candles}",
                f"Missing candles:   {validation.missing_candles}",
                f"Market closures:   {len(validation.market_closed_intervals)}",
                f"Validation status: {validation.status.value}",
            ]
        )

    lines.append(f"Prerequisites:     {outcome.guard.reason}")
    lines.append(f"Verification:      {outcome.status.value.upper()}")

    if outcome.record_path is not None:
        lines.append(f"Record:            {outcome.record_path}")

    if outcome.problems:
        lines.append("Problems:")
        lines.extend(f"  - {problem}" for problem in outcome.problems)

    if outcome.status is VerificationStatus.BLOCKED:
        lines.append(
            "BLOCKED is not a failure of the data and not a verification. "
            "Nothing has been proven."
        )

    return "\n".join(lines)


def thresholds_from_args(args: argparse.Namespace) -> QualityThresholds:
    return QualityThresholds(
        missing_warn_ratio=args.missing_warn_ratio,
        missing_fail_ratio=args.missing_fail_ratio,
    )


def run_verify_stage(
    args: argparse.Namespace,
    *,
    provider_factory: ProviderFactory | None = None,
) -> int:
    live = provider_factory is None
    provider = (provider_factory or (lambda: provider_from_args(args)))()

    try:
        outcome = run_stage(
            provider,
            stage=Stage(args.stage),
            symbol=args.symbol,
            start=args.start,
            end=args.end,
            timeframe=args.timeframe,
            data_root=args.data_root,
            manifest_root=args.manifest_root,
            quality_root=args.quality_root,
            checkpoint_root=args.checkpoint_root,
            verification_root=args.verification_root,
            calendar=get_calendar(args.calendar),
            chunk_size=args.chunk_size,
            thresholds=thresholds_from_args(args),
            live=live,
            override=args.force_unverified,
        )
    finally:
        close = getattr(provider, "close", None)

        if callable(close):
            close()

    if args.as_json and outcome.record is not None:
        print(outcome.record.model_dump_json(indent=2))
    elif args.as_json:
        print(
            json.dumps(
                {
                    "stage": outcome.stage.value,
                    "status": outcome.status.value,
                    "problems": outcome.problems,
                },
                indent=2,
            )
        )
    else:
        print(format_stage(outcome))

    return 0 if outcome.verified else 1


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


def provider_from_args(args: argparse.Namespace) -> MarketDataProvider:
    """Build the provider the request asked for."""
    if args.provider == "csv":
        if not args.source:
            raise ValueError("--source is required when --provider csv")

        return CsvMarketDataProvider(args.source)

    return DukascopyProvider(
        retry_policy=retry_policy_from_args(args),
        rate_limit=rate_limit_from_args(args),
    )


def run_download(
    args: argparse.Namespace,
    *,
    provider_factory: ProviderFactory | None = None,
) -> int:
    if provider_factory is None:

        def provider_factory() -> MarketDataProvider:
            return provider_from_args(args)

    provider = provider_factory()

    guard = check_download(
        VerificationStore(args.verification_root),
        provider=provider.name,
        configuration=provider.configuration(),
        symbol=args.symbol,
        timeframe=args.timeframe,
        start=args.start,
        end=args.end,
        override=args.force_unverified,
    )

    if not guard.allowed:
        close = getattr(provider, "close", None)

        if callable(close):
            close()

        print(f"refused: {guard.reason}")
        print(
            "Verify the smaller stages first, for example:\n"
            f"  marketdata verify-stage --stage smoke --symbol {args.symbol}"
            f" --start {args.start:%Y-%m-%dT%H:%M:%SZ}\n"
            "or pass --force-unverified to proceed anyway, which is recorded "
            "in the dataset provenance."
        )

        return 2

    if guard.overridden:
        print(f"WARNING: {guard.reason}")

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
            verification=guard.reason,
            unverified_override=guard.overridden,
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

        if args.command == "compare":
            return run_compare(args)

        if args.command == "provider-check":
            return run_provider_check(args, provider_factory=provider_factory)

        if args.command == "verify-stage":
            return run_verify_stage(args, provider_factory=provider_factory)
    except (ProviderError, ValueError) as exc:
        print(f"error: {exc}")
        return 1

    parser.error(f"unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
