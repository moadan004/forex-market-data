from __future__ import annotations

from datetime import UTC, datetime, timedelta

from pydantic import BaseModel

from marketdata.models.candle import Candle
from marketdata.models.timeframe import timeframe_cadence
from marketdata.providers.base import MarketDataProvider
from marketdata.providers.errors import (
    ProviderAuthError,
    ProviderError,
    TransientProviderError,
)
from marketdata.validation.candles import partition_candles
from marketdata.verification.status import VerificationStatus, worst


class CheckResult(BaseModel):
    """One preflight question and what the provider answered."""

    model_config = {"frozen": True}

    name: str
    status: VerificationStatus
    detail: str


class PreflightReport(BaseModel):
    """
    Whether a provider can supply usable data at all.

    Answered by making real requests. A report produced against a mock
    transport proves the client code, never the provider — the caller is
    responsible for not confusing the two, and the CLI says which it did.
    """

    provider: str
    provider_configuration: dict[str, str]
    symbol: str
    timeframe: str
    start: datetime
    end: datetime
    candles: int
    checks: list[CheckResult]
    status: VerificationStatus
    checked_at: datetime

    @property
    def passed(self) -> bool:
        return self.status.verified

    @property
    def blocked(self) -> bool:
        return self.status is VerificationStatus.BLOCKED


def classify_failure(error: ProviderError) -> tuple[VerificationStatus, str]:
    """
    Decide what a provider failure says about the provider.

    Being refused or unable to connect says nothing about whether the data
    would be good, so it blocks rather than fails. Data we did receive and
    could not use is a genuine failure.
    """
    if isinstance(error, ProviderAuthError):
        return (
            VerificationStatus.BLOCKED,
            f"refused by policy or credentials: {error}",
        )

    if isinstance(error, TransientProviderError):
        return (
            VerificationStatus.BLOCKED,
            f"unreachable after retries: {error}",
        )

    return VerificationStatus.FAIL, str(error)


def default_window(timeframe: str, end: datetime | None = None) -> tuple[datetime, ...]:
    """
    Return a small recent window worth asking any provider for.

    Sized to a handful of candles at the requested cadence, and pushed back
    from the present so a provider that publishes with a delay is not
    accused of returning nothing.
    """
    cadence = timeframe_cadence(timeframe) or timedelta(minutes=1)
    finish = (end or datetime.now(UTC)) - cadence * 10

    return finish - cadence * 60, finish


def _why_unreachable(provider: MarketDataProvider) -> str:
    """
    Ask a failing provider for the reason behind a bare "not reachable".

    ``health_check`` answers yes or no by contract, so the cause of a no is
    lost unless it is asked for. The reason is what a reader needs: an
    egress policy denial and a wrong endpoint look identical otherwise.
    """
    try:
        provider.get_supported_symbols()
    except ProviderError as exc:
        return classify_failure(exc)[1]

    return "provider did not answer"


def check_provider(
    provider: MarketDataProvider,
    *,
    symbol: str,
    timeframe: str = "1min",
    start: datetime | None = None,
    end: datetime | None = None,
) -> PreflightReport:
    """
    Ask a provider everything that must hold before trusting it with a range.

    Each question is answered in order and recorded; a question that cannot
    be answered because an earlier one failed is skipped rather than guessed
    at.
    """
    if (start is None) != (end is None):
        raise ValueError("supply both start and end, or neither")

    if start is None or end is None:
        start, end = default_window(timeframe)

    if start >= end:
        raise ValueError("start must be before end")

    checks: list[CheckResult] = []
    candles: list[Candle] = []

    def record(name: str, status: VerificationStatus, detail: str) -> None:
        checks.append(CheckResult(name=name, status=status, detail=detail))

    # 1. Reachability. A provider that cannot be reached is always BLOCKED
    #    rather than failed — nothing has been learned about its data — but
    #    the reason matters, and health_check answers only yes or no. When it
    #    says no, ask again in a way that surfaces the cause.
    try:
        reachable = provider.health_check()
    except ProviderError as exc:
        _, detail = classify_failure(exc)
        record("reachable", VerificationStatus.BLOCKED, detail)
        reachable = False
    else:
        if reachable:
            record("reachable", VerificationStatus.PASS, "provider answered")
        else:
            record("reachable", VerificationStatus.BLOCKED, _why_unreachable(provider))

    if not reachable:
        return _finish(provider, symbol, timeframe, start, end, candles, checks)

    # 2. The symbol is one the provider serves.
    try:
        supported = list(provider.get_supported_symbols())
    except ProviderError as exc:
        status, detail = classify_failure(exc)
        record("symbol_accepted", status, detail)

        return _finish(provider, symbol, timeframe, start, end, candles, checks)

    normalized = symbol.strip().upper()

    if not supported:
        record(
            "symbol_accepted",
            VerificationStatus.WARN,
            "provider advertises no symbol list; cannot confirm before fetching",
        )
    elif normalized in {value.strip().upper() for value in supported}:
        record("symbol_accepted", VerificationStatus.PASS, f"{normalized} is offered")
    else:
        record(
            "symbol_accepted",
            VerificationStatus.FAIL,
            f"{normalized} is not among {len(supported)} offered symbols",
        )

        return _finish(provider, symbol, timeframe, start, end, candles, checks)

    # 3. The timeframe is accepted, and 4. candles come back.
    try:
        candles = provider.fetch_candles(symbol, start, end, timeframe)
    except ProviderError as exc:
        status, detail = classify_failure(exc)
        record("timeframe_accepted", status, detail)
        record("candles_returned", status, "no response to inspect")

        return _finish(provider, symbol, timeframe, start, end, candles, checks)
    except ValueError as exc:
        record("timeframe_accepted", VerificationStatus.FAIL, str(exc))
        record("candles_returned", VerificationStatus.FAIL, "no response to inspect")

        return _finish(provider, symbol, timeframe, start, end, candles, checks)

    record("timeframe_accepted", VerificationStatus.PASS, f"{timeframe} accepted")

    if candles:
        record("candles_returned", VerificationStatus.PASS, f"{len(candles)} candles")
    else:
        record(
            "candles_returned",
            VerificationStatus.FAIL,
            "no candles for the requested window",
        )

        return _finish(provider, symbol, timeframe, start, end, candles, checks)

    # 5. The rows are canonical candles. Reaching here already proves it —
    #    a provider builds Candle instances, and a payload that could not be
    #    parsed would have raised above — so this records the evidence.
    record(
        "candles_parsed",
        VerificationStatus.PASS
        if all(isinstance(candle, Candle) for candle in candles)
        else VerificationStatus.FAIL,
        "rows are canonical Candle instances",
    )

    # 6. Timestamps are UTC, ordered, and inside the requested window.
    record(*_check_timestamps(candles, start, end))

    # 7. OHLC invariants hold.
    _, violations = partition_candles(candles)

    if violations:
        record(
            "ohlc_valid",
            VerificationStatus.FAIL,
            f"{len(violations)} rows break an invariant, first: {violations[0].reason}",
        )
    else:
        record("ohlc_valid", VerificationStatus.PASS, "every row satisfies the OHLC")

    # 8. Pagination stitches adjacent windows without loss or overlap.
    record(*_check_pagination(provider, symbol, timeframe, start, end, candles))

    return _finish(provider, symbol, timeframe, start, end, candles, checks)


def _check_timestamps(
    candles: list[Candle],
    start: datetime,
    end: datetime,
) -> tuple[str, VerificationStatus, str]:
    stamps = [candle.timestamp for candle in candles]

    naive = [stamp for stamp in stamps if stamp.tzinfo is None]

    if naive:
        return ("timestamps_utc", VerificationStatus.FAIL, f"{len(naive)} are naive")

    offset = [stamp for stamp in stamps if stamp.utcoffset() != timedelta(0)]

    if offset:
        return (
            "timestamps_utc",
            VerificationStatus.FAIL,
            f"{len(offset)} are not UTC",
        )

    outside = [stamp for stamp in stamps if not start <= stamp < end]

    if outside:
        return (
            "timestamps_utc",
            VerificationStatus.FAIL,
            f"{len(outside)} fall outside the requested window",
        )

    if stamps != sorted(stamps):
        return (
            "timestamps_utc",
            VerificationStatus.FAIL,
            "timestamps are not chronological",
        )

    return ("timestamps_utc", VerificationStatus.PASS, "UTC, ordered and in range")


def _check_pagination(
    provider: MarketDataProvider,
    symbol: str,
    timeframe: str,
    start: datetime,
    end: datetime,
    whole: list[Candle],
) -> tuple[str, VerificationStatus, str]:
    """
    Ask for the same range in two halves and compare with the whole.

    Whatever paging a provider does internally, the boundary is where rows
    get lost or repeated. Splitting the window is the cheapest way to catch
    that, and it works for any provider rather than assuming one's scheme.
    """
    middle = start + (end - start) / 2

    if middle <= start or middle >= end:
        return ("pagination", VerificationStatus.WARN, "window too small to split")

    try:
        first = provider.fetch_candles(symbol, start, middle, timeframe)
        second = provider.fetch_candles(symbol, middle, end, timeframe)
    except ProviderError as exc:
        status, detail = classify_failure(exc)

        return ("pagination", status, detail)

    stitched = [candle.timestamp for candle in first] + [
        candle.timestamp for candle in second
    ]
    expected = [candle.timestamp for candle in whole]

    if sorted(stitched) != sorted(expected):
        missing = len(set(expected) - set(stitched))
        extra = len(set(stitched) - set(expected))

        return (
            "pagination",
            VerificationStatus.FAIL,
            (
                f"split window disagrees with the whole: {missing} missing, "
                f"{extra} unexpected"
            ),
        )

    if len(set(stitched)) != len(stitched):
        return (
            "pagination",
            VerificationStatus.FAIL,
            "the halves overlap: a row was returned twice",
        )

    return (
        "pagination",
        VerificationStatus.PASS,
        f"{len(first)} + {len(second)} rows stitch to {len(expected)}",
    )


def _finish(
    provider: MarketDataProvider,
    symbol: str,
    timeframe: str,
    start: datetime,
    end: datetime,
    candles: list[Candle],
    checks: list[CheckResult],
) -> PreflightReport:
    return PreflightReport(
        provider=provider.name,
        provider_configuration=provider.configuration(),
        symbol=symbol.strip().upper(),
        timeframe=timeframe,
        start=start,
        end=end,
        candles=len(candles),
        checks=checks,
        status=worst(check.status for check in checks),
        checked_at=datetime.now(UTC),
    )


__all__ = [
    "CheckResult",
    "PreflightReport",
    "check_provider",
    "classify_failure",
    "default_window",
]
