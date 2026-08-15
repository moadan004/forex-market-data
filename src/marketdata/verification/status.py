from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

DEFAULT_MISSING_WARN_RATIO = 0.0
DEFAULT_MISSING_FAIL_RATIO = 0.01


class VerificationStatus(StrEnum):
    """
    Outcome of a check, a stage, or a whole verification run.

    ``BLOCKED`` is deliberately distinct from ``FAIL``. A provider we cannot
    reach tells us nothing about whether the provider or our code is
    correct, and recording it as a failure would be a false accusation and
    would hide the fact that the work is simply not done yet.
    """

    PASS = "pass"
    WARN = "warn"
    FAIL = "fail"
    BLOCKED = "blocked"

    @property
    def verified(self) -> bool:
        """Whether this outcome counts as having proven something."""
        return self in (VerificationStatus.PASS, VerificationStatus.WARN)


SEVERITY: dict[VerificationStatus, int] = {
    VerificationStatus.PASS: 0,
    VerificationStatus.WARN: 1,
    VerificationStatus.BLOCKED: 2,
    VerificationStatus.FAIL: 3,
}
"""How statuses compare when several checks are combined.

``FAIL`` outranks ``BLOCKED``: if we managed to obtain data and it was
structurally wrong, that finding stands regardless of what else could not
be reached.
"""


def worst(statuses) -> VerificationStatus:
    """Return the most severe status, or PASS when there are none."""
    collected = list(statuses)

    if not collected:
        return VerificationStatus.PASS

    return max(collected, key=lambda status: SEVERITY[status])


@dataclass(frozen=True)
class QualityThresholds:
    """
    How much imperfection a staged acquisition tolerates.

    **Structural defects have no tolerance.** Invalid OHLC rows, duplicate
    timestamps, rows outside the requested range, an unreadable file, a
    schema that does not match, or a manifest that disagrees with what is
    stored all indicate a bug in this pipeline or a corrupted file — not a
    property of the market. Any occurrence is a FAIL.

    **Missing candles are tolerated within a ratio.** A minute in which no
    tick arrives produces no candle, and spot FX genuinely has such minutes:
    thin liquidity around the daily rollover, and holidays that no calendar
    here lists. Treating any gap as a failure would reject data that is
    simply what the market did.

    The default ratios are a **starting policy, not an empirical finding**.
    No real Dukascopy data has ever been observed by this project, so the
    tolerable gap rate for this provider and instrument is unknown. 1% of
    expected candles is chosen as a conservative opening position: large
    enough to absorb ordinary thin-liquidity minutes, small enough that a
    systematically broken download cannot pass. Recalibrate from the first
    real monthly acquisition and record the reasoning when doing so.
    """

    missing_warn_ratio: float = DEFAULT_MISSING_WARN_RATIO
    missing_fail_ratio: float = DEFAULT_MISSING_FAIL_RATIO

    def __post_init__(self) -> None:
        for name in ("missing_warn_ratio", "missing_fail_ratio"):
            value = getattr(self, name)

            if not 0 <= value <= 1:
                raise ValueError(f"{name} must be between 0 and 1")

        if self.missing_warn_ratio > self.missing_fail_ratio:
            raise ValueError(
                "missing_warn_ratio cannot exceed missing_fail_ratio: "
                "a run cannot fail before it warns"
            )

    def grade_missing(self, missing: int, expected: int | None) -> VerificationStatus:
        """Grade the shortfall of a dataset against its expected candles."""
        if missing <= 0:
            return VerificationStatus.PASS

        if not expected:
            # Candles are missing but nothing said how many to expect, so the
            # shortfall cannot be put in proportion. Do not wave it through.
            return VerificationStatus.FAIL

        ratio = missing / expected

        if ratio > self.missing_fail_ratio:
            return VerificationStatus.FAIL

        if ratio > self.missing_warn_ratio:
            return VerificationStatus.WARN

        return VerificationStatus.PASS

    def describe(self) -> str:
        return (
            f"missing candles warn above {self.missing_warn_ratio:.2%}, "
            f"fail above {self.missing_fail_ratio:.2%}; "
            "structural defects always fail"
        )


__all__ = [
    "DEFAULT_MISSING_FAIL_RATIO",
    "DEFAULT_MISSING_WARN_RATIO",
    "SEVERITY",
    "QualityThresholds",
    "VerificationStatus",
    "worst",
]
