from __future__ import annotations

import httpx

RETRY_AFTER_HEADER = "Retry-After"

TRANSIENT_STATUS_CODES = frozenset({408, 425, 429})
"""Client-side status codes worth retrying.

408 Request Timeout and 425 Too Early describe a request that can simply be
sent again; 429 Too Many Requests asks us to slow down rather than stop.
"""


class ProviderError(RuntimeError):
    """Base class for every market-data provider failure."""

    retryable = False


class TransientProviderError(ProviderError):
    """A failure that may succeed if the request is sent again."""

    retryable = True

    def __init__(self, message: str, *, retry_after: float | None = None) -> None:
        super().__init__(message)
        self.retry_after = retry_after


class ProviderTimeoutError(TransientProviderError):
    """The provider did not respond in time."""


class ProviderConnectionError(TransientProviderError):
    """The connection to the provider failed or was cut."""


class ProviderRateLimitError(TransientProviderError):
    """The provider asked us to slow down."""


class ProviderServerError(TransientProviderError):
    """The provider failed on its own side."""


class PermanentProviderError(ProviderError):
    """A failure that will keep failing until the request itself changes."""


class ProviderAuthError(PermanentProviderError):
    """The request was rejected as unauthenticated or forbidden."""


class ProviderClientError(PermanentProviderError):
    """The request was rejected as malformed or unsatisfiable."""


class ProviderDataError(PermanentProviderError):
    """The provider answered, but not with data we can use."""


def _parse_retry_after(response: httpx.Response) -> float | None:
    """Return the Retry-After delay in seconds when the provider sent one."""
    raw = response.headers.get(RETRY_AFTER_HEADER)

    if raw is None:
        return None

    try:
        seconds = float(raw.strip())
    except ValueError:
        # The header also allows an HTTP date. Backoff already bounds the
        # wait, so an unparseable value is simply ignored.
        return None

    return seconds if seconds >= 0 else None


def classify_status_error(
    error: httpx.HTTPStatusError,
    *,
    context: str,
) -> ProviderError:
    """Classify an HTTP status response as transient or permanent."""
    status = error.response.status_code
    detail = f"{context}: HTTP {status}"

    if status == 429:
        return ProviderRateLimitError(
            f"{detail} (rate limited)",
            retry_after=_parse_retry_after(error.response),
        )

    if status in TRANSIENT_STATUS_CODES:
        return ProviderTimeoutError(detail)

    if status >= 500:
        return ProviderServerError(detail)

    if status in {401, 403}:
        return ProviderAuthError(f"{detail} (not authorized)")

    if status >= 400:
        return ProviderClientError(detail)

    # raise_for_status only raises on 4xx and 5xx, so anything here is a
    # status we do not understand; do not hammer the provider over it.
    return ProviderClientError(detail)


def classify_http_error(error: httpx.HTTPError, *, context: str) -> ProviderError:
    """
    Classify an httpx failure as transient or permanent.

    Only failures that can plausibly succeed on a second attempt are
    transient. Anything caused by the request itself, by credentials, or by
    a policy decision is permanent: retrying it wastes time and hammers the
    provider without changing the outcome.
    """
    if isinstance(error, httpx.HTTPStatusError):
        return classify_status_error(error, context=context)

    if isinstance(error, httpx.TimeoutException):
        return ProviderTimeoutError(f"{context}: {error!s} (timeout)")

    if isinstance(error, httpx.ProxyError):
        # A proxy refusing the tunnel is an egress policy decision, not a
        # flaky network. Retrying cannot change the verdict.
        return ProviderAuthError(f"{context}: {error!s} (proxy rejected)")

    if isinstance(error, httpx.UnsupportedProtocol | httpx.LocalProtocolError):
        return ProviderClientError(f"{context}: {error!s}")

    if isinstance(error, httpx.NetworkError | httpx.RemoteProtocolError):
        return ProviderConnectionError(f"{context}: {error!s}")

    return ProviderClientError(f"{context}: {error!s}")


__all__ = [
    "PermanentProviderError",
    "ProviderAuthError",
    "ProviderClientError",
    "ProviderConnectionError",
    "ProviderDataError",
    "ProviderError",
    "ProviderRateLimitError",
    "ProviderServerError",
    "ProviderTimeoutError",
    "TransientProviderError",
    "classify_http_error",
    "classify_status_error",
]
