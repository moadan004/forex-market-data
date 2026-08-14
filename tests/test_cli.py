import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from marketdata.cli import build_parser, main, parse_datetime
from marketdata.models.candle import Candle
from marketdata.providers.base import MarketDataProvider

START = datetime(2026, 8, 14, 12, 0, tzinfo=UTC)
MINUTE = timedelta(minutes=1)


class StubProvider(MarketDataProvider):
    closed = False

    @property
    def name(self) -> str:
        return "stub"

    def get_supported_symbols(self) -> list[str]:
        return ["EUR/USD"]

    def fetch_candles(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        timeframe: str = "1min",
    ) -> list[Candle]:
        return [
            Candle(
                timestamp=start + MINUTE * index,
                symbol=symbol,
                open=Decimal("1.1700"),
                high=Decimal("1.1710"),
                low=Decimal("1.1690"),
                close=Decimal("1.1705"),
                volume=Decimal(100),
            )
            for index in range(60)
        ]

    def health_check(self) -> bool:
        return True

    def close(self) -> None:
        type(self).closed = True


def download_argv(tmp_path, *extra):
    return [
        "download",
        "--symbol",
        "EUR/USD",
        "--start",
        "2026-08-14T12:00:00Z",
        "--end",
        "2026-08-14T13:00:00Z",
        "--output-root",
        str(tmp_path / "processed"),
        "--manifest-root",
        str(tmp_path / "manifests"),
        "--quality-root",
        str(tmp_path / "quality"),
        *extra,
    ]


def test_parse_datetime_accepts_zulu_suffix():
    assert parse_datetime("2026-08-14T12:00:00Z") == START


def test_parse_datetime_converts_to_utc():
    assert parse_datetime("2026-08-14T15:00:00+03:00") == START


def test_parse_datetime_rejects_naive_input():
    with pytest.raises(Exception, match="timezone"):
        parse_datetime("2026-08-14T12:00:00")


def test_parse_datetime_rejects_garbage():
    with pytest.raises(Exception, match="invalid ISO-8601"):
        parse_datetime("not-a-date")


def test_parser_defaults_to_one_minute_candles():
    args = build_parser().parse_args(
        [
            "download",
            "--symbol",
            "EUR/USD",
            "--start",
            "2026-08-14T12:00:00Z",
            "--end",
            "2026-08-14T13:00:00Z",
        ]
    )

    assert args.timeframe == "1min"
    assert args.output_root == "data/processed"
    assert args.strict is False


def test_download_prints_a_full_summary(tmp_path, capsys):
    exit_code = main(download_argv(tmp_path), provider_factory=StubProvider)

    output = capsys.readouterr().out

    assert exit_code == 0
    assert "Provider:          stub" in output
    assert "Symbol:            EUR/USD" in output
    assert "Timeframe:         1min" in output
    assert "Requested range:   2026-08-14T12:00:00Z -> 2026-08-14T13:00:00Z" in output
    assert "Actual range:      2026-08-14T12:00:00Z -> 2026-08-14T12:59:00Z" in output
    assert "Rows downloaded:   60" in output
    assert "Rows retained:     60" in output
    assert "Duplicates:        0" in output
    assert "Quality status:    ok" in output
    assert "Parquet files:     1" in output
    assert "Manifest:" in output
    assert "Quality report:" in output


def test_download_writes_the_expected_artifacts(tmp_path):
    main(download_argv(tmp_path), provider_factory=StubProvider)

    parquet = list((tmp_path / "processed").rglob("*.parquet"))
    manifests = list((tmp_path / "manifests").rglob("*.json"))
    reports = list((tmp_path / "quality").rglob("*.json"))

    assert len(parquet) == 1
    assert len(manifests) == 1
    assert len(reports) == 1


def test_download_json_output_is_the_quality_report(tmp_path, capsys):
    exit_code = main(download_argv(tmp_path, "--json"), provider_factory=StubProvider)

    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["symbol"] == "EUR/USD"
    assert payload["status"] == "ok"
    assert payload["final_rows"] == 60


def test_download_closes_the_provider(tmp_path):
    StubProvider.closed = False

    main(download_argv(tmp_path), provider_factory=StubProvider)

    assert StubProvider.closed is True


def test_download_reports_errors_without_a_traceback(tmp_path, capsys):
    class FailingProvider(StubProvider):
        def fetch_candles(self, symbol, start, end, timeframe="1min"):
            raise ValueError("boom")

    exit_code = main(download_argv(tmp_path), provider_factory=FailingProvider)

    assert exit_code == 1
    assert "error: boom" in capsys.readouterr().out
