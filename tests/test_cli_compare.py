"""The ``marketdata compare`` command."""

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from marketdata.cli import main
from marketdata.models.candle import Candle
from marketdata.storage.manifest import (
    DatasetProvenance,
    create_manifest,
    write_manifest,
)
from marketdata.storage.parquet import ParquetStorage

SYMBOL = "EUR_USD"
MONDAY = datetime(2026, 8, 10, tzinfo=UTC)


def series(count: int, *, price: str = "1.10000000", volume: str = "10"):
    value = Decimal(price)

    return [
        Candle(
            timestamp=MONDAY + timedelta(minutes=index),
            symbol=SYMBOL,
            open=value,
            high=value + Decimal("0.00050000"),
            low=value - Decimal("0.00050000"),
            close=value,
            volume=Decimal(volume),
        )
        for index in range(count)
    ]


def store(root, candles):
    ParquetStorage(root).write(candles, symbol=SYMBOL, timeframe="1min")


def write_provenance(manifest_root, *, provider: str):
    """Record who produced a dataset, the way the pipeline does."""
    manifest = create_manifest(
        symbol=SYMBOL,
        timeframe="1min",
        candles_count=0,
        start=MONDAY,
        end=MONDAY + timedelta(hours=1),
        provider=provider,
        files=[],
        provenance=DatasetProvenance(
            provider=provider,
            provider_configuration={},
            symbol=SYMBOL,
            timeframe="1min",
            requested_start=MONDAY,
            requested_end=MONDAY + timedelta(hours=1),
            actual_start=MONDAY,
            actual_end=MONDAY,
            rows=0,
            quality_status="ok",
            calendar="forex",
            chunk_size="1month",
            rate_limit=None,
            retry=None,
            acquired_at=MONDAY,
        ),
    )

    return write_manifest(manifest, manifest_root / SYMBOL / f"{provider}.json")


def compare_argv(tmp_path, *extra):
    return [
        "compare",
        "--left",
        str(tmp_path / "left"),
        "--right",
        str(tmp_path / "right"),
        "--symbol",
        "EUR/USD",
        "--timeframe",
        "1min",
        *extra,
    ]


@pytest.fixture
def agreeing(tmp_path):
    candles = series(60)
    store(tmp_path / "left", candles)
    store(tmp_path / "right", candles)

    return tmp_path


@pytest.fixture
def disagreeing(tmp_path):
    store(tmp_path / "left", series(60, price="1.10000000"))
    store(tmp_path / "right", series(60, price="1.20000000"))

    return tmp_path


class TestHumanOutput:
    def test_agreement_exits_zero_and_says_so(self, agreeing, capsys):
        code = main(compare_argv(agreeing))
        output = capsys.readouterr().out

        assert code == 0
        assert "Verdict:           PASS" in output
        assert "Candles compared:  60" in output
        assert "Matching:          60" in output
        assert "Mismatching:       0" in output

    def test_the_summary_names_both_datasets(self, agreeing, capsys):
        main(compare_argv(agreeing))
        output = capsys.readouterr().out

        assert str(agreeing / "left") in output
        assert str(agreeing / "right") in output
        assert "Left provider:" in output
        assert "Right provider:" in output

    def test_the_summary_reports_ranges_and_thresholds(self, agreeing, capsys):
        main(compare_argv(agreeing))
        output = capsys.readouterr().out

        assert "2026-08-10T00:00:00Z" in output
        assert "prices agree within 0.0001 relative" in output

    def test_disagreement_exits_nonzero(self, disagreeing, capsys):
        code = main(compare_argv(disagreeing))
        output = capsys.readouterr().out

        assert code == 1
        assert "Verdict:           FAIL" in output
        assert "Max price diff:    0.10000000" in output
        assert "disagree on price" in output

    def test_missing_candles_are_reported(self, tmp_path, capsys):
        store(tmp_path / "left", series(120))
        store(tmp_path / "right", series(60))

        code = main(compare_argv(tmp_path))
        output = capsys.readouterr().out

        assert code == 1
        assert "Missing in right:  60" in output
        assert "Missing in left:   0" in output

    def test_nothing_to_compare_is_blocked_not_passed(self, tmp_path, capsys):
        code = main(compare_argv(tmp_path))
        output = capsys.readouterr().out

        assert code == 1
        assert "Verdict:           BLOCKED" in output
        assert "nothing has been verified" in output

    def test_a_corrupt_dataset_reports_rather_than_crashes(self, agreeing, capsys):
        path = ParquetStorage(agreeing / "right").partition_files(
            symbol=SYMBOL, timeframe="1min"
        )[0]
        path.write_bytes(b"not parquet")

        code = main(compare_argv(agreeing))
        output = capsys.readouterr().out

        assert code == 1
        assert "Verdict:           FAIL" in output
        assert "unreadable" in output


class TestJsonOutput:
    def test_json_is_machine_readable(self, agreeing, capsys):
        code = main(compare_argv(agreeing, "--json"))
        payload = json.loads(capsys.readouterr().out)

        assert code == 0
        assert payload["status"] == "pass"
        assert payload["candles_compared"] == 60
        assert payload["symbol"] == "EUR/USD"
        assert payload["left"]["side"] == "left"
        assert payload["right"]["side"] == "right"

    def test_json_carries_the_thresholds_that_were_applied(self, agreeing, capsys):
        main(compare_argv(agreeing, "--json", "--price-tolerance", "0.02"))
        payload = json.loads(capsys.readouterr().out)

        assert payload["thresholds"]["price_tolerance"] == "0.02"

    def test_json_locates_each_difference(self, disagreeing, capsys):
        main(compare_argv(disagreeing, "--json"))
        payload = json.loads(capsys.readouterr().out)

        assert payload["status"] == "fail"
        assert payload["differences"]
        assert payload["differences"][0]["field"] in {"open", "high", "low", "close"}


class TestOptions:
    def test_a_wider_tolerance_accepts_the_difference(self, disagreeing, capsys):
        code = main(compare_argv(disagreeing, "--price-tolerance", "0.5"))

        assert code == 0
        assert "Verdict:           PASS" in capsys.readouterr().out

    def test_a_narrower_tolerance_rejects_agreement(self, tmp_path, capsys):
        store(tmp_path / "left", series(60, price="1.10000000"))
        store(tmp_path / "right", series(60, price="1.10005000"))

        assert main(compare_argv(tmp_path)) == 0

        code = main(compare_argv(tmp_path, "--price-tolerance", "0.0000001"))

        assert code == 1
        assert "FAIL" in capsys.readouterr().out

    def test_volume_differences_can_be_made_to_fail(self, tmp_path, capsys):
        store(tmp_path / "left", series(60, volume="100"))
        store(tmp_path / "right", series(60, volume="500"))

        assert main(compare_argv(tmp_path)) == 0

        code = main(compare_argv(tmp_path, "--volume-mismatch-fail-ratio", "0.5"))

        assert code == 1
        assert "disagree on volume" in capsys.readouterr().out

    def test_the_range_can_be_restricted(self, tmp_path, capsys):
        store(tmp_path / "left", series(120))
        store(tmp_path / "right", series(60))

        code = main(
            compare_argv(
                tmp_path,
                "--start",
                "2026-08-10T00:00:00Z",
                "--end",
                "2026-08-10T01:00:00Z",
            )
        )

        assert code == 0
        assert "Candles compared:  60" in capsys.readouterr().out

    def test_a_non_decimal_tolerance_is_refused(self, agreeing):
        with pytest.raises(SystemExit):
            main(compare_argv(agreeing, "--price-tolerance", "wide"))

    def test_a_negative_tolerance_is_refused(self, agreeing):
        with pytest.raises(SystemExit):
            main(compare_argv(agreeing, "--price-tolerance", "-1"))

    def test_an_inverted_range_is_reported_not_raised(self, agreeing, capsys):
        code = main(
            compare_argv(
                agreeing,
                "--start",
                "2026-08-10T02:00:00Z",
                "--end",
                "2026-08-10T01:00:00Z",
            )
        )

        assert code == 1
        assert "start must be before end" in capsys.readouterr().out


class TestProviderIdentity:
    def test_the_manifest_roots_name_both_providers(self, agreeing, capsys):
        for side, provider in (("left", "csv"), ("right", "dukascopy")):
            write_provenance(agreeing / f"{side}-manifests", provider=provider)

        main(
            compare_argv(
                agreeing,
                "--left-manifest-root",
                str(agreeing / "left-manifests"),
                "--right-manifest-root",
                str(agreeing / "right-manifests"),
            )
        )
        output = capsys.readouterr().out

        assert "Left provider:     csv" in output
        assert "Right provider:    dukascopy" in output

    def test_an_unidentified_dataset_is_named_unknown(self, agreeing, capsys):
        main(compare_argv(agreeing))

        assert "Left provider:     unknown" in capsys.readouterr().out


class TestRecording:
    def test_a_record_is_written_when_asked_for(self, agreeing, capsys):
        code = main(compare_argv(agreeing, "--record-root", str(agreeing / "records")))
        output = capsys.readouterr().out

        assert code == 0
        assert "Record:" in output

        written = list((agreeing / "records").rglob("comparison.json"))

        assert len(written) == 1

        payload = json.loads(written[0].read_text())

        assert payload["report"]["status"] == "pass"
        assert payload["version"] == 1
        assert payload["key"]["symbol"] == SYMBOL
        assert payload["fingerprint"]

    def test_no_record_is_written_by_default(self, agreeing):
        main(compare_argv(agreeing))

        assert not (agreeing / "records").exists()

    def test_a_failing_comparison_is_recorded_too(self, disagreeing):
        main(compare_argv(disagreeing, "--record-root", str(disagreeing / "records")))

        written = list((disagreeing / "records").rglob("comparison.json"))
        payload = json.loads(written[0].read_text())

        assert payload["report"]["status"] == "fail"

    def test_the_symbol_spelling_does_not_expire_a_record(self, agreeing):
        root = str(agreeing / "records")
        main(compare_argv(agreeing, "--record-root", root))

        written = next((agreeing / "records").rglob("comparison.json"))
        slashed = json.loads(written.read_text())["fingerprint"]

        argv = compare_argv(agreeing, "--record-root", root)
        argv[argv.index("EUR/USD")] = "EUR_USD"
        main(argv)

        underscored = json.loads(written.read_text())["fingerprint"]

        assert underscored == slashed

    def test_rerunning_after_a_change_replaces_the_record(self, agreeing):
        root = str(agreeing / "records")
        main(compare_argv(agreeing, "--record-root", root))

        written = next((agreeing / "records").rglob("comparison.json"))
        before = json.loads(written.read_text())["fingerprint"]

        store(agreeing / "right", series(60, price="1.50000000"))
        main(compare_argv(agreeing, "--record-root", root))

        after = json.loads(written.read_text())["fingerprint"]

        assert after != before
