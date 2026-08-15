"""Staged verification: thresholds, records, the safety guard and the CLI."""

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from marketdata.calendar import AlwaysOpenCalendar
from marketdata.cli import main
from marketdata.downloader.chunks import DurationChunkSize, MonthlyChunkSize
from marketdata.providers.csv import CsvMarketDataProvider
from marketdata.storage.parquet import ParquetStorage
from marketdata.verification.guard import check_download, check_prerequisites
from marketdata.verification.records import (
    RECORD_VERSION,
    VerificationStore,
    configuration_fingerprint,
)
from marketdata.verification.runner import resolve_range, run_stage
from marketdata.verification.stages import Stage, stage_end, stage_for_range
from marketdata.verification.status import (
    QualityThresholds,
    VerificationStatus,
    worst,
)

FIXTURES = Path(__file__).parent / "fixtures" / "csv"
DATASET = FIXTURES / "dataset"

# A Monday, so the calendar never confuses a closure with a gap.
START = datetime(2026, 8, 10, 0, 0, tzinfo=UTC)
MINUTE = timedelta(minutes=1)
HOUR = timedelta(hours=1)


def run(tmp_path, *, stage=Stage.SMOKE, source=DATASET, start=START, **kwargs):
    kwargs.setdefault("calendar", AlwaysOpenCalendar())
    # Half-hour chunks prove chunking on the smoke window; longer stages use
    # month chunks so a test does not plan thousands of them.
    kwargs.setdefault(
        "chunk_size",
        DurationChunkSize(amount=30, unit="min")
        if stage is Stage.SMOKE
        else MonthlyChunkSize(),
    )

    return run_stage(
        CsvMarketDataProvider(source),
        stage=stage,
        symbol="EUR/USD",
        start=start,
        data_root=tmp_path / "processed",
        manifest_root=tmp_path / "manifests",
        quality_root=tmp_path / "quality",
        checkpoint_root=tmp_path / "checkpoints",
        verification_root=tmp_path / "verification",
        **kwargs,
    )


def store(tmp_path) -> VerificationStore:
    return VerificationStore(tmp_path / "verification")


def csv_fingerprint(source=DATASET) -> str:
    return configuration_fingerprint(CsvMarketDataProvider(source).configuration())


# --------------------------------------------------------------------------
# Stages
# --------------------------------------------------------------------------


def test_stage_windows():
    assert stage_end(Stage.SMOKE, START) == START + HOUR
    assert stage_end(Stage.DAILY, START) == START + timedelta(days=1)
    assert stage_end(Stage.MONTHLY, START) == datetime(2026, 9, 10, tzinfo=UTC)


def test_a_month_end_start_is_clamped():
    assert stage_end(Stage.MONTHLY, datetime(2026, 1, 31, tzinfo=UTC)) == datetime(
        2026, 2, 28, tzinfo=UTC
    )


def test_historical_has_no_fixed_window():
    with pytest.raises(ValueError, match="no fixed duration"):
        stage_end(Stage.HISTORICAL, START)

    with pytest.raises(ValueError, match="needs an explicit end"):
        resolve_range(Stage.HISTORICAL, START)


def test_a_smaller_stage_refuses_an_explicit_end():
    with pytest.raises(ValueError, match="decides its own end"):
        resolve_range(Stage.SMOKE, START, START + HOUR)


def test_stage_for_range_picks_the_smallest_that_fits():
    assert stage_for_range(START, START + HOUR) is Stage.SMOKE
    assert stage_for_range(START, START + HOUR * 2) is Stage.DAILY
    assert stage_for_range(START, START + timedelta(days=2)) is Stage.MONTHLY
    assert stage_for_range(START, START + timedelta(days=400)) is Stage.HISTORICAL


def test_prerequisites_are_every_smaller_stage():
    assert Stage.SMOKE.prerequisites == ()
    assert Stage.DAILY.prerequisites == (Stage.SMOKE,)
    assert Stage.HISTORICAL.prerequisites == (
        Stage.SMOKE,
        Stage.DAILY,
        Stage.MONTHLY,
    )


# --------------------------------------------------------------------------
# Thresholds
# --------------------------------------------------------------------------


def test_a_complete_dataset_passes_the_threshold():
    assert QualityThresholds().grade_missing(0, 60) is VerificationStatus.PASS


def test_a_small_shortfall_warns_and_a_large_one_fails():
    thresholds = QualityThresholds(missing_fail_ratio=0.01)

    assert thresholds.grade_missing(1, 1000) is VerificationStatus.WARN
    assert thresholds.grade_missing(50, 1000) is VerificationStatus.FAIL


def test_a_shortfall_without_an_expectation_fails():
    """Nothing said how many to expect, so the gap cannot be put in proportion."""
    assert QualityThresholds().grade_missing(5, None) is VerificationStatus.FAIL
    assert QualityThresholds().grade_missing(5, 0) is VerificationStatus.FAIL


def test_thresholds_are_validated():
    with pytest.raises(ValueError, match="between 0 and 1"):
        QualityThresholds(missing_warn_ratio=2)

    with pytest.raises(ValueError, match="cannot fail before it warns"):
        QualityThresholds(missing_warn_ratio=0.5, missing_fail_ratio=0.1)


def test_failure_outranks_blocked():
    """Data we obtained and found wrong outranks data we could not obtain."""
    assert (
        worst([VerificationStatus.BLOCKED, VerificationStatus.FAIL])
        is VerificationStatus.FAIL
    )
    assert (
        worst([VerificationStatus.PASS, VerificationStatus.WARN])
        is VerificationStatus.WARN
    )
    assert worst([]) is VerificationStatus.PASS


def test_only_pass_and_warn_count_as_verified():
    assert VerificationStatus.PASS.verified is True
    assert VerificationStatus.WARN.verified is True
    assert VerificationStatus.FAIL.verified is False
    assert VerificationStatus.BLOCKED.verified is False


# --------------------------------------------------------------------------
# Running a stage
# --------------------------------------------------------------------------


def test_a_smoke_stage_passes_and_stores_a_dataset(tmp_path):
    outcome = run(tmp_path)

    assert outcome.status is VerificationStatus.PASS
    assert outcome.verified is True
    assert outcome.result.retained_count == 60
    assert outcome.validation.status.value == "ok"
    assert outcome.problems == []
    assert (
        len(
            ParquetStorage(tmp_path / "processed").partition_files(
                symbol="EUR/USD", timeframe="1min"
            )
        )
        == 1
    )


def test_a_stage_runs_the_real_pipeline(tmp_path):
    """Chunking, checkpoints, manifest and quality report all happen."""
    outcome = run(tmp_path)

    assert outcome.result.chunks_total == 2
    assert outcome.result.manifest.exists()
    assert outcome.result.quality_report_path.exists()
    assert outcome.result.checkpoint_path.exists()


def test_a_daily_stage_covers_a_day(tmp_path):
    run(tmp_path)

    outcome = run(tmp_path, stage=Stage.DAILY)

    assert outcome.result.requested_end == START + timedelta(days=1)
    # The fixture only holds two hours, so the rest of the day is missing.
    assert outcome.status is VerificationStatus.FAIL
    assert outcome.validation.missing_candles > 0


def test_a_daily_stage_passes_when_the_day_is_covered(tmp_path):
    run(tmp_path)

    outcome = run(
        tmp_path,
        stage=Stage.DAILY,
        thresholds=QualityThresholds(missing_fail_ratio=1.0),
    )

    assert outcome.status is VerificationStatus.WARN
    assert outcome.verified is True


def test_a_monthly_stage_runs_after_its_prerequisites(tmp_path):
    run(tmp_path)
    run(tmp_path, stage=Stage.DAILY, thresholds=QualityThresholds(missing_fail_ratio=1))

    outcome = run(
        tmp_path,
        stage=Stage.MONTHLY,
        thresholds=QualityThresholds(missing_fail_ratio=1),
    )

    assert outcome.status is VerificationStatus.WARN
    assert outcome.result.requested_end == datetime(2026, 9, 10, tzinfo=UTC)


def test_an_empty_result_fails(tmp_path):
    outcome = run(tmp_path, start=datetime(2030, 1, 7, tzinfo=UTC))

    assert outcome.status is VerificationStatus.FAIL
    assert "no candles were stored" in outcome.problems


def test_a_provider_that_cannot_be_reached_blocks(tmp_path):
    outcome = run(tmp_path, source=tmp_path / "absent")

    assert outcome.status is VerificationStatus.BLOCKED
    assert outcome.verified is False


def test_invalid_ohlc_fails_the_stage(tmp_path):
    """Structural defects have no tolerance, whatever the threshold."""
    outcome = run(
        tmp_path,
        source=FIXTURES / "messy_1min.csv",
        thresholds=QualityThresholds(missing_fail_ratio=1.0),
    )

    # The messy fixture's invalid row is dropped on ingest, so the dataset is
    # clean but short; the shortfall is what is graded.
    assert outcome.validation.invalid_rows == 0
    assert outcome.status is VerificationStatus.WARN


def test_a_corrupt_partition_fails_the_stage(tmp_path):
    run(tmp_path)

    partition = ParquetStorage(tmp_path / "processed").partition_files(
        symbol="EUR/USD", timeframe="1min"
    )[0]
    partition.write_bytes(b"not parquet")

    outcome = run(tmp_path, stage=Stage.DAILY)

    assert outcome.status is VerificationStatus.FAIL
    assert outcome.validation.readable is False
    assert any("could not be read" in problem for problem in outcome.problems)


def test_duplicate_candles_fail_the_stage(tmp_path):
    run(tmp_path)

    partition = ParquetStorage(tmp_path / "processed").partition_files(
        symbol="EUR/USD", timeframe="1min"
    )[0]
    pq.write_table(pq.read_table(partition), partition.with_name("extra.parquet"))

    outcome = run(tmp_path, stage=Stage.DAILY)

    assert outcome.status is VerificationStatus.FAIL
    assert outcome.validation.duplicate_candles > 0


def test_a_manifest_mismatch_fails_the_stage(tmp_path):
    run(tmp_path)

    manifests = list((tmp_path / "manifests").rglob("*.json"))
    payload = json.loads(manifests[0].read_text())
    payload["row_count"] = 9999
    manifests[0].write_text(json.dumps(payload))

    outcome = run(tmp_path, stage=Stage.DAILY)

    assert outcome.status is VerificationStatus.FAIL
    assert any("claims 9999 rows" in problem for problem in outcome.problems)


def test_an_unexpected_out_of_range_row_fails_the_stage(tmp_path):
    run(tmp_path)

    storage = ParquetStorage(tmp_path / "processed")
    stored = storage.read_candles(symbol="EUR/USD", timeframe="1min")
    intruder = stored[0].model_copy(update={"timestamp": START + timedelta(days=40)})
    storage.write([intruder], symbol="EUR/USD", timeframe="1min")

    outcome = run(tmp_path, stage=Stage.DAILY)

    assert outcome.status is VerificationStatus.FAIL
    assert outcome.validation.out_of_range_rows == 1


def test_a_market_closure_is_not_a_shortfall(tmp_path):
    """A weekend window expects nothing, so nothing is missing."""
    from marketdata.calendar import ForexCalendar

    outcome = run(
        tmp_path,
        start=datetime(2026, 8, 15, 0, 0, tzinfo=UTC),  # a Saturday
        calendar=ForexCalendar(),
    )

    assert outcome.validation.expected_candles == 0
    assert outcome.validation.missing_candles == 0
    # Nothing was stored, which is correct for a closed market but is still
    # not evidence that the provider works.
    assert outcome.status is VerificationStatus.FAIL


# --------------------------------------------------------------------------
# Verification records
# --------------------------------------------------------------------------


def test_a_passing_stage_writes_a_record(tmp_path):
    outcome = run(tmp_path)

    assert outcome.record_path.exists()
    assert outcome.record_path == (
        tmp_path / "verification" / "csv" / "EUR_USD" / "1min" / "smoke.json"
    )

    payload = json.loads(outcome.record_path.read_text())

    assert payload["version"] == RECORD_VERSION
    assert payload["stage"] == "smoke"
    assert payload["status"] == "pass"
    assert payload["provider"] == "csv"
    assert payload["symbol"] == "EUR/USD"
    assert payload["timeframe"] == "1min"
    assert payload["rows"] == 60
    assert payload["expected_rows"] == 60
    assert payload["quality_status"] == "ok"
    assert payload["validation_status"] == "ok"
    assert payload["validation_passed"] is True
    assert payload["live"] is False
    assert payload["manifest"]
    assert payload["dataset_files"]
    assert "structural defects always fail" in payload["thresholds"]


def test_a_record_survives_a_round_trip(tmp_path):
    run(tmp_path)

    record = store(tmp_path).load(
        provider="csv",
        symbol="EUR/USD",
        timeframe="1min",
        stage=Stage.SMOKE,
    )

    assert record is not None
    assert record.verified is True
    assert record.applies_to(csv_fingerprint()) is True


def test_a_failed_stage_records_the_failure_but_does_not_count(tmp_path):
    outcome = run(tmp_path, start=datetime(2030, 1, 7, tzinfo=UTC))

    assert outcome.record.status is VerificationStatus.FAIL

    assert (
        store(tmp_path).valid_record(
            provider="csv",
            symbol="EUR/USD",
            timeframe="1min",
            stage=Stage.SMOKE,
            fingerprint=csv_fingerprint(),
        )
        is None
    )


def test_a_changed_configuration_invalidates_a_record(tmp_path):
    run(tmp_path)

    other = configuration_fingerprint({"source": "somewhere/else"})

    assert (
        store(tmp_path).valid_record(
            provider="csv",
            symbol="EUR/USD",
            timeframe="1min",
            stage=Stage.SMOKE,
            fingerprint=other,
        )
        is None
    )


def test_a_record_from_an_older_schema_is_ignored(tmp_path):
    outcome = run(tmp_path)

    payload = json.loads(outcome.record_path.read_text())
    payload["version"] = RECORD_VERSION - 1
    outcome.record_path.write_text(json.dumps(payload))

    assert (
        store(tmp_path).valid_record(
            provider="csv",
            symbol="EUR/USD",
            timeframe="1min",
            stage=Stage.SMOKE,
            fingerprint=csv_fingerprint(),
        )
        is None
    )


def test_an_unreadable_record_is_ignored(tmp_path):
    outcome = run(tmp_path)
    outcome.record_path.write_text("{not json")

    assert (
        store(tmp_path).load(
            provider="csv",
            symbol="EUR/USD",
            timeframe="1min",
            stage=Stage.SMOKE,
        )
        is None
    )


def test_records_are_scoped_per_symbol_and_timeframe(tmp_path):
    run(tmp_path)

    assert (
        store(tmp_path).load(
            provider="csv", symbol="GBP/USD", timeframe="1min", stage=Stage.SMOKE
        )
        is None
    )
    assert (
        store(tmp_path).load(
            provider="csv", symbol="EUR/USD", timeframe="1hour", stage=Stage.SMOKE
        )
        is None
    )


def test_saving_leaves_no_temporary_file(tmp_path):
    run(tmp_path)

    assert list((tmp_path / "verification").rglob("*.tmp")) == []


# --------------------------------------------------------------------------
# The safety guard
# --------------------------------------------------------------------------


def guard(tmp_path, stage, **kwargs):
    return check_prerequisites(
        store(tmp_path),
        provider="csv",
        configuration=CsvMarketDataProvider(DATASET).configuration(),
        symbol="EUR/USD",
        timeframe="1min",
        stage=stage,
        **kwargs,
    )


def test_smoke_needs_no_prior_verification(tmp_path):
    decision = guard(tmp_path, Stage.SMOKE)

    assert decision.allowed is True
    assert decision.required == ()
    assert "needs no prior verification" in decision.reason


def test_a_stage_is_refused_until_its_prerequisite_passes(tmp_path):
    assert guard(tmp_path, Stage.DAILY).allowed is False

    run(tmp_path)

    decision = guard(tmp_path, Stage.DAILY)

    assert decision.allowed is True
    assert decision.satisfied == (Stage.SMOKE,)
    assert "smoke=pass" in decision.reason


def test_historical_needs_all_three(tmp_path):
    decision = guard(tmp_path, Stage.HISTORICAL)

    assert decision.allowed is False
    assert decision.missing == (Stage.SMOKE, Stage.DAILY, Stage.MONTHLY)
    assert "refusing a historical acquisition" in decision.reason


def test_the_override_is_explicit_and_reported(tmp_path):
    decision = guard(tmp_path, Stage.HISTORICAL, override=True)

    assert decision.allowed is True
    assert decision.overridden is True
    assert decision.missing == (Stage.SMOKE, Stage.DAILY, Stage.MONTHLY)
    assert "without verification" in decision.reason


def test_a_stage_run_is_blocked_by_its_prerequisites(tmp_path):
    outcome = run(tmp_path, stage=Stage.MONTHLY)

    assert outcome.status is VerificationStatus.BLOCKED
    assert outcome.result is None
    assert outcome.record is None
    assert "daily" in outcome.problems[0]


def test_a_blocked_stage_writes_nothing(tmp_path):
    run(tmp_path, stage=Stage.HISTORICAL)

    assert not (tmp_path / "processed").exists()
    assert not (tmp_path / "verification").exists()


def test_a_download_smaller_than_a_month_is_unguarded(tmp_path):
    """Small ranges are the stages themselves; refusing them leaves no start."""
    for end in (START + HOUR, START + timedelta(days=1), START + timedelta(days=20)):
        decision = check_download(
            store(tmp_path),
            provider="csv",
            configuration={"source": str(DATASET)},
            symbol="EUR/USD",
            timeframe="1min",
            start=START,
            end=end,
        )

        assert decision.allowed is True
        assert decision.required == ()


def test_a_historical_download_is_refused_without_prerequisites(tmp_path):
    decision = check_download(
        store(tmp_path),
        provider="csv",
        configuration={"source": str(DATASET)},
        symbol="EUR/USD",
        timeframe="1min",
        start=START,
        end=START + timedelta(days=400),
    )

    assert decision.allowed is False
    assert decision.stage is Stage.HISTORICAL


def test_a_historical_download_is_allowed_after_prerequisites(tmp_path):
    run(tmp_path)
    run(tmp_path, stage=Stage.DAILY, thresholds=QualityThresholds(missing_fail_ratio=1))
    run(
        tmp_path,
        stage=Stage.MONTHLY,
        thresholds=QualityThresholds(missing_fail_ratio=1),
    )

    decision = check_download(
        store(tmp_path),
        provider="csv",
        configuration=CsvMarketDataProvider(DATASET).configuration(),
        symbol="EUR/USD",
        timeframe="1min",
        start=START,
        end=START + timedelta(days=400),
    )

    assert decision.allowed is True
    assert decision.satisfied == (Stage.SMOKE, Stage.DAILY, Stage.MONTHLY)


# --------------------------------------------------------------------------
# Provenance
# --------------------------------------------------------------------------


def test_the_manifest_records_how_the_data_was_acquired(tmp_path):
    outcome = run(tmp_path)

    provenance = json.loads(outcome.result.manifest.read_text())["provenance"]

    assert provenance["provider"] == "csv"
    assert provenance["provider_configuration"]["source"] == str(DATASET)
    assert provenance["symbol"] == "EUR/USD"
    assert provenance["timeframe"] == "1min"
    assert provenance["requested_start"].startswith("2026-08-10T00:00:00")
    assert provenance["requested_end"].startswith("2026-08-10T01:00:00")
    assert provenance["actual_start"].startswith("2026-08-10T00:00:00")
    assert provenance["rows"] == 60
    assert provenance["quality_status"] == "ok"
    assert provenance["calendar"] == "24x7"
    assert provenance["chunk_size"] == "30min"
    assert provenance["application_version"]
    assert provenance["acquired_at"]
    assert provenance["unverified_override"] is False


def test_provenance_never_records_a_credential(tmp_path):
    from marketdata.downloader.pipeline import DownloadPipeline
    from marketdata.providers.dukascopy import DukascopyProvider

    pipeline = DownloadPipeline(
        DukascopyProvider(api_key="SUPER-SECRET"),
        output_root=tmp_path / "processed",
        manifest_root=tmp_path / "manifests",
        quality_root=tmp_path / "quality",
        checkpoint_root=tmp_path / "checkpoints",
    )
    provenance = pipeline.provider.configuration()

    assert provenance["api_key"] == "set"
    assert "SUPER-SECRET" not in json.dumps(provenance)


def test_a_dukascopy_run_records_its_pacing(tmp_path):
    import httpx
    from conftest import instant_limiter

    from marketdata.downloader.pipeline import DownloadPipeline
    from marketdata.providers.dukascopy import DukascopyProvider

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.params["path"] == "api/instrumentList":
            return httpx.Response(200, json=[{"id": 1, "name": "EUR/USD"}])

        return httpx.Response(
            200,
            json={
                "candles": [
                    {
                        "timestamp": int((START + MINUTE * index).timestamp() * 1000),
                        "bid_open": 1.17,
                        "bid_high": 1.171,
                        "bid_low": 1.169,
                        "bid_close": 1.1705,
                    }
                    for index in range(60)
                ]
            },
        )

    pipeline = DownloadPipeline(
        DukascopyProvider(
            client=httpx.Client(transport=httpx.MockTransport(handler)),
            rate_limiter=instant_limiter(),
        ),
        output_root=tmp_path / "processed",
        manifest_root=tmp_path / "manifests",
        quality_root=tmp_path / "quality",
        checkpoint_root=tmp_path / "checkpoints",
        calendar=AlwaysOpenCalendar(),
    )

    result = pipeline.run(symbol="EUR/USD", start=START, end=START + HOUR)
    provenance = json.loads(result.manifest.read_text())["provenance"]

    assert "requests/sec" in provenance["rate_limit"]
    assert "attempts" in provenance["retry"]


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------


def roots(tmp_path) -> list[str]:
    return [
        "--data-root",
        str(tmp_path / "processed"),
        "--manifest-root",
        str(tmp_path / "manifests"),
        "--quality-root",
        str(tmp_path / "quality"),
        "--checkpoint-root",
        str(tmp_path / "checkpoints"),
        "--verification-root",
        str(tmp_path / "verification"),
    ]


def verify_argv(tmp_path, stage="smoke", *extra):
    return [
        "verify-stage",
        "--stage",
        stage,
        "--provider",
        "csv",
        "--source",
        str(DATASET),
        "--symbol",
        "EUR/USD",
        "--start",
        "2026-08-10T00:00:00Z",
        "--calendar",
        "24x7",
        # Small chunks prove chunking on the smoke window; a month split into
        # half-hours would plan fifteen hundred of them.
        "--chunk-size",
        "30min" if stage == "smoke" else "1month",
        *roots(tmp_path),
        *extra,
    ]


def test_provider_check_reports_every_check(tmp_path, capsys):
    exit_code = main(
        [
            "provider-check",
            "--provider",
            "csv",
            "--source",
            str(DATASET),
            "--symbol",
            "EUR/USD",
            "--start",
            "2026-08-10T00:00:00Z",
            "--end",
            "2026-08-10T01:00:00Z",
        ]
    )

    output = capsys.readouterr().out

    assert exit_code == 0
    assert "Provider:          csv" in output
    assert "PASS     reachable" in output
    assert "PASS     pagination" in output
    assert "Result:            PASS" in output


def test_provider_check_emits_json(tmp_path, capsys):
    main(
        [
            "provider-check",
            "--provider",
            "csv",
            "--source",
            str(DATASET),
            "--symbol",
            "EUR/USD",
            "--start",
            "2026-08-10T00:00:00Z",
            "--end",
            "2026-08-10T01:00:00Z",
            "--json",
        ]
    )

    payload = json.loads(capsys.readouterr().out)

    assert payload["status"] == "pass"
    assert payload["candles"] == 60
    assert len(payload["checks"]) == 8


def test_provider_check_exits_non_zero_when_blocked(tmp_path, capsys):
    exit_code = main(
        [
            "provider-check",
            "--provider",
            "csv",
            "--source",
            str(tmp_path / "absent"),
            "--symbol",
            "EUR/USD",
            "--start",
            "2026-08-10T00:00:00Z",
            "--end",
            "2026-08-10T01:00:00Z",
        ]
    )

    assert exit_code == 1
    assert "Result:            BLOCKED" in capsys.readouterr().out


def test_verify_stage_reports_the_full_summary(tmp_path, capsys):
    exit_code = main(verify_argv(tmp_path))

    output = capsys.readouterr().out

    assert exit_code == 0
    assert "Provider:          csv" in output
    assert "Symbol:            EUR/USD" in output
    assert "Timeframe:         1min" in output
    assert "Stage:             smoke" in output
    assert "Requested range:   2026-08-10T00:00:00Z -> 2026-08-10T01:00:00Z" in output
    assert "Actual range:      2026-08-10T00:00:00Z -> 2026-08-10T00:59:00Z" in output
    assert "Rows:              60" in output
    assert "Quality status:    ok" in output
    assert "Validation status: ok" in output
    assert "Verification:      PASS" in output


def test_verify_stage_emits_the_record_as_json(tmp_path, capsys):
    exit_code = main(verify_argv(tmp_path, "smoke", "--json"))

    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["stage"] == "smoke"
    assert payload["status"] == "pass"
    assert payload["rows"] == 60


def test_verify_stage_blocks_without_prerequisites(tmp_path, capsys):
    exit_code = main(verify_argv(tmp_path, "monthly"))

    output = capsys.readouterr().out

    assert exit_code == 1
    assert "Verification:      BLOCKED" in output
    assert "not verified" in output


def test_a_blocked_stage_emits_json_too(tmp_path, capsys):
    main(verify_argv(tmp_path, "monthly", "--json"))

    payload = json.loads(capsys.readouterr().out)

    assert payload["status"] == "blocked"
    assert payload["stage"] == "monthly"


def test_the_override_lets_a_stage_run(tmp_path, capsys):
    exit_code = main(
        verify_argv(
            tmp_path,
            "daily",
            "--force-unverified",
            "--missing-fail-ratio",
            "1.0",
        )
    )

    output = capsys.readouterr().out

    assert exit_code == 0
    assert "without verification" in output


def test_the_historical_download_is_refused_from_the_command_line(tmp_path, capsys):
    exit_code = main(
        [
            "download",
            "--provider",
            "csv",
            "--source",
            str(DATASET),
            "--symbol",
            "EUR/USD",
            "--start",
            "2019-01-01T00:00:00Z",
            "--end",
            "2026-01-01T00:00:00Z",
            "--calendar",
            "24x7",
            *roots(tmp_path),
        ]
    )

    output = capsys.readouterr().out

    assert exit_code == 2
    assert "refused" in output
    assert "verify-stage --stage smoke" in output
    assert not (tmp_path / "processed").exists()


def test_the_historical_download_runs_after_verification(tmp_path, capsys):
    main(verify_argv(tmp_path))
    main(verify_argv(tmp_path, "daily", "--missing-fail-ratio", "1.0"))
    main(verify_argv(tmp_path, "monthly", "--missing-fail-ratio", "1.0"))
    capsys.readouterr()

    exit_code = main(
        [
            "download",
            "--provider",
            "csv",
            "--source",
            str(DATASET),
            "--symbol",
            "EUR/USD",
            "--start",
            "2026-08-10T00:00:00Z",
            "--end",
            "2027-08-10T00:00:00Z",
            "--calendar",
            "24x7",
            *roots(tmp_path),
        ]
    )

    assert exit_code == 0
    assert "refused" not in capsys.readouterr().out


def test_the_override_is_recorded_in_provenance(tmp_path, capsys):
    exit_code = main(
        [
            "download",
            "--provider",
            "csv",
            "--source",
            str(DATASET),
            "--symbol",
            "EUR/USD",
            "--start",
            "2026-08-10T00:00:00Z",
            "--end",
            "2027-08-10T00:00:00Z",
            "--calendar",
            "24x7",
            "--force-unverified",
            *roots(tmp_path),
        ]
    )

    output = capsys.readouterr().out
    manifest = json.loads(next((tmp_path / "manifests").rglob("*.json")).read_text())

    assert exit_code == 0
    assert "WARNING" in output
    assert manifest["provenance"]["unverified_override"] is True
    assert "unproven" in manifest["provenance"]["verification"]


def test_an_unknown_stage_is_rejected(tmp_path):
    with pytest.raises(SystemExit):
        main(verify_argv(tmp_path, "yearly"))


def test_a_schema_mismatch_is_caught_by_a_stage(tmp_path):
    """The stage re-reads from disk, so a bad partition cannot slip past."""
    run(tmp_path)

    directory = (
        ParquetStorage(tmp_path / "processed").dataset_path("EUR/USD", "1min")
        / "year=2026"
        / "month=09"
    )
    directory.mkdir(parents=True)
    pq.write_table(
        pa.Table.from_pylist([{"timestamp": START, "close": 1.17}]),
        directory / "candles.parquet",
    )

    outcome = run(tmp_path, stage=Stage.DAILY)

    assert outcome.status is VerificationStatus.FAIL
    assert outcome.validation.schema_consistent is False
