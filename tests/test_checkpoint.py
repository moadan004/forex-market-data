import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from marketdata.downloader.checkpoint import (
    CheckpointStore,
    ChunkStatus,
    mark_completed,
    mark_failed,
    mark_running,
)
from marketdata.downloader.chunks import MonthlyChunkSize, plan_chunks

START = datetime(2019, 1, 1, tzinfo=UTC)
END = datetime(2019, 4, 1, tzinfo=UTC)
MONTH = MonthlyChunkSize()


@pytest.fixture
def store(tmp_path) -> CheckpointStore:
    return CheckpointStore(tmp_path)


@pytest.fixture
def chunks():
    return plan_chunks(START, END, MONTH)


def open_checkpoint(store, chunks, *, resume=True, start=START, end=END):
    return store.open(
        provider="stub",
        symbol="EUR/USD",
        timeframe="1min",
        start=start,
        end=end,
        chunks=chunks,
        chunk_size=MONTH.label,
        resume=resume,
    )


def test_fresh_run_starts_every_chunk_pending(store, chunks):
    checkpoint = open_checkpoint(store, chunks)

    assert len(checkpoint.chunks) == 3
    assert all(record.status is ChunkStatus.PENDING for record in checkpoint.chunks)
    assert checkpoint.completed_chunks == 0
    assert checkpoint.is_complete is False
    assert len(checkpoint.pending_chunks()) == 3


def test_chunk_records_mirror_the_plan(store, chunks):
    checkpoint = open_checkpoint(store, chunks)

    for record, chunk in zip(checkpoint.chunks, chunks, strict=True):
        assert record.matches(chunk)


def test_nothing_is_stored_until_saved(store, chunks):
    open_checkpoint(store, chunks)

    assert store.load(symbol="EUR/USD", timeframe="1min", start=START, end=END) is None


def test_progress_round_trips_through_disk(store, chunks):
    checkpoint = open_checkpoint(store, chunks)

    mark_running(checkpoint, 0)
    mark_completed(checkpoint, 0, row_count=42, files=[Path("a.parquet")])
    path = store.save(checkpoint)

    reloaded = store.load(
        symbol="EUR/USD",
        timeframe="1min",
        start=START,
        end=END,
    )

    assert reloaded is not None
    assert reloaded.chunks[0].status is ChunkStatus.COMPLETED
    assert reloaded.chunks[0].row_count == 42
    assert reloaded.chunks[0].files == ["a.parquet"]
    assert reloaded.chunks[0].completed_at is not None
    assert reloaded.chunks[0].attempts == 1

    payload = json.loads(path.read_text())

    assert payload["symbol"] == "EUR/USD"
    assert payload["timeframe"] == "1min"
    assert payload["chunk_size"] == "1month"
    assert payload["chunks"][0]["chunk_start"].startswith("2019-01-01")
    assert payload["chunks"][0]["chunk_end"].startswith("2019-02-01")


def test_partially_completed_run_resumes_from_the_first_gap(store, chunks):
    checkpoint = open_checkpoint(store, chunks)

    mark_running(checkpoint, 0)
    mark_completed(checkpoint, 0, row_count=10, files=[])
    store.save(checkpoint)

    resumed = open_checkpoint(store, chunks)

    assert resumed.completed_chunks == 1
    assert [record.index for record in resumed.pending_chunks()] == [1, 2]


def test_failed_chunk_is_retried_on_resume(store, chunks):
    checkpoint = open_checkpoint(store, chunks)

    mark_running(checkpoint, 1)
    mark_failed(checkpoint, 1, "provider timeout")
    store.save(checkpoint)

    resumed = open_checkpoint(store, chunks)

    assert resumed.failed_chunks == 0
    assert [record.index for record in resumed.pending_chunks()] == [0, 1, 2]


def test_retrying_a_failed_chunk_counts_attempts(store, chunks):
    checkpoint = open_checkpoint(store, chunks)

    mark_running(checkpoint, 0)
    mark_failed(checkpoint, 0, "provider timeout")

    assert checkpoint.chunks[0].error == "provider timeout"

    mark_running(checkpoint, 0)

    assert checkpoint.chunks[0].attempts == 2
    assert checkpoint.chunks[0].error is None
    assert checkpoint.chunks[0].status is ChunkStatus.RUNNING

    mark_completed(checkpoint, 0, row_count=5, files=[])

    assert checkpoint.chunks[0].status is ChunkStatus.COMPLETED
    assert checkpoint.chunks[0].error is None


def test_already_completed_run_has_nothing_pending(store, chunks):
    checkpoint = open_checkpoint(store, chunks)

    for index in range(len(chunks)):
        mark_running(checkpoint, index)
        mark_completed(checkpoint, index, row_count=1, files=[])

    store.save(checkpoint)

    resumed = open_checkpoint(store, chunks)

    assert resumed.is_complete
    assert resumed.pending_chunks() == []
    assert resumed.completed_chunks == 3


def test_resume_can_be_disabled(store, chunks):
    checkpoint = open_checkpoint(store, chunks)

    mark_running(checkpoint, 0)
    mark_completed(checkpoint, 0, row_count=10, files=[])
    store.save(checkpoint)

    restarted = open_checkpoint(store, chunks, resume=False)

    assert restarted.completed_chunks == 0
    assert len(restarted.pending_chunks()) == 3


def test_changing_the_chunk_size_discards_incompatible_progress(store, chunks):
    checkpoint = open_checkpoint(store, chunks)

    for index in range(len(chunks)):
        mark_running(checkpoint, index)
        mark_completed(checkpoint, index, row_count=1, files=[])

    store.save(checkpoint)

    rechunked = store.open(
        provider="stub",
        symbol="EUR/USD",
        timeframe="1min",
        start=START,
        end=END,
        chunks=plan_chunks(START, END, MonthlyChunkSize(months=3)),
        chunk_size="3months",
        resume=True,
    )

    assert len(rechunked.chunks) == 1
    assert rechunked.completed_chunks == 0
    assert rechunked.chunk_size == "3months"


def test_matching_boundaries_survive_a_replan(store):
    """Extending a range keeps the months that were already downloaded."""
    checkpoint = open_checkpoint(store, plan_chunks(START, END, MONTH))

    for index in range(3):
        mark_running(checkpoint, index)
        mark_completed(checkpoint, index, row_count=7, files=[])

    store.save(checkpoint)

    longer = plan_chunks(START, datetime(2019, 6, 1, tzinfo=UTC), MONTH)
    extended = store.open(
        provider="stub",
        symbol="EUR/USD",
        timeframe="1min",
        start=START,
        end=END,
        chunks=longer,
        chunk_size=MONTH.label,
        resume=True,
    )

    assert len(extended.chunks) == 5
    assert extended.completed_chunks == 3
    assert [record.index for record in extended.pending_chunks()] == [3, 4]


def test_checkpoints_are_scoped_per_request(store, chunks):
    checkpoint = open_checkpoint(store, chunks)
    mark_running(checkpoint, 0)
    mark_completed(checkpoint, 0, row_count=1, files=[])
    store.save(checkpoint)

    other = store.load(
        symbol="GBP/USD",
        timeframe="1min",
        start=START,
        end=END,
    )
    other_timeframe = store.load(
        symbol="EUR/USD",
        timeframe="1hour",
        start=START,
        end=END,
    )

    assert other is None
    assert other_timeframe is None


def test_checkpoint_path_layout(store):
    path = store.path_for(
        symbol="EUR/USD",
        timeframe="1min",
        start=START,
        end=END,
    )

    assert path.parent.name == "EUR_USD"
    assert path.name == "1min_20190101T000000Z_20190401T000000Z.json"


def test_saving_leaves_no_temporary_file(store, chunks, tmp_path):
    store.save(open_checkpoint(store, chunks))

    assert list(tmp_path.rglob("*.tmp")) == []


def test_unknown_chunk_index_is_rejected(store, chunks):
    checkpoint = open_checkpoint(store, chunks)

    with pytest.raises(KeyError, match="No checkpoint for chunk"):
        mark_running(checkpoint, 99)


def test_incompatible_version_starts_over(store, chunks):
    checkpoint = open_checkpoint(store, chunks)
    mark_running(checkpoint, 0)
    mark_completed(checkpoint, 0, row_count=1, files=[])
    path = store.save(checkpoint)

    payload = json.loads(path.read_text())
    payload["version"] = 999
    path.write_text(json.dumps(payload))

    resumed = open_checkpoint(store, chunks)

    assert resumed.completed_chunks == 0
