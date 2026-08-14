from __future__ import annotations

import os
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path

from pydantic import BaseModel, Field

from marketdata.downloader.chunks import Chunk
from marketdata.storage.parquet import normalize_symbol_path

CHECKPOINT_VERSION = 1


class ChunkStatus(StrEnum):
    """Lifecycle of a single chunk within a download."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class ChunkCheckpoint(BaseModel):
    """Recorded progress for one chunk."""

    index: int
    chunk_start: datetime
    chunk_end: datetime
    status: ChunkStatus = ChunkStatus.PENDING
    attempts: int = 0
    row_count: int = 0
    files: list[str] = Field(default_factory=list)
    error: str | None = None
    completed_at: datetime | None = None
    updated_at: datetime | None = None

    @property
    def is_complete(self) -> bool:
        return self.status is ChunkStatus.COMPLETED

    def matches(self, chunk: Chunk) -> bool:
        """Return whether this record describes exactly ``chunk``."""
        return (
            self.index == chunk.index
            and self.chunk_start == chunk.start
            and self.chunk_end == chunk.end
        )


class DownloadCheckpoint(BaseModel):
    """
    Resumable state for one download request.

    Identified by provider, symbol, timeframe and the requested range, so a
    rerun of the same command finds its own progress and nothing else.
    """

    version: int = CHECKPOINT_VERSION
    provider: str
    symbol: str
    timeframe: str
    requested_start: datetime
    requested_end: datetime
    chunk_size: str
    created_at: datetime
    updated_at: datetime
    chunks: list[ChunkCheckpoint]

    @property
    def completed_chunks(self) -> int:
        return sum(1 for chunk in self.chunks if chunk.status is ChunkStatus.COMPLETED)

    @property
    def failed_chunks(self) -> int:
        return sum(1 for chunk in self.chunks if chunk.status is ChunkStatus.FAILED)

    @property
    def is_complete(self) -> bool:
        return all(chunk.is_complete for chunk in self.chunks)

    def pending_chunks(self) -> list[ChunkCheckpoint]:
        """Return the chunks still needing work, in order."""
        return [chunk for chunk in self.chunks if not chunk.is_complete]

    def record(self, index: int) -> ChunkCheckpoint:
        """Return the record for a chunk index."""
        try:
            return self.chunks[index]
        except IndexError:
            raise KeyError(f"No checkpoint for chunk {index}") from None


def _now() -> datetime:
    return datetime.now(UTC)


def _fresh_chunks(chunks: list[Chunk]) -> list[ChunkCheckpoint]:
    return [
        ChunkCheckpoint(
            index=chunk.index,
            chunk_start=chunk.start,
            chunk_end=chunk.end,
        )
        for chunk in chunks
    ]


def _carry_over(
    previous: DownloadCheckpoint,
    chunks: list[Chunk],
) -> list[ChunkCheckpoint]:
    """
    Rebuild the chunk list, keeping progress that still applies.

    A stored record is only reused when its boundaries match the newly
    planned chunk exactly. Anything else — a different chunk size, a moved
    boundary — starts again, because the data it covers no longer lines up
    with what is about to be downloaded.
    """
    by_boundaries = {
        (record.chunk_start, record.chunk_end): record for record in previous.chunks
    }

    rebuilt: list[ChunkCheckpoint] = []

    for chunk in chunks:
        stored = by_boundaries.get((chunk.start, chunk.end))

        if stored is not None and stored.is_complete:
            rebuilt.append(stored.model_copy(update={"index": chunk.index}))
            continue

        rebuilt.append(
            ChunkCheckpoint(
                index=chunk.index,
                chunk_start=chunk.start,
                chunk_end=chunk.end,
            )
        )

    return rebuilt


class CheckpointStore:
    """Persist download progress as one JSON file per request."""

    def __init__(self, root: str | Path = "data/checkpoints") -> None:
        self.root = Path(root)

    def path_for(
        self,
        *,
        symbol: str,
        timeframe: str,
        start: datetime,
        end: datetime,
    ) -> Path:
        slug = f"{timeframe}_{start:%Y%m%dT%H%M%SZ}_{end:%Y%m%dT%H%M%SZ}"

        return self.root / normalize_symbol_path(symbol) / f"{slug}.json"

    def load(
        self,
        *,
        symbol: str,
        timeframe: str,
        start: datetime,
        end: datetime,
    ) -> DownloadCheckpoint | None:
        """Return stored progress for a request, or ``None`` when there is none."""
        path = self.path_for(
            symbol=symbol,
            timeframe=timeframe,
            start=start,
            end=end,
        )

        if not path.exists():
            return None

        return DownloadCheckpoint.model_validate_json(path.read_text())

    def save(self, checkpoint: DownloadCheckpoint) -> Path:
        """
        Write progress atomically.

        A download is interrupted precisely when something goes wrong, so a
        half-written checkpoint is a realistic outcome of a plain write. The
        replace is atomic, leaving either the previous state or the new one.
        """
        path = self.path_for(
            symbol=checkpoint.symbol,
            timeframe=checkpoint.timeframe,
            start=checkpoint.requested_start,
            end=checkpoint.requested_end,
        )
        path.parent.mkdir(parents=True, exist_ok=True)

        checkpoint.updated_at = _now()

        temporary = path.with_name(f"{path.name}.tmp")
        temporary.write_text(checkpoint.model_dump_json(indent=2))
        os.replace(temporary, path)

        return path

    def open(
        self,
        *,
        provider: str,
        symbol: str,
        timeframe: str,
        start: datetime,
        end: datetime,
        chunks: list[Chunk],
        chunk_size: str,
        resume: bool = True,
    ) -> DownloadCheckpoint:
        """
        Return the checkpoint to work against for this request.

        With ``resume`` disabled, or when no compatible progress exists, every
        chunk starts pending.
        """
        previous = (
            self.load(symbol=symbol, timeframe=timeframe, start=start, end=end)
            if resume
            else None
        )

        if previous is None or previous.version != CHECKPOINT_VERSION:
            now = _now()

            return DownloadCheckpoint(
                provider=provider,
                symbol=symbol,
                timeframe=timeframe,
                requested_start=start,
                requested_end=end,
                chunk_size=chunk_size,
                created_at=now,
                updated_at=now,
                chunks=_fresh_chunks(chunks),
            )

        previous.chunk_size = chunk_size
        previous.provider = provider
        previous.chunks = _carry_over(previous, chunks)

        return previous


def mark_running(checkpoint: DownloadCheckpoint, index: int) -> None:
    record = checkpoint.record(index)
    record.status = ChunkStatus.RUNNING
    record.attempts += 1
    record.error = None
    record.updated_at = _now()


def mark_completed(
    checkpoint: DownloadCheckpoint,
    index: int,
    *,
    row_count: int,
    files: list[Path],
) -> None:
    record = checkpoint.record(index)
    record.status = ChunkStatus.COMPLETED
    record.row_count = row_count
    record.files = [str(path) for path in files]
    record.error = None
    record.completed_at = _now()
    record.updated_at = record.completed_at


def mark_failed(checkpoint: DownloadCheckpoint, index: int, error: str) -> None:
    record = checkpoint.record(index)
    record.status = ChunkStatus.FAILED
    record.error = error
    record.updated_at = _now()
