"""
app/database.py

SQLAlchemy setup for the Store Intelligence API.

- Defines the `EventRecord` ORM table, which mirrors `app.models.Event`
  (the Pydantic schema) but as a flat, queryable SQL table.
- Provides an async engine + session factory backed by SQLite.
- Provides `init_db()` to create tables on startup and `get_db()` as a
  FastAPI dependency for request-scoped sessions.

WHY A SEPARATE ORM MODEL FROM THE PYDANTIC `Event`?
----------------------------------------------------
`app.models.Event` is the API CONTRACT (what /events/ingest validates).
`EventRecord` here is the STORAGE representation (a SQL table row).
They describe the same "thing", but:
  - `metadata` (a nested object in Event) is stored as a single JSON column
  - `event_type` (an Enum in Event) is stored as a plain string column
  - extra DB-only bookkeeping columns (e.g. `received_at`) can be added
    without touching the public API contract
Conversion between the two happens in app/ingestion.py.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import AsyncIterator

from sqlalchemy import Boolean, DateTime, Float, Index, Integer, JSON, String
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


# ──────────────────────────────────────────────────────────────────────────
# Engine + session setup
# ──────────────────────────────────────────────────────────────────────────

# Default to a local SQLite file under ./data/. Overridable via env var so
# Docker can point this at a mounted volume (see docker-compose.yml).
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "sqlite+aiosqlite:///./data/store_intelligence.db",
)

# Make sure ./data/ exists for the SQLite file (SQLite creates the .db file
# itself, but NOT the parent directory).
if DATABASE_URL.startswith("sqlite"):
    db_path = DATABASE_URL.split("///")[-1]
    db_dir = os.path.dirname(db_path)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)

engine = create_async_engine(
    DATABASE_URL,
    echo=False,   # set to True for verbose SQL logging while debugging
    future=True,
)

async_session = async_sessionmaker(
    engine,
    expire_on_commit=False,
)


class Base(DeclarativeBase):
    pass


# ──────────────────────────────────────────────────────────────────────────
# EventRecord — the `events` table
# ──────────────────────────────────────────────────────────────────────────

class EventRecord(Base):
    """One row per ingested event.

    Mirrors `app.models.Event` field-for-field, with `metadata` flattened
    into a single JSON column (queried via SQLite's json_extract() when
    individual sub-fields like session_seq or queue_depth are needed).
    """

    __tablename__ = "events"

    # event_id is the PRIMARY KEY -> gives us idempotency for free.
    # Inserting the same event_id twice raises IntegrityError, which
    # app/ingestion.py catches and counts as a "duplicate" rather than
    # an error.
    event_id: Mapped[str] = mapped_column(String(36), primary_key=True)

    store_id: Mapped[str] = mapped_column(String(64), nullable=False)
    camera_id: Mapped[str] = mapped_column(String(64), nullable=False)
    visitor_id: Mapped[str] = mapped_column(String(64), nullable=False)
    event_type: Mapped[str] = mapped_column(String(32), nullable=False)

    # stored as a timezone-aware UTC datetime
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    zone_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    dwell_ms: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    is_staff: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    confidence: Mapped[float] = mapped_column(Float, nullable=False)

    # the nested `metadata` object (queue_depth, sku_zone, session_seq, ...)
    # stored as a single JSON blob.
    event_metadata: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)

    # bookkeeping: when the API received this event (NOT the event's own
    # `timestamp`). Used by /health for "last event timestamp" + STALE_FEED.
    received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )

    __table_args__ = (
        # Most analytics queries filter by store + time range
        # (metrics, funnel, heatmap, anomalies all do "today's events
        # for store X").
        Index("ix_events_store_timestamp", "store_id", "timestamp"),
        # Session reconstruction groups events by visitor_id.
        Index("ix_events_visitor", "visitor_id"),
        # /health needs "most recent event per store" quickly.
        Index("ix_events_store_received", "store_id", "received_at"),
    )

    def __repr__(self) -> str:  # pragma: no cover - debugging aid only
        return (
            f"<EventRecord {self.event_type} store={self.store_id} "
            f"visitor={self.visitor_id} ts={self.timestamp.isoformat()}>"
        )


# ──────────────────────────────────────────────────────────────────────────
# Lifecycle helpers
# ──────────────────────────────────────────────────────────────────────────

async def init_db() -> None:
    """Create all tables if they don't exist yet.

    Called once at FastAPI startup (see app/main.py's lifespan handler).
    Using `create_all` (rather than Alembic migrations) is a deliberate
    simplicity choice for this challenge -- see CHOICES.md.
    """
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def get_db() -> AsyncIterator[AsyncSession]:
    """FastAPI dependency yielding a request-scoped AsyncSession.

    Usage:
        @app.get("/something")
        async def handler(db: AsyncSession = Depends(get_db)):
            ...
    """
    async with async_session() as session:
        yield session