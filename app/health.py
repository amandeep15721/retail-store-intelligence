"""
app/health.py

GET /health

Service health endpoint. Returns:
  - Overall service status (OK | DEGRADED)
  - Per-store last event timestamp and feed freshness
  - STALE_FEED warning if any store has had no events in the last 10 minutes

This is the endpoint an on-call engineer checks first when something
seems wrong. It must be accurate and fast -- no heavy queries.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import EventRecord, get_db
from app.models import HealthResponse, StoreHealth

router = APIRouter()

STALE_FEED_MINUTES = 10   # flag store as STALE_FEED if no events in this window


@router.get("/health", response_model=HealthResponse)
async def get_health(
    db: AsyncSession = Depends(get_db),
) -> HealthResponse:
    """Return service health and per-store feed freshness.

    Scans all known stores (stores that have ever sent at least one event)
    and reports their most recent event timestamp. A store is flagged
    STALE_FEED if its last event is older than STALE_FEED_MINUTES.

    A store that has NEVER sent any events is flagged NO_DATA -- this is
    different from STALE_FEED (which means "was working, now silent").

    Overall status:
      OK       -- all stores with data have fresh feeds
      DEGRADED -- at least one store is STALE_FEED
    """
    now = datetime.now(timezone.utc)
    stale_cutoff = now - timedelta(minutes=STALE_FEED_MINUTES)

    # Get the most recent `received_at` per store across ALL time.
    # We use `received_at` (when the API received the event) rather than
    # `timestamp` (when the event happened in the video) because:
    #   - `timestamp` reflects clip time, which could be hours/days old
    #   - `received_at` reflects pipeline activity -- if the pipeline
    #     stops sending events, `received_at` goes stale even if clip
    #     timestamps are recent. This is what we actually want to detect.
    last_event_stmt = (
        select(
            EventRecord.store_id,
            func.max(EventRecord.received_at).label("last_received"),
        )
        .group_by(EventRecord.store_id)
        .order_by(EventRecord.store_id)
    )
    rows = (await db.execute(last_event_stmt)).all()

    stores: list[StoreHealth] = []
    any_stale = False

    for row in rows:
        store_id, last_received = row.store_id, row.last_received

        if last_received is None:
            # store exists in DB but somehow has no received_at (shouldn't
            # happen with our schema, but handle it gracefully)
            stores.append(StoreHealth(
                store_id=store_id,
                last_event_at=None,
                status="NO_DATA",
                lag_minutes=None,
            ))
            continue

        # ensure timezone-aware for comparison
        if last_received.tzinfo is None:
            last_received = last_received.replace(tzinfo=timezone.utc)

        lag_minutes = round((now - last_received).total_seconds() / 60, 1)
        is_stale = last_received < stale_cutoff

        if is_stale:
            any_stale = True

        stores.append(StoreHealth(
            store_id=store_id,
            last_event_at=last_received,
            status="STALE_FEED" if is_stale else "OK",
            lag_minutes=lag_minutes,
        ))

    # if no stores have ever sent events, the service itself is still OK --
    # it's just waiting for data. Return a single synthetic NO_DATA entry
    # so the health response is never an empty list (which could mislead
    # an on-call engineer into thinking the endpoint itself is broken).
    if not stores:
        stores.append(StoreHealth(
            store_id="*",
            last_event_at=None,
            status="NO_DATA",
            lag_minutes=None,
        ))

    overall_status = "DEGRADED" if any_stale else "OK"

    return HealthResponse(
        status=overall_status,
        stores=stores,
        generated_at=now,
    )