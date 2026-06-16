"""
app/metrics.py
 
GET /stores/{store_id}/metrics
 
Returns "today's" store-level metrics: unique visitors, avg dwell per zone,
current queue depth, and queue abandonment rate. Excludes staff
(is_staff=true) from all customer-facing counts.
 
INCREMENTAL BUILD NOTE
----------------------
`conversion_rate` is stubbed as `null` for now -- computing it requires
correlating visitor sessions with pos_transactions.csv, which hasn't been
loaded into the system yet. Everything else in this endpoint is fully
computed from the `events` table alone. See CHOICES.md.
"""
 
from __future__ import annotations
 
from datetime import date as date_type, datetime, time, timedelta, timezone
from typing import Optional
 
from fastapi import APIRouter, Depends, Query
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
 
from app.database import EventRecord, get_db
from app.models import EventType, MetricsResponse, ZoneDwell
 
router = APIRouter()
 
 
def _day_bounds(target_date: date_type) -> tuple[datetime, datetime]:
    """Return [start, end) UTC datetime bounds for a given calendar date."""
    start = datetime.combine(target_date, time.min, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    return start, end
 
 
@router.get("/stores/{store_id}/metrics", response_model=MetricsResponse)
async def get_store_metrics(
    store_id: str,
    date: Optional[date_type] = Query(
        default=None,
        description="UTC calendar date to compute metrics for. "
                     "Defaults to today (UTC) if not provided.",
    ),
    db: AsyncSession = Depends(get_db),
) -> MetricsResponse:
    """Compute today's metrics for one store.
 
    Design notes:
      - "Today" defaults to the current UTC date, but accepts an optional
        `?date=YYYY-MM-DD` override -- useful for testing against
        clip-derived timestamps that aren't "today" in real life, while
        still being "real-time" (always queries live data, never cached)
        by default.
      - `store_id` is NOT validated against a known list -- a store with
        zero events simply returns zero-valued metrics. This is how the
        "zero-traffic store must not crash or return null" requirement
        is satisfied: every field below has a safe default for the
        empty case.
    """
    target_date = date or datetime.now(timezone.utc).date()
    start, end = _day_bounds(target_date)
 
    # ── Unique visitors (non-staff) ────────────────────────────────────
    unique_visitors_stmt = select(
        func.count(func.distinct(EventRecord.visitor_id))
    ).where(
        EventRecord.store_id == store_id,
        EventRecord.is_staff.is_(False),
        EventRecord.timestamp >= start,
        EventRecord.timestamp < end,
    )
    unique_visitors = (await db.execute(unique_visitors_stmt)).scalar() or 0
 
    # ── Average dwell per zone (from ZONE_DWELL events, non-staff) ─────
    dwell_stmt = (
        select(
            EventRecord.zone_id,
            func.avg(EventRecord.dwell_ms),
            func.count(EventRecord.event_id),
        )
        .where(
            EventRecord.store_id == store_id,
            EventRecord.is_staff.is_(False),
            EventRecord.event_type == EventType.ZONE_DWELL.value,
            EventRecord.timestamp >= start,
            EventRecord.timestamp < end,
        )
        .group_by(EventRecord.zone_id)
    )
    dwell_rows = (await db.execute(dwell_stmt)).all()
    avg_dwell_per_zone = [
        ZoneDwell(
            zone_id=zone_id,
            avg_dwell_ms=float(avg_ms),
            sample_count=count,
        )
        for zone_id, avg_ms, count in dwell_rows
        if zone_id is not None
    ]
 
    # ── Current queue depth: from the most recent BILLING_QUEUE_JOIN ────
    latest_queue_stmt = (
        select(EventRecord.event_metadata)
        .where(
            EventRecord.store_id == store_id,
            EventRecord.event_type == EventType.BILLING_QUEUE_JOIN.value,
            EventRecord.timestamp >= start,
            EventRecord.timestamp < end,
        )
        .order_by(EventRecord.timestamp.desc())
        .limit(1)
    )
    latest_queue_row = (await db.execute(latest_queue_stmt)).first()
    if latest_queue_row is not None and latest_queue_row[0]:
        queue_depth = int(latest_queue_row[0].get("queue_depth") or 0)
    else:
        queue_depth = 0
 
    # ── Abandonment rate: ABANDON / JOIN (non-staff) ────────────────────
    join_count_stmt = select(func.count()).where(
        EventRecord.store_id == store_id,
        EventRecord.is_staff.is_(False),
        EventRecord.event_type == EventType.BILLING_QUEUE_JOIN.value,
        EventRecord.timestamp >= start,
        EventRecord.timestamp < end,
    )
    abandon_count_stmt = select(func.count()).where(
        EventRecord.store_id == store_id,
        EventRecord.is_staff.is_(False),
        EventRecord.event_type == EventType.BILLING_QUEUE_ABANDON.value,
        EventRecord.timestamp >= start,
        EventRecord.timestamp < end,
    )
    join_count = (await db.execute(join_count_stmt)).scalar() or 0
    abandon_count = (await db.execute(abandon_count_stmt)).scalar() or 0
    abandonment_rate = (abandon_count / join_count) if join_count > 0 else 0.0
 
    return MetricsResponse(
        store_id=store_id,
        date=target_date.isoformat(),
        unique_visitors=unique_visitors,
        conversion_rate=None,  # TODO: requires POS correlation (next step)
        avg_dwell_per_zone=avg_dwell_per_zone,
        queue_depth=queue_depth,
        abandonment_rate=round(abandonment_rate, 4),
        generated_at=datetime.now(timezone.utc),
    )