"""
app/heatmap.py

GET /stores/{store_id}/heatmap

Returns zone visit frequency + avg dwell time, normalised to a 0-100
heat score ready for grid heatmap rendering.

Key rules from the spec:
  - heat_score is normalised 0-100 across zones (highest visit count = 100)
  - data_confidence = False if fewer than 20 sessions in window
  - Staff excluded from all counts
  - Empty store must return empty zones list, not crash
"""

from __future__ import annotations

from datetime import date as date_type, datetime, time, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import EventRecord, get_db
from app.models import EventType, HeatmapResponse, HeatmapZone

router = APIRouter()


def _day_bounds(target_date: date_type) -> tuple[datetime, datetime]:
    """Return [start, end) UTC datetime bounds for a given calendar date."""
    start = datetime.combine(target_date, time.min, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    return start, end


def _normalise(value: int, max_value: int) -> float:
    """Normalise a value to 0-100 scale.

    The zone with the highest visit count scores 100.
    All others are proportional to it.
    Returns 0.0 if max_value is 0 (empty store edge case).
    """
    if max_value == 0:
        return 0.0
    return round((value / max_value) * 100, 1)


@router.get("/stores/{store_id}/heatmap", response_model=HeatmapResponse)
async def get_store_heatmap(
    store_id: str,
    date: Optional[date_type] = Query(
        default=None,
        description="UTC calendar date. Defaults to today (UTC) if not provided.",
    ),
    db: AsyncSession = Depends(get_db),
) -> HeatmapResponse:
    """Compute zone-level heatmap data for one store.

    Steps:
      1. Count distinct visitors per zone (from ZONE_ENTER events)
      2. Average dwell time per zone (from ZONE_DWELL events)
      3. Normalise visit counts to 0-100 heat scores
      4. Flag low-confidence zones (< 20 sessions)
    """
    target_date = date or datetime.now(timezone.utc).date()
    start, end = _day_bounds(target_date)

    base_filters = [
        EventRecord.store_id == store_id,
        EventRecord.is_staff.is_(False),
        EventRecord.timestamp >= start,
        EventRecord.timestamp < end,
        EventRecord.zone_id.isnot(None),
    ]

    # ── Step 1: visit count per zone (from ZONE_ENTER) ─────────────────
    # COUNT(DISTINCT visitor_id) per zone so one visitor browsing the same
    # zone twice still counts as 1 visit for heatmap purposes.
    visit_stmt = (
        select(
            EventRecord.zone_id,
            func.count(func.distinct(EventRecord.visitor_id)).label("visit_count"),
        )
        .where(
            *base_filters,
            EventRecord.event_type == EventType.ZONE_ENTER.value,
        )
        .group_by(EventRecord.zone_id)
    )
    visit_rows = (await db.execute(visit_stmt)).all()

    # build a dict: zone_id -> visit_count for easy lookup
    visit_counts: dict[str, int] = {
        row.zone_id: row.visit_count
        for row in visit_rows
        if row.zone_id
    }

    # ── Step 2: avg dwell per zone (from ZONE_DWELL events) ────────────
    # ZONE_DWELL events carry the actual dwell_ms values (emitted every
    # 30s of continuous presence). AVG across all such events per zone
    # gives mean dwell time for that zone.
    dwell_stmt = (
        select(
            EventRecord.zone_id,
            func.avg(EventRecord.dwell_ms).label("avg_dwell_ms"),
        )
        .where(
            *base_filters,
            EventRecord.event_type == EventType.ZONE_DWELL.value,
        )
        .group_by(EventRecord.zone_id)
    )
    dwell_rows = (await db.execute(dwell_stmt)).all()

    # build a dict: zone_id -> avg_dwell_ms
    avg_dwells: dict[str, float] = {
        row.zone_id: float(row.avg_dwell_ms)
        for row in dwell_rows
        if row.zone_id and row.avg_dwell_ms is not None
    }

    # ── Step 3: normalise visit counts to 0-100 heat scores ────────────
    # The zone with the highest visit_count scores 100. All others are
    # proportional. This makes the heatmap rendering tool-agnostic --
    # the consumer just maps 0-100 to a colour gradient.
    max_visits = max(visit_counts.values(), default=0)

    # Merge into HeatmapZone objects.
    # Use visit_counts as the source of truth for which zones exist --
    # a zone with dwell data but no ZONE_ENTER events is anomalous and
    # excluded. A zone with ZONE_ENTER but no ZONE_DWELL data gets
    # avg_dwell_ms=0.0 (dwell tracking not yet triggered for that zone).
    zones: list[HeatmapZone] = []
    for zone_id, visit_count in sorted(
        visit_counts.items(), key=lambda x: x[1], reverse=True
    ):
        zones.append(
            HeatmapZone(
                zone_id=zone_id,
                visit_count=visit_count,
                avg_dwell_ms=avg_dwells.get(zone_id, 0.0),
                heat_score=_normalise(visit_count, max_visits),
                # spec: data_confidence=False if fewer than 20 sessions
                data_confidence=visit_count >= 20,
            )
        )

    return HeatmapResponse(
        store_id=store_id,
        date=target_date.isoformat(),
        zones=zones,
        generated_at=datetime.now(timezone.utc),
    )