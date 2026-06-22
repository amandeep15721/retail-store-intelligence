"""
app/funnel.py

GET /stores/{store_id}/funnel

Returns the conversion funnel for a store:
  ENTRY → ZONE_VISIT → BILLING_QUEUE → PURCHASE

Key rules from the spec:
  - SESSION is the unit, not raw events. A visitor who entered 3 zones
    still counts as 1 session at the ZONE_VISIT stage.
  - Re-entries must NOT double-count a visitor. We use the FIRST ENTRY
    event per visitor_id per day to define a session.
  - PURCHASE (bottom of funnel) is stubbed as null until POS correlation
    is implemented (same incremental approach as /metrics).
  - Staff (is_staff=true) are excluded from all counts.
"""

from __future__ import annotations

from datetime import date as date_type, datetime, time, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import EventRecord, get_db
from app.models import EventType, FunnelResponse, FunnelStage

router = APIRouter()


def _day_bounds(target_date: date_type) -> tuple[datetime, datetime]:
    """Return [start, end) UTC datetime bounds for a given calendar date."""
    start = datetime.combine(target_date, time.min, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    return start, end


def _drop_off_pct(current: int, previous: int) -> float:
    """Calculate percentage drop-off from the previous stage to this one.

    e.g. 120 entries -> 98 zone visits = (120-98)/120 * 100 = 18.33%

    Returns 0.0 for the first stage (no previous stage to compare against)
    or when previous is 0 (avoids division by zero).
    """
    if previous == 0:
        return 0.0
    return round((previous - current) / previous * 100, 2)


@router.get("/stores/{store_id}/funnel", response_model=FunnelResponse)
async def get_store_funnel(
    store_id: str,
    date: Optional[date_type] = Query(
        default=None,
        description="UTC calendar date. Defaults to today (UTC) if not provided.",
    ),
    db: AsyncSession = Depends(get_db),
) -> FunnelResponse:
    """Compute the session-level conversion funnel for one store.

    Each stage counts UNIQUE VISITOR SESSIONS (not raw event counts):
      ENTRY        -- visitors who crossed the entry threshold at least once
      ZONE_VISIT   -- of those, who visited at least one named zone
      BILLING_QUEUE-- of those, who joined the billing queue at least once
      PURCHASE     -- stubbed null (requires POS correlation)

    Re-entry handling: a visitor_id that appears in multiple ENTRY events
    on the same day (re-entry) is still counted as ONE session at the
    ENTRY stage. This prevents re-entry inflation.
    """
    target_date = date or datetime.now(timezone.utc).date()
    start, end = _day_bounds(target_date)

    # Base filter reused across all stage queries
    base_filters = [
        EventRecord.store_id == store_id,
        EventRecord.is_staff.is_(False),
        EventRecord.timestamp >= start,
        EventRecord.timestamp < end,
    ]

    # ── Stage 1: ENTRY ─────────────────────────────────────────────────
    # Count distinct visitor_ids that have at least one ENTRY event.
    # Using COUNT(DISTINCT visitor_id) on ENTRY events naturally handles
    # re-entry: the same visitor_id appearing twice still counts once.
    entry_stmt = select(
        func.count(func.distinct(EventRecord.visitor_id))
    ).where(
        *base_filters,
        EventRecord.event_type == EventType.ENTRY.value,
    )
    entry_count = (await db.execute(entry_stmt)).scalar() or 0

    # ── Stage 2: ZONE_VISIT ────────────────────────────────────────────
    # Count distinct visitor_ids that have at least one ZONE_ENTER event.
    # We do NOT require these visitors to also have an ENTRY event in the
    # same query -- some visitors may have entered before the clip started,
    # so being strict here would under-count zone visits.
    zone_stmt = select(
        func.count(func.distinct(EventRecord.visitor_id))
    ).where(
        *base_filters,
        EventRecord.event_type == EventType.ZONE_ENTER.value,
    )
    zone_count = (await db.execute(zone_stmt)).scalar() or 0

    # Funnel constraint: ZONE_VISIT cannot exceed ENTRY (in a well-formed
    # dataset it won't, but clips with partial coverage could produce this).
    # Cap it to keep drop-off percentages meaningful.
    zone_count = min(zone_count, entry_count)

    # ── Stage 3: BILLING_QUEUE ─────────────────────────────────────────
    # Count distinct visitor_ids that have at least one BILLING_QUEUE_JOIN.
    billing_stmt = select(
        func.count(func.distinct(EventRecord.visitor_id))
    ).where(
        *base_filters,
        EventRecord.event_type == EventType.BILLING_QUEUE_JOIN.value,
    )
    billing_count = (await db.execute(billing_stmt)).scalar() or 0
    billing_count = min(billing_count, zone_count)

    # ── Stage 4: PURCHASE (stubbed) ────────────────────────────────────
    # Requires correlating billing zone dwell times with pos_transactions.csv.
    # Will be implemented as a follow-up step once the POS table is loaded.
    # For now, the PURCHASE stage is omitted from the response rather than
    # returning a misleading 0 -- the stages list will have 3 entries.
    # See CHOICES.md for rationale.
    purchase_count: Optional[int] = None

    # ── Build stages list ──────────────────────────────────────────────
    stages: list[FunnelStage] = [
        FunnelStage(
            stage="ENTRY",
            count=entry_count,
            drop_off_pct=0.0,          # first stage -- no drop-off to compute
        ),
        FunnelStage(
            stage="ZONE_VISIT",
            count=zone_count,
            drop_off_pct=_drop_off_pct(zone_count, entry_count),
        ),
        FunnelStage(
            stage="BILLING_QUEUE",
            count=billing_count,
            drop_off_pct=_drop_off_pct(billing_count, zone_count),
        ),
    ]

    # Only add PURCHASE stage when we have real data for it
    if purchase_count is not None:
        stages.append(
            FunnelStage(
                stage="PURCHASE",
                count=purchase_count,
                drop_off_pct=_drop_off_pct(purchase_count, billing_count),
            )
        )

    return FunnelResponse(
        store_id=store_id,
        date=target_date.isoformat(),
        stages=stages,
        session_unit=True,
        generated_at=datetime.now(timezone.utc),
    )