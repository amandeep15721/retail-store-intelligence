"""
app/anomalies.py

GET /stores/{store_id}/anomalies

Detects three operational anomalies in real time:

  1. BILLING_QUEUE_SPIKE  -- current queue depth exceeds threshold
                             WARN  if depth > 5
                             CRITICAL if depth > 10
  2. DEAD_ZONE            -- a zone with no visits in the last 30 minutes
                             INFO severity
  3. CONVERSION_DROP      -- stubbed (needs POS data, same as /metrics)

Severity levels: INFO / WARN / CRITICAL
Each anomaly includes a `suggested_action` string for on-call engineers.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import EventRecord, get_db
from app.models import AnomalyItem, AnomalyResponse, EventType

router = APIRouter()

# ── Thresholds (easy to tune without touching query logic) ─────────────────
QUEUE_WARN_THRESHOLD = 5
QUEUE_CRITICAL_THRESHOLD = 10
DEAD_ZONE_MINUTES = 30       # a zone with no visits in this window is "dead"


@router.get("/stores/{store_id}/anomalies", response_model=AnomalyResponse)
async def get_store_anomalies(
    store_id: str,
    db: AsyncSession = Depends(get_db),
) -> AnomalyResponse:
    """Detect active operational anomalies for one store.

    Unlike /metrics and /funnel, anomalies are always computed against
    NOW (no date parameter) -- they reflect the current live state of
    the store, not a historical summary.
    """
    now = datetime.now(timezone.utc)
    anomalies: list[AnomalyItem] = []

    # ── Anomaly 1: BILLING_QUEUE_SPIKE ─────────────────────────────────
    # Read the queue_depth from the most recent BILLING_QUEUE_JOIN event.
    # If the depth exceeds our threshold, raise an anomaly.
    latest_queue_stmt = (
        select(EventRecord.event_metadata, EventRecord.timestamp)
        .where(
            EventRecord.store_id == store_id,
            EventRecord.event_type == EventType.BILLING_QUEUE_JOIN.value,
            EventRecord.is_staff.is_(False),
        )
        .order_by(EventRecord.timestamp.desc())
        .limit(1)
    )
    latest_queue_row = (await db.execute(latest_queue_stmt)).first()

    if latest_queue_row is not None:
        metadata, queue_ts = latest_queue_row
        queue_depth = int((metadata or {}).get("queue_depth") or 0)

        if queue_depth > QUEUE_CRITICAL_THRESHOLD:
            anomalies.append(AnomalyItem(
                type="BILLING_QUEUE_SPIKE",
                severity="CRITICAL",
                message=f"Queue depth {queue_depth} exceeds critical threshold of {QUEUE_CRITICAL_THRESHOLD}.",
                suggested_action=(
                    "Immediately deploy additional staff to billing counter. "
                    "Consider opening a second billing lane if available."
                ),
                detected_at=now,
            ))
        elif queue_depth > QUEUE_WARN_THRESHOLD:
            anomalies.append(AnomalyItem(
                type="BILLING_QUEUE_SPIKE",
                severity="WARN",
                message=f"Queue depth {queue_depth} exceeds warning threshold of {QUEUE_WARN_THRESHOLD}.",
                suggested_action=(
                    "Monitor billing counter closely. "
                    "Alert floor staff to redirect customers if queue grows further."
                ),
                detected_at=now,
            ))

    # ── Anomaly 2: DEAD_ZONE ────────────────────────────────────────────
    # Find all zones that had ANY visit today, then check which ones have
    # had NO visit in the last DEAD_ZONE_MINUTES.
    # A zone is only flagged "dead" if it had activity earlier today --
    # zones with zero all-day traffic are not flagged (could be closed).
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    dead_zone_cutoff = now - timedelta(minutes=DEAD_ZONE_MINUTES)

    # zones that had at least one visit today
    all_zones_stmt = (
        select(func.distinct(EventRecord.zone_id))
        .where(
            EventRecord.store_id == store_id,
            EventRecord.event_type == EventType.ZONE_ENTER.value,
            EventRecord.is_staff.is_(False),
            EventRecord.timestamp >= today_start,
            EventRecord.zone_id.isnot(None),
        )
    )
    all_zone_rows = (await db.execute(all_zones_stmt)).all()
    all_zones = {row[0] for row in all_zone_rows if row[0]}

    # zones that had a visit in the last DEAD_ZONE_MINUTES
    active_zones_stmt = (
        select(func.distinct(EventRecord.zone_id))
        .where(
            EventRecord.store_id == store_id,
            EventRecord.event_type == EventType.ZONE_ENTER.value,
            EventRecord.is_staff.is_(False),
            EventRecord.timestamp >= dead_zone_cutoff,
            EventRecord.zone_id.isnot(None),
        )
    )
    active_zone_rows = (await db.execute(active_zones_stmt)).all()
    active_zones = {row[0] for row in active_zone_rows if row[0]}

    # dead zones = had visits today, but NOT in the last 30 minutes
    dead_zones = all_zones - active_zones
    for zone_id in sorted(dead_zones):
        anomalies.append(AnomalyItem(
            type="DEAD_ZONE",
            severity="INFO",
            message=(
                f"Zone '{zone_id}' has had no customer visits "
                f"in the last {DEAD_ZONE_MINUTES} minutes."
            ),
            suggested_action=(
                f"Check if zone '{zone_id}' display is attracting attention. "
                "Consider repositioning signage or notifying floor staff."
            ),
            detected_at=now,
        ))

    # ── Anomaly 3: CONVERSION_DROP (stubbed) ────────────────────────────
    # Requires 7-day average conversion rate from POS data.
    # Will be implemented once pos_transactions table is loaded.
    # Not included in response until real data is available.

    return AnomalyResponse(
        store_id=store_id,
        anomalies=anomalies,
        generated_at=now,
    )