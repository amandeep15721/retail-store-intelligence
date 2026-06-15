"""
app/ingestion.py

POST /events/ingest

Accepts batches of up to 500 raw event payloads. Each event is validated
INDIVIDUALLY against `app.models.Event`. Valid events are deduplicated by
`event_id` (idempotent) and stored; invalid events are reported individually
WITHOUT failing the whole request (partial success).
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field, ValidationError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import EventRecord, get_db
from app.models import Event, IngestErrorDetail, IngestResponse

router = APIRouter()


# ──────────────────────────────────────────────────────────────────────────
# Request shape
# ──────────────────────────────────────────────────────────────────────────

class RawEventBatch(BaseModel):
    """The request body for POST /events/ingest.

    DELIBERATELY LOOSE ON PURPOSE: `events` is `list[dict]`, NOT
    `list[Event]`.

    If we used `list[Event]` directly, FastAPI/Pydantic would reject the
    ENTIRE request with a 422 the moment a SINGLE event fails validation --
    which contradicts the spec's "partial success on malformed events"
    requirement.

    Instead:
      - This model validates the OUTER shape: must be a JSON object with an
        "events" key containing a list of 1-500 objects.
      - Each INNER event is validated against `Event` manually inside the
        endpoint (see below), so we can accept the valid ones and report
        errors for the invalid ones individually.
    """

    model_config = {"extra": "forbid"}

    events: list[dict[str, Any]] = Field(
        ...,
        min_length=1,
        max_length=500,
        description="Batch of raw event objects, 1-500 per request.",
    )


# ──────────────────────────────────────────────────────────────────────────
# Conversion: Pydantic Event (API contract) -> SQLAlchemy EventRecord (storage)
# ──────────────────────────────────────────────────────────────────────────

def event_to_record(event: Event) -> EventRecord:
    """Convert a validated `Event` into an `EventRecord` row.

    See app/database.py for why these are two separate classes.
    """
    return EventRecord(
        event_id=str(event.event_id),
        store_id=event.store_id,
        camera_id=event.camera_id,
        visitor_id=event.visitor_id,
        event_type=event.event_type.value,   # Enum -> plain string for storage
        timestamp=event.timestamp,
        zone_id=event.zone_id,
        dwell_ms=event.dwell_ms,
        is_staff=event.is_staff,
        confidence=event.confidence,
        event_metadata=event.metadata.model_dump(),  # nested model -> dict/JSON
    )


def _format_validation_error(exc: ValidationError) -> str:
    """Turn a Pydantic ValidationError into a single human-readable string.

    e.g. "event_type: Input should be 'ENTRY', 'EXIT', ... ; dwell_ms: ..."
    """
    parts = []
    for err in exc.errors():
        loc = ".".join(str(p) for p in err["loc"])
        parts.append(f"{loc}: {err['msg']}")
    return "; ".join(parts) if parts else str(exc)


# ──────────────────────────────────────────────────────────────────────────
# Endpoint
# ──────────────────────────────────────────────────────────────────────────

@router.post("/events/ingest", response_model=IngestResponse)
async def ingest_events(
    batch: RawEventBatch,
    db: AsyncSession = Depends(get_db),
) -> IngestResponse:
    """Ingest a batch of events.

    Behaviour:
      - Each event in `events` is validated individually against `Event`.
        Events that fail validation are reported in `errors` and counted
        in `rejected`, WITHOUT failing the rest of the batch.
      - Valid events are deduplicated by `event_id`:
          * an event_id already present in the database, OR
          * a repeated event_id within this same batch
        is counted in `duplicates` and NOT re-inserted (idempotency).
      - Everything else is inserted and counted in `accepted`.

    Invariant: accepted + duplicates + rejected == len(batch.events)
    """
    errors: list[IngestErrorDetail] = []
    valid_events: list[Event] = []

    # ── Step 1: validate each event individually ──────────────────────
    for index, raw_event in enumerate(batch.events):
        try:
            event = Event.model_validate(raw_event)
        except ValidationError as exc:
            errors.append(
                IngestErrorDetail(
                    index=index,
                    event_id=str(raw_event.get("event_id"))
                    if isinstance(raw_event, dict) and raw_event.get("event_id")
                    else None,
                    error=_format_validation_error(exc),
                )
            )
            continue
        valid_events.append(event)

    # ── Step 2: find which event_ids already exist (idempotency check) ─
    incoming_ids = [str(e.event_id) for e in valid_events]

    existing_ids: set[str] = set()
    if incoming_ids:
        result = await db.execute(
            select(EventRecord.event_id).where(EventRecord.event_id.in_(incoming_ids))
        )
        existing_ids = {row[0] for row in result.all()}

    # ── Step 3: insert new events; skip DB duplicates AND in-batch dupes ─
    accepted = 0
    duplicates = 0
    seen_in_batch: set[str] = set()

    for event in valid_events:
        eid = str(event.event_id)
        if eid in existing_ids or eid in seen_in_batch:
            duplicates += 1
            continue
        seen_in_batch.add(eid)
        db.add(event_to_record(event))
        accepted += 1

    await db.commit()

    return IngestResponse(
        accepted=accepted,
        duplicates=duplicates,
        rejected=len(errors),
        errors=errors,
    )