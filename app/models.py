"""
app/models.py

Core event schema for the Store Intelligence pipeline.

This is the single source of truth for the event contract between:
  - the detection pipeline (pipeline/emit.py)   -> produces events
  - the ingestion API       (app/ingestion.py)  -> validates + stores events
  - the analytics layer      (app/metrics.py, funnel.py, heatmap.py, anomalies.py)

Every event emitted by the detection pipeline MUST validate against `Event`
below. Run `pipeline/validate_against_samples.py` against
`sample_events.jsonl` to confirm compliance before wiring up the rest of the
system.
"""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Optional
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, field_validator, model_validator


# ──────────────────────────────────────────────────────────────────────────
# Event type catalogue
# ──────────────────────────────────────────────────────────────────────────

class EventType(str, Enum):
    """All event types the detection pipeline may emit.

    See the problem statement's "Event Type Catalogue" table for the precise
    semantics of when each event must be emitted.
    """

    ENTRY = "ENTRY"                                   # inbound crossing of entry threshold -> new session
    EXIT = "EXIT"                                     # outbound crossing of entry threshold -> closes session
    ZONE_ENTER = "ZONE_ENTER"                         # visitor enters a named zone
    ZONE_EXIT = "ZONE_EXIT"                           # visitor leaves a named zone
    ZONE_DWELL = "ZONE_DWELL"                         # emitted every 30s of continuous dwell in a zone
    BILLING_QUEUE_JOIN = "BILLING_QUEUE_JOIN"         # visitor enters billing zone while queue_depth > 0
    BILLING_QUEUE_ABANDON = "BILLING_QUEUE_ABANDON"   # visitor leaves billing zone before a POS txn follows
    REENTRY = "REENTRY"                               # same visitor_id detected again after a prior EXIT


# Event types that represent an *instant* (zero duration).
# Used by validators / the pipeline to decide whether dwell_ms must be 0.
INSTANTANEOUS_EVENT_TYPES = {
    EventType.ENTRY,
    EventType.EXIT,
    EventType.ZONE_ENTER,
    EventType.ZONE_EXIT,
    EventType.BILLING_QUEUE_JOIN,
    EventType.BILLING_QUEUE_ABANDON,
    EventType.REENTRY,
}

# Event types that do NOT carry a zone_id (entry/exit threshold events).
ZONELESS_EVENT_TYPES = {
    EventType.ENTRY,
    EventType.EXIT,
    EventType.REENTRY,
}


# ──────────────────────────────────────────────────────────────────────────
# Metadata sub-object
# ──────────────────────────────────────────────────────────────────────────

class EventMetadata(BaseModel):
    """The `metadata` block inside an Event.

    All fields are optional because their relevance depends on event_type:
      - queue_depth : populated for BILLING_QUEUE_JOIN, else null
      - sku_zone    : the product/SKU label associated with zone_id
                      (from store_layout.json), if applicable
      - session_seq : ordinal position of this event within the visitor's
                      session (1-indexed). Required for every event so that
                      session reconstruction / funnel logic is deterministic.
    """

    model_config = {"extra": "allow"}  # tolerate extra fields without failing validation

    queue_depth: Optional[int] = Field(
        default=None,
        ge=0,
        description="Integer queue depth at time of event. Populated for "
                     "BILLING_QUEUE_JOIN; null otherwise.",
    )
    sku_zone: Optional[str] = Field(
        default=None,
        description="Zone/SKU label from store_layout.json, e.g. 'MOISTURISER'.",
    )
    session_seq: int = Field(
        ...,
        ge=1,
        description="Ordinal position of this event in the visitor's session "
                    "(1 = first event of the session).",
    )

    @field_validator("queue_depth")
    @classmethod
    def _queue_depth_non_negative(cls, v: Optional[int]) -> Optional[int]:
        if v is not None and v < 0:
            raise ValueError("queue_depth must be >= 0")
        return v


# ──────────────────────────────────────────────────────────────────────────
# Core Event model
# ──────────────────────────────────────────────────────────────────────────

class Event(BaseModel):
    """A single structured behavioural event emitted by the detection pipeline.

    This matches the required output schema in the problem statement exactly
    (field names, types, and semantics). Field-level docstrings mirror the
    inline comments from the spec.
    """

    model_config = {
        "extra": "forbid",       # reject unknown top-level fields -> catches schema drift early
        "use_enum_values": False,  # keep EventType as an Enum instance internally
    }

    event_id: UUID = Field(
        default_factory=uuid4,
        description="Globally unique UUIDv4 identifying this event. "
                     "Generated by the pipeline (or server, on ingest).",
    )
    store_id: str = Field(
        ...,
        min_length=1,
        description="Store identifier from store_layout.json, e.g. 'STORE_BLR_002'.",
    )
    camera_id: str = Field(
        ...,
        min_length=1,
        description="Which camera produced this event, e.g. 'CAM_ENTRY_01'.",
    )
    visitor_id: str = Field(
        ...,
        min_length=1,
        description="Re-ID token, unique per visit session, e.g. 'VIS_c8a2f1'.",
    )
    event_type: EventType = Field(
        ...,
        description="The type of behavioural event. See EventType enum.",
    )
    timestamp: datetime = Field(
        ...,
        description="ISO-8601 UTC timestamp, derived from clip start time + "
                     "frame offset.",
    )
    zone_id: Optional[str] = Field(
        default=None,
        description="Zone name from store_layout.json. Null for ENTRY / EXIT "
                     "/ REENTRY events (which occur at the entry threshold, "
                     "not inside a named zone).",
    )
    dwell_ms: int = Field(
        ...,
        ge=0,
        description="Duration in milliseconds. 0 for instantaneous events "
                     "(ENTRY, EXIT, ZONE_ENTER, ZONE_EXIT, BILLING_QUEUE_*, "
                     "REENTRY). Positive for ZONE_DWELL.",
    )
    is_staff: bool = Field(
        ...,
        description="True if the detection pipeline classified this person "
                     "as staff (e.g. by uniform colour). Staff are excluded "
                     "from customer-facing metrics.",
    )
    confidence: float = Field(
        ...,
        ge=0.0,
        le=1.0,
        description="Detection confidence in [0, 1]. Low-confidence events "
                     "must still be emitted, not suppressed -- downstream "
                     "consumers decide how to treat them.",
    )
    metadata: EventMetadata = Field(
        ...,
        description="Structured metadata -- see EventMetadata.",
    )

    # ── Validators ──────────────────────────────────────────────────────

    @field_validator("timestamp")
    @classmethod
    def _timestamp_must_be_utc(cls, v: datetime) -> datetime:
        """Normalise naive timestamps to UTC and reject timezone-aware
        timestamps that aren't UTC, so every stored timestamp is
        unambiguous and comparable."""
        if v.tzinfo is None:
            return v.replace(tzinfo=timezone.utc)
        return v.astimezone(timezone.utc)

    @model_validator(mode="after")
    def _validate_zone_consistency(self) -> "Event":
        """Cross-field checks tying event_type to zone_id / dwell_ms.

        - ENTRY / EXIT / REENTRY happen at the threshold, not in a zone
          -> zone_id must be None.
        - ZONE_DWELL must carry a positive dwell_ms (>= 30000 ms per spec,
          since it's emitted every 30s of continuous dwell).
        - Instantaneous event types must have dwell_ms == 0.
        """
        if self.event_type in ZONELESS_EVENT_TYPES and self.zone_id is not None:
            raise ValueError(
                f"{self.event_type.value} events must have zone_id=null "
                f"(got zone_id={self.zone_id!r})"
            )

        if self.event_type == EventType.ZONE_DWELL:
            if self.dwell_ms < 30_000:
                raise ValueError(
                    "ZONE_DWELL events must have dwell_ms >= 30000 "
                    "(emitted every 30s of continued dwell)"
                )
        elif self.event_type in INSTANTANEOUS_EVENT_TYPES:
            if self.dwell_ms != 0:
                raise ValueError(
                    f"{self.event_type.value} is instantaneous and must have "
                    f"dwell_ms=0 (got {self.dwell_ms})"
                )

        if self.event_type == EventType.BILLING_QUEUE_JOIN:
            if self.metadata.queue_depth is None:
                raise ValueError(
                    "BILLING_QUEUE_JOIN events must set metadata.queue_depth"
                )

        return self

    # ── Convenience helpers ─────────────────────────────────────────────

    def is_session_boundary(self) -> bool:
        """True if this event starts (ENTRY) or ends (EXIT) a session."""
        return self.event_type in (EventType.ENTRY, EventType.EXIT)


# ──────────────────────────────────────────────────────────────────────────
# Batch ingestion wrapper (used by POST /events/ingest)
# ──────────────────────────────────────────────────────────────────────────

class EventBatch(BaseModel):
    """Request body for POST /events/ingest.

    Accepts up to 500 events per call (per spec). Validation of individual
    events happens per-item in ingestion.py so that malformed events can be
    reported individually without failing the whole batch (partial success).
    """

    model_config = {"extra": "forbid"}

    events: list[Event] = Field(
        ...,
        min_length=1,
        max_length=500,
        description="Batch of events to ingest. Max 500 per request.",
    )


class IngestErrorDetail(BaseModel):
    """Structured error info for a single malformed event in a batch."""

    index: int = Field(..., description="Position of the malformed event in the request batch.")
    event_id: Optional[str] = Field(default=None, description="event_id if it could be parsed, else null.")
    error: str = Field(..., description="Human-readable validation error message.")


class IngestResponse(BaseModel):
    """Response body for POST /events/ingest.

    Supports partial success: some events may be accepted while others are
    rejected, without the whole request failing with a 5xx.
    """

    accepted: int = Field(..., description="Number of events successfully stored.")
    duplicates: int = Field(..., description="Number of events skipped because event_id already existed.")
    rejected: int = Field(..., description="Number of events that failed validation.")
    errors: list[IngestErrorDetail] = Field(
        default_factory=list,
        description="Details for each rejected event.",
    )
    
    
    
 
# ──────────────────────────────────────────────────────────────────────────
# GET /stores/{id}/metrics response models
# ──────────────────────────────────────────────────────────────────────────
 
class ZoneDwell(BaseModel):
    """Average dwell time for one zone, used in MetricsResponse.avg_dwell_per_zone."""
 
    zone_id: str
    avg_dwell_ms: float = Field(
        ..., ge=0,
        description="Average dwell_ms across ZONE_DWELL events for this zone.",
    )
    sample_count: int = Field(
        ..., ge=0,
        description="Number of ZONE_DWELL events this average is based on.",
    )
 
 
class MetricsResponse(BaseModel):
    """Response body for GET /stores/{id}/metrics.
 
    NOTE: `conversion_rate` is currently always `null`. Computing it
    requires correlating visitor sessions with pos_transactions.csv (a
    visitor in the billing zone within 5 minutes before a POS transaction
    counts as converted). This is a deliberate incremental step -- see
    CHOICES.md for the rationale -- and will be filled in once the POS
    table + correlation logic is added.
    """
 
    store_id: str
    date: str = Field(..., description="ISO-8601 date (YYYY-MM-DD) these metrics cover, UTC.")
    unique_visitors: int = Field(
        ..., ge=0,
        description="Distinct non-staff visitor_ids with activity on this date.",
    )
    conversion_rate: Optional[float] = Field(
        default=None,
        description="visitors who purchased / unique_visitors. null until POS correlation is implemented.",
    )
    avg_dwell_per_zone: list[ZoneDwell] = Field(default_factory=list)
    queue_depth: int = Field(
        ..., ge=0,
        description="Most recently observed billing queue depth; 0 if no queue activity.",
    )
    abandonment_rate: float = Field(
        ..., ge=0.0, le=1.0,
        description="BILLING_QUEUE_ABANDON / BILLING_QUEUE_JOIN; 0.0 if there were no queue joins.",
    )
    generated_at: datetime = Field(
        ..., description="When this response was computed -- demonstrates it's real-time, not cached.",
    )
    
    
    
    
# ──────────────────────────────────────────────────────────────────────────
# GET /stores/{id}/funnel response models
# ──────────────────────────────────────────────────────────────────────────
 
class FunnelStage(BaseModel):
    """One stage in the conversion funnel."""
 
    stage: str = Field(..., description="Stage name: ENTRY, ZONE_VISIT, BILLING_QUEUE, PURCHASE")
    count: int = Field(..., ge=0, description="Number of unique sessions that reached this stage.")
    drop_off_pct: float = Field(
        ..., ge=0.0, le=100.0,
        description="Percentage of sessions lost vs the PREVIOUS stage. 0.0 for the first stage.",
    )
 
 
class FunnelResponse(BaseModel):
    """Response body for GET /stores/{id}/funnel."""
 
    store_id: str
    date: str
    stages: list[FunnelStage] = Field(default_factory=list)
    session_unit: bool = Field(
        default=True,
        description="Always true -- counts are per unique visitor session, not raw events.",
    )
    generated_at: datetime
    


# ──────────────────────────────────────────────────────────────────────────
# GET /stores/{id}/heatmap response models
# ──────────────────────────────────────────────────────────────────────────
 
class HeatmapZone(BaseModel):
    """Heatmap data for one zone."""
 
    zone_id: str
    visit_count: int = Field(..., ge=0, description="Distinct visitors who entered this zone.")
    avg_dwell_ms: float = Field(..., ge=0, description="Average dwell time in milliseconds.")
    heat_score: float = Field(..., ge=0.0, le=100.0, description="Normalised 0-100 score across all zones.")
    data_confidence: bool = Field(
        ...,
        description="False if fewer than 20 sessions contributed -- treat score with caution.",
    )
 
 
class HeatmapResponse(BaseModel):
    """Response body for GET /stores/{id}/heatmap."""
 
    store_id: str
    date: str
    zones: list[HeatmapZone] = Field(default_factory=list)
    generated_at: datetime