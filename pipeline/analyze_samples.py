"""
pipeline/analyze_samples.py

PURPOSE
-------
`sample_events.jsonl` (provided by Purplle) is in a DIFFERENT format from the
"Required Output Schema" defined in the problem statement / app/models.py.

This script:
  1. Loads the raw sample events (whatever shape they're in).
  2. Groups them by their raw `event_type` to show you the variety of shapes.
  3. Demonstrates a NORMALIZATION layer that maps each raw event into our
     canonical `app.models.Event` schema.
  4. Reports which fields could be mapped directly, which had to be derived/
     defaulted, and which raw fields were dropped.

This is meant to be read, not just run -- it documents the mapping decisions
you'll want to carry into `pipeline/emit.py` and into CHOICES.md
("event schema design rationale").

USAGE
-----
    python pipeline/analyze_samples.py path/to/sample_events.jsonl
"""

from __future__ import annotations

import json
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

# allow running this script directly from pipeline/ as well as project root
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.models import Event, EventMetadata, EventType  # noqa: E402


# ──────────────────────────────────────────────────────────────────────────
# Step 1: Inspect the raw shapes
# ──────────────────────────────────────────────────────────────────────────

def load_raw_events(path: Path) -> list[dict]:
    events = []
    with open(path, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                events.append(json.loads(line))
            except json.JSONDecodeError as e:
                print(f"  ! line {line_num}: invalid JSON ({e})")
    return events


def summarize_raw_shapes(raw_events: list[dict]) -> None:
    print("=" * 70)
    print("RAW SHAPE SUMMARY")
    print("=" * 70)

    by_type: dict[str, list[dict]] = defaultdict(list)
    for ev in raw_events:
        by_type[ev.get("event_type", "<missing>")].append(ev)

    print(f"\nTotal raw events: {len(raw_events)}")
    print(f"Distinct raw event_type values: {sorted(by_type.keys())}\n")

    for raw_type, events in sorted(by_type.items()):
        all_keys: set[str] = set()
        for ev in events:
            all_keys.update(ev.keys())
        print(f"  '{raw_type}'  (count={len(events)})")
        print(f"      fields: {sorted(all_keys)}")
    print()


# ──────────────────────────────────────────────────────────────────────────
# Step 2: Normalization layer -- raw sample format -> canonical Event
# ──────────────────────────────────────────────────────────────────────────

# Mapping from raw `event_type` strings -> our EventType enum.
# NOTE: this is a STARTING POINT based on the 200-event sample. Your real
# pipeline (emit.py) generates ALL of these directly from detection/tracking
# logic -- it never needs to "translate" sample data. This mapping exists
# purely so we can sanity-check our schema design against the sample shapes.
RAW_TYPE_MAP = {
    "entry": EventType.ENTRY,
    "exit": EventType.EXIT,
    "zone_entered": EventType.ZONE_ENTER,
    "zone_exited": EventType.ZONE_EXIT,
    "queue_completed": EventType.BILLING_QUEUE_JOIN,
    "queue_abandoned": EventType.BILLING_QUEUE_ABANDON,
    # not present in the sample but part of our schema:
    #   ZONE_DWELL  -> derived by OUR pipeline from consecutive
    #                  zone_entered/zone_exited pairs (30s+ dwell)
    #   REENTRY     -> derived by OUR pipeline's Re-ID logic when a
    #                  visitor_id reappears after a prior EXIT
}


def _parse_ts(raw: dict, *candidates: str) -> datetime:
    """Pick the first present timestamp-like field and parse it to UTC."""
    for key in candidates:
        if raw.get(key):
            ts = datetime.fromisoformat(raw[key])
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            return ts.astimezone(timezone.utc)
    raise ValueError(f"No timestamp field found among {candidates} in {raw}")


def normalize_event(raw: dict, session_seq_counter: Counter) -> Event | None:
    """Convert one raw sample-format event into a canonical `Event`.

    Returns None if the raw event_type has no mapping (so the caller can
    report it as "unmapped" rather than crashing).
    """
    raw_type = raw.get("event_type")
    mapped_type = RAW_TYPE_MAP.get(raw_type)
    if mapped_type is None:
        return None

    # visitor_id: raw uses either `id_token` (entry/exit) or `track_id`
    # (zone/queue events). Our canonical schema uses ONE visitor_id per
    # session -- in a real pipeline this comes from your Re-ID/tracker,
    # not from two different raw fields. Here we just normalize the field
    # name so the SHAPE matches; the VALUE mapping (track_id <-> id_token)
    # is a tracking-layer concern, not a schema concern.
    visitor_id = raw.get("id_token")
    if visitor_id is None and raw.get("track_id") is not None:
        visitor_id = f"TRACK_{raw['track_id']}"

    # store_id: raw uses `store_code` (e.g. "store_1076") or `store_id`
    # (e.g. "ST1076"). Both need mapping to the store_layout.json IDs
    # (e.g. "STORE_BLR_002") -- that lookup table lives in store_layout.json,
    # not in the event schema itself.
    store_id = raw.get("store_id") or raw.get("store_code") or "UNKNOWN_STORE"

    # camera_id: present in both formats, sometimes a short code
    # ("cam1") sometimes a full ID ("PURPLLE_MUM_1076_CAM6") -- pass through.
    camera_id = str(raw.get("camera_id", "UNKNOWN_CAM"))

    # timestamp: raw events use different field names depending on type.
    if mapped_type in (EventType.BILLING_QUEUE_JOIN,):
        timestamp = _parse_ts(raw, "queue_join_ts")
    elif mapped_type == EventType.BILLING_QUEUE_ABANDON:
        timestamp = _parse_ts(raw, "queue_exit_ts", "queue_join_ts")
    else:
        timestamp = _parse_ts(raw, "event_timestamp", "event_time")

    # zone_id: only present for zone/queue events; ENTRY/EXIT have none.
    zone_id = None
    if mapped_type not in (EventType.ENTRY, EventType.EXIT, EventType.REENTRY):
        zone_id = raw.get("zone_id")

    # dwell_ms:
    #   - ENTRY/EXIT/ZONE_ENTER/ZONE_EXIT -> 0 (instantaneous, per schema)
    #   - BILLING_QUEUE_JOIN/ABANDON -> derive from wait_seconds if present
    if mapped_type in (EventType.BILLING_QUEUE_JOIN, EventType.BILLING_QUEUE_ABANDON):
        dwell_ms = int(raw.get("wait_seconds", 0)) * 1000
    else:
        dwell_ms = 0

    # is_staff: present directly only on entry/exit in the sample.
    # zone/queue events don't carry it -- in OUR pipeline, is_staff is a
    # per-VISITOR attribute decided once (e.g. by the staff classifier) and
    # propagated to every event in that visitor's session.
    is_staff = bool(raw.get("is_staff", False))

    # confidence: NOT PRESENT in the raw sample at all. Our schema requires
    # it. In the real pipeline this comes from the detector's per-detection
    # confidence score. Here we default to 1.0 with a clear comment --
    # this is a sample-data limitation, not a schema gap.
    confidence = 1.0

    # metadata.queue_depth: only meaningful for BILLING_QUEUE_JOIN.
    queue_depth = None
    if mapped_type == EventType.BILLING_QUEUE_JOIN:
        queue_depth = raw.get("queue_position_at_join")

    # metadata.sku_zone: raw `zone_name` is the closest analogue.
    sku_zone = raw.get("zone_name")

    # metadata.session_seq: NOT PRESENT in raw sample. This is a
    # PER-SESSION ordinal that only OUR pipeline can compute, because it
    # requires tracking each visitor's full event sequence over time.
    # Here we fake it with a simple per-visitor counter for demo purposes.
    session_seq_counter[visitor_id] += 1
    session_seq = session_seq_counter[visitor_id]

    return Event(
        store_id=store_id,
        camera_id=camera_id,
        visitor_id=visitor_id or "UNKNOWN_VISITOR",
        event_type=mapped_type,
        timestamp=timestamp,
        zone_id=zone_id,
        dwell_ms=dwell_ms,
        is_staff=is_staff,
        confidence=confidence,
        metadata=EventMetadata(
            queue_depth=queue_depth,
            sku_zone=sku_zone,
            session_seq=session_seq,
        ),
    )


# ──────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────

def main(path: Path) -> None:
    raw_events = load_raw_events(path)
    summarize_raw_shapes(raw_events)

    print("=" * 70)
    print("NORMALIZATION DEMO (raw sample format -> canonical Event)")
    print("=" * 70)

    session_seq_counter: Counter = Counter()
    mapped, unmapped = 0, 0
    unmapped_types: set[str] = set()

    for i, raw in enumerate(raw_events, start=1):
        try:
            event = normalize_event(raw, session_seq_counter)
        except Exception as e:
            print(f"  ! line {i}: normalization error -> {e}")
            continue

        if event is None:
            unmapped += 1
            unmapped_types.add(raw.get("event_type", "<missing>"))
            continue

        mapped += 1
        if i <= 3 or i in (5, 11, 13):  # show a few representative examples
            print(f"\n  line {i} ({raw.get('event_type')}):")
            print(f"    raw   : {json.dumps(raw)[:160]}...")
            print(f"    mapped: {event.model_dump_json()}")

    print("\n" + "-" * 70)
    print(f"Mapped successfully : {mapped}")
    print(f"No mapping defined  : {unmapped}  (types: {sorted(unmapped_types)})")
    print("-" * 70)

    print("""
SUMMARY OF FINDINGS FOR CHOICES.md / DESIGN.md
-----------------------------------------------
1. sample_events.jsonl uses a different, more granular "raw detection
   signal" format than the canonical Event schema in the problem statement.
2. Fields that map directly: event_type (with renaming), camera_id,
   zone_id, zone_name->sku_zone, timestamps.
3. Fields that require DERIVATION by our pipeline (not present in raw
   sample): event_id (generate), confidence (from detector), dwell_ms
   for ZONE_DWELL (computed from zone_entered/zone_exited pairs),
   session_seq (running counter per visitor session), REENTRY (Re-ID
   logic comparing against prior EXIT events).
4. Fields present in raw sample but NOT in canonical schema (dropped):
   gender_pred, age_pred, age_bucket, is_face_hidden, group_id,
   group_size, zone_hotspot_x/y, zone_type, is_revenue_zone,
   queue_served_ts, abandoned, queue_event_id.
   -> These are useful SIGNALS for the detection layer (e.g. group_id
      helps with "group entry" edge case, is_face_hidden could inform
      confidence) but are not part of the API contract.
5. DECISION: our pipeline emit.py treats the canonical Event schema
   (app/models.py) as the ONLY output contract. Any raw-format fields
   are consumed internally by detect.py/tracker.py and never exposed
   past emit.py.
""")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python pipeline/analyze_samples.py path/to/sample_events.jsonl")
        sys.exit(1)
    main(Path(sys.argv[1]))