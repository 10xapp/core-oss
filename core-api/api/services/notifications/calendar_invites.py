"""Calendar invite notification reconciliation for sync-driven calendar events."""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timezone
import logging
from typing import Any, Dict, List, Mapping, Optional, Sequence

from api.services.notifications.create import NotificationType
from lib.supabase_client import get_service_role_client

logger = logging.getLogger(__name__)

ACTIONABLE_RESPONSE_STATUSES = frozenset({"needs_action", "tentative"})
CANCELLED_EVENT_STATUSES = frozenset({"cancelled", "canceled"})


@dataclass(frozen=True)
class CalendarInviteSnapshot:
    """Actionable invite state for a synced calendar event row."""

    event_id: str
    event_title: str
    starts_at: str
    organizer_name: str
    response_status: str
    account_email: Optional[str] = None
    recurring_event_id: Optional[str] = None

    @property
    def notification_key_id(self) -> str:
        """ID used for grouping — recurring series ID if available, else instance ID."""
        return self.recurring_event_id or self.event_id


def _normalize_email(value: Any) -> Optional[str]:
    if not isinstance(value, str):
        return None
    normalized = value.strip().lower()
    return normalized or None


def _normalize_response_status(value: Any) -> Optional[str]:
    if not isinstance(value, str):
        return None

    normalized = value.strip().replace("_", "").replace("-", "").lower()
    mapping = {
        "accepted": "accepted",
        "declined": "declined",
        "needsaction": "needs_action",
        "none": "needs_action",
        "notresponded": "needs_action",
        "organizer": "accepted",
        "tentative": "tentative",
        "tentativelyaccepted": "tentative",
    }
    return mapping.get(normalized)


def _extract_account_attendee(raw_item: Mapping[str, Any], account_email: Optional[str]) -> Optional[Dict[str, Any]]:
    attendees = raw_item.get("attendees")
    if not isinstance(attendees, list):
        return None

    normalized_account_email = _normalize_email(account_email)

    for attendee in attendees:
        if not isinstance(attendee, dict):
            continue

        attendee_email = _normalize_email(attendee.get("email"))
        email_address = attendee.get("emailAddress")
        if attendee_email is None and isinstance(email_address, dict):
            attendee_email = _normalize_email(email_address.get("address"))

        is_self = attendee.get("self") is True
        if not is_self and normalized_account_email is not None and attendee_email != normalized_account_email:
            continue
        if not is_self and normalized_account_email is None:
            continue

        response_status = attendee.get("responseStatus")
        status_obj = attendee.get("status")
        if response_status is None and isinstance(status_obj, dict):
            response_status = status_obj.get("response")

        display_name = attendee.get("displayName")
        if display_name is None and isinstance(email_address, dict):
            display_name = email_address.get("name")

        return {
            "email": attendee_email or normalized_account_email,
            "display_name": display_name,
            "response_status": _normalize_response_status(response_status),
        }

    return None


def _extract_organizer_name(event_row: Mapping[str, Any]) -> str:
    raw_item = event_row.get("raw_item")
    organizer = raw_item.get("organizer") if isinstance(raw_item, dict) else None

    if isinstance(organizer, dict):
        display_name = organizer.get("displayName")
        if isinstance(display_name, str) and display_name.strip():
            return display_name.strip()

        email_address = organizer.get("emailAddress")
        if isinstance(email_address, dict):
            name = email_address.get("name")
            if isinstance(name, str) and name.strip():
                return name.strip()

            address = _normalize_email(email_address.get("address"))
            if address:
                return address

        email = _normalize_email(organizer.get("email"))
        if email:
            return email

    organizer_email = _normalize_email(event_row.get("organizer_email"))
    return organizer_email or "Someone"


def _to_iso_string(value: Any) -> str:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, str):
        return value
    return ""


def build_calendar_invite_snapshot(
    event_row: Optional[Mapping[str, Any]],
    account_email: Optional[str],
) -> Optional[CalendarInviteSnapshot]:
    """Return actionable invite state for a calendar row, or ``None``."""
    if not event_row or not event_row.get("id"):
        return None

    event_status = str(event_row.get("status") or "").strip().lower()
    if event_status in CANCELLED_EVENT_STATUSES:
        return None

    if event_row.get("is_organizer") is True:
        return None

    raw_item = event_row.get("raw_item")
    attendee = _extract_account_attendee(raw_item, account_email) if isinstance(raw_item, dict) else None
    response_status = attendee.get("response_status") if attendee else None
    if response_status not in ACTIONABLE_RESPONSE_STATUSES:
        return None

    starts_at = _to_iso_string(event_row.get("start_time"))
    if not starts_at:
        return None

    title = event_row.get("title")
    if not isinstance(title, str) or not title.strip():
        title = "(No title)"

    recurring_event_id = event_row.get("recurring_event_id")
    if not isinstance(recurring_event_id, str) or not recurring_event_id.strip():
        recurring_event_id = None

    return CalendarInviteSnapshot(
        event_id=str(event_row["id"]),
        event_title=title.strip(),
        starts_at=starts_at,
        organizer_name=_extract_organizer_name(event_row),
        response_status=response_status,
        account_email=_normalize_email(account_email),
        recurring_event_id=recurring_event_id,
    )


def get_calendar_event_rows_by_external_ids(
    *,
    client,
    user_id: str,
    external_ids: Sequence[str],
    connection_id: Optional[str] = None,
) -> Dict[str, Dict[str, Any]]:
    """Load calendar event rows keyed by external provider ID."""
    unique_external_ids = [external_id for external_id in dict.fromkeys(external_ids) if external_id]
    if not unique_external_ids:
        return {}

    query = client.table("calendar_events").select("*").eq("user_id", user_id)
    if connection_id:
        query = query.eq("ext_connection_id", connection_id)

    if len(unique_external_ids) == 1:
        query = query.eq("external_id", unique_external_ids[0])
    else:
        query = query.in_("external_id", unique_external_ids)

    result = query.execute()
    rows = result.data or []
    return {
        row["external_id"]: row
        for row in rows
        if isinstance(row, dict) and row.get("external_id")
    }


def _group_key(calendar_event_id: str) -> str:
    return f"calendar:{calendar_event_id}"


def _preference_allows_calendar_notifications(client, user_id: str) -> bool:
    result = client.table("notification_preferences") \
        .select("*") \
        .eq("user_id", user_id) \
        .is_("workspace_id", "null") \
        .eq("category", "calendar") \
        .maybe_single() \
        .execute()

    pref = result.data if result is not None else None
    if not pref:
        return True

    muted_until = pref.get("muted_until")
    if isinstance(muted_until, str):
        try:
            muted_until_dt = datetime.fromisoformat(muted_until.replace("Z", "+00:00"))
            if muted_until_dt > datetime.now(timezone.utc):
                return False
        except ValueError:
            logger.warning("Invalid muted_until on calendar notification preference: %s", muted_until)

    return pref.get("in_app", True)


def _build_notification_title(snapshot: CalendarInviteSnapshot) -> str:
    if snapshot.organizer_name and snapshot.organizer_name != "Someone":
        return f'{snapshot.organizer_name} invited you to "{snapshot.event_title}"'
    return f'Calendar invite: "{snapshot.event_title}"'


def _build_notification_body(snapshot: CalendarInviteSnapshot) -> str:
    if snapshot.response_status == "tentative":
        return "Tentatively accepted"
    return "Awaiting your response"


def _notification_payload(snapshot: CalendarInviteSnapshot) -> Dict[str, Any]:
    return {
        "event_id": snapshot.event_id,
        "event_title": snapshot.event_title,
        "starts_at": snapshot.starts_at,
        "organizer_name": snapshot.organizer_name,
        "account_email": snapshot.account_email,
        "workspace_id": None,
        "actor_name": snapshot.organizer_name,
        "response_status": snapshot.response_status,
    }


def _notification_state_by_group_key(
    client,
    *,
    user_id: str,
    calendar_event_ids: Sequence[str],
) -> Dict[str, Dict[str, Any]]:
    group_keys = [_group_key(event_id) for event_id in dict.fromkeys(calendar_event_ids) if event_id]
    if not group_keys:
        return {}

    query = client.table("notifications") \
        .select("group_key, archived, data") \
        .eq("user_id", user_id) \
        .eq("type", NotificationType.CALENDAR_INVITE)

    if len(group_keys) == 1:
        query = query.eq("group_key", group_keys[0])
    else:
        query = query.in_("group_key", group_keys)

    last_err: Optional[Exception] = None
    for attempt in range(1, 4):
        try:
            result = query.execute()
            break
        except Exception as e:
            last_err = e
            if attempt < 3:
                logger.warning(f"⚠️ Notification state query failed (attempt {attempt}/3): {e}")
                time.sleep(0.5 * attempt)
            else:
                raise last_err

    presence: Dict[str, Dict[str, Any]] = {}
    for row in result.data or []:
        if not isinstance(row, dict):
            continue
        group_key = row.get("group_key")
        if not isinstance(group_key, str):
            continue
        state = presence.setdefault(group_key, {"any": False, "active": None})
        state["any"] = True
        if row.get("archived") is False:
            state["active"] = row
    return presence


def _notification_matches_snapshot(notification_row: Mapping[str, Any], snapshot: CalendarInviteSnapshot) -> bool:
    data = notification_row.get("data")
    if not isinstance(data, Mapping):
        return False

    # Reconciliation already collapses recurring events down to one
    # representative snapshot per series. Compare the representative
    # instance fields so the notification advances when that choice changes.
    return (
        data.get("event_id") == snapshot.event_id
        and data.get("event_title") == snapshot.event_title
        and data.get("starts_at") == snapshot.starts_at
        and data.get("organizer_name") == snapshot.organizer_name
        and data.get("account_email") == snapshot.account_email
        and data.get("response_status") == snapshot.response_status
    )


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _upsert_calendar_invite_notification(client, *, user_id: str, snapshot: CalendarInviteSnapshot) -> Optional[Dict[str, Any]]:
    if not _preference_allows_calendar_notifications(client, user_id):
        logger.info("Skipping calendar invite notification due to preferences user_id=%s event_id=%s", user_id, snapshot.event_id)
        return None

    group_key = _group_key(snapshot.notification_key_id)
    payload = _notification_payload(snapshot)
    title = _build_notification_title(snapshot)
    body = _build_notification_body(snapshot)

    # UPDATE existing active notification — only change content, preserve
    # the user's read/seen state so recurring events don't re-appear as
    # new notifications every sync cycle.
    update_payload = {
        "workspace_id": None,
        "resource_type": "calendar_event",
        "resource_id": snapshot.event_id,
        "actor_id": None,
        "title": title,
        "body": body,
        "data": payload,
    }

    updated = client.table("notifications") \
        .update(update_payload) \
        .eq("user_id", user_id) \
        .eq("type", NotificationType.CALENDAR_INVITE) \
        .eq("group_key", group_key) \
        .eq("archived", False) \
        .execute()

    if updated.data:
        return updated.data[0]

    # No active notification exists — INSERT a new one with unread state.
    now = _now_iso()
    insert_payload = {
        "user_id": user_id,
        "workspace_id": None,
        "type": NotificationType.CALENDAR_INVITE,
        "title": title,
        "body": body,
        "resource_type": "calendar_event",
        "resource_id": snapshot.event_id,
        "actor_id": None,
        "data": payload,
        "group_key": group_key,
        "read": False,
        "seen": False,
        "archived": False,
        "created_at": now,
    }

    try:
        inserted = client.table("notifications").insert(insert_payload).execute()
        return inserted.data[0] if inserted.data else {}
    except Exception as err:
        if "duplicate key value" not in str(err).lower():
            raise

        logger.info(
            "Calendar invite notification insert raced with another writer user_id=%s event_id=%s",
            user_id,
            snapshot.event_id,
        )
        retried = client.table("notifications") \
            .update(update_payload) \
            .eq("user_id", user_id) \
            .eq("type", NotificationType.CALENDAR_INVITE) \
            .eq("group_key", group_key) \
            .eq("archived", False) \
            .execute()
        return retried.data[0] if retried.data else {}


def _resolve_calendar_invite_notifications(
    client,
    *,
    user_id: str,
    calendar_event_ids: Sequence[str],
) -> Sequence[Dict[str, Any]]:
    group_keys = [_group_key(event_id) for event_id in dict.fromkeys(calendar_event_ids) if event_id]
    if not group_keys:
        return []

    query = client.table("notifications") \
        .update({
            "read": True,
            "seen": True,
            "archived": True,
        }) \
        .eq("user_id", user_id) \
        .eq("type", NotificationType.CALENDAR_INVITE) \
        .eq("archived", False)

    if len(group_keys) == 1:
        query = query.eq("group_key", group_keys[0])
    else:
        query = query.in_("group_key", group_keys)

    result = query.execute()
    return result.data or []


def _pick_representative_snapshot(snapshots: List[CalendarInviteSnapshot]) -> CalendarInviteSnapshot:
    """Pick the next upcoming actionable instance as the series representative.

    For recurring events with multiple actionable instances, prefer the
    nearest future occurrence so the notification shows the most relevant date.
    """
    now_iso = datetime.now(timezone.utc).isoformat()

    upcoming = [s for s in snapshots if s.starts_at >= now_iso]
    if upcoming:
        upcoming.sort(key=lambda s: s.starts_at)
        return upcoming[0]

    # All instances are in the past — pick the most recent one.
    snapshots_sorted = sorted(snapshots, key=lambda s: s.starts_at, reverse=True)
    return snapshots_sorted[0]


def reconcile_calendar_invite_notifications(
    *,
    user_id: str,
    account_email: Optional[str],
    previous_rows_by_external_id: Mapping[str, Mapping[str, Any]],
    current_rows_by_external_id: Mapping[str, Mapping[str, Any]],
    client=None,
) -> Dict[str, int]:
    """Upsert or resolve calendar invite notifications after a sync pass.

    For recurring events, all instances of the same series are grouped under
    one notification using the series ``recurring_event_id``.  A single
    representative instance (the next upcoming one) is chosen so the
    notification doesn't flip-flop between instances on every sync.
    """
    service_client = client or get_service_role_client()

    # --- Step 1: Build snapshots, grouped by notification_key_id ----------
    actionable_by_key: Dict[str, List[CalendarInviteSnapshot]] = {}
    non_actionable_key_ids: List[str] = []

    for row in current_rows_by_external_id.values():
        if not isinstance(row, Mapping) or not row.get("id"):
            continue
        snapshot = build_calendar_invite_snapshot(row, account_email)
        if snapshot:
            actionable_by_key.setdefault(snapshot.notification_key_id, []).append(snapshot)
        else:
            # Non-actionable — track for potential resolution
            recurring_id = row.get("recurring_event_id")
            if isinstance(recurring_id, str) and recurring_id.strip():
                non_actionable_key_ids.append(recurring_id)
            else:
                non_actionable_key_ids.append(str(row["id"]))

    # Track previous rows that disappeared from current (deleted events)
    for external_id, row in previous_rows_by_external_id.items():
        if external_id in current_rows_by_external_id:
            continue
        if not isinstance(row, Mapping) or not row.get("id"):
            continue
        prev_snapshot = build_calendar_invite_snapshot(row, account_email)
        key = prev_snapshot.notification_key_id if prev_snapshot else str(row["id"])
        if key not in actionable_by_key:
            non_actionable_key_ids.append(key)

    # --- Step 2: Pick one representative per series -----------------------
    representatives: Dict[str, CalendarInviteSnapshot] = {}
    for key, group_snapshots in actionable_by_key.items():
        representatives[key] = _pick_representative_snapshot(group_snapshots)

    # --- Step 3: Load existing notification state -------------------------
    all_key_ids = list(set(list(representatives.keys()) + non_actionable_key_ids))
    notification_presence = _notification_state_by_group_key(
        service_client,
        user_id=user_id,
        calendar_event_ids=all_key_ids,
    )

    upserted = 0
    resolved = 0

    # --- Step 4: Upsert for actionable series -----------------------------
    for key, snapshot in representatives.items():
        presence = notification_presence.get(_group_key(key), {"any": False, "active": None})
        active_notification = presence["active"]
        should_upsert = (
            active_notification is None
            or not _notification_matches_snapshot(active_notification, snapshot)
        )
        if not should_upsert:
            continue
        if _upsert_calendar_invite_notification(service_client, user_id=user_id, snapshot=snapshot) is not None:
            upserted += 1

    # --- Step 5: Resolve non-actionable series ----------------------------
    keys_to_resolve = [
        key
        for key in dict.fromkeys(non_actionable_key_ids)
        if key not in representatives
    ]
    if keys_to_resolve:
        resolved_rows = _resolve_calendar_invite_notifications(
            service_client,
            user_id=user_id,
            calendar_event_ids=keys_to_resolve,
        )
        resolved = len({
            row.get("group_key")
            for row in resolved_rows
            if isinstance(row, Mapping) and isinstance(row.get("group_key"), str)
        })

    return {"upserted": upserted, "resolved": resolved}
