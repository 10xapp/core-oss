"""
Calendar service - Organized module structure
"""
from .fetch_events import (
    get_events,
    get_upcoming_events,
    get_all_events,
    get_today_events,
    get_event_by_id
)
from .create_event import create_event
from .update_event import update_event
from .delete_event import delete_event
from .respond_to_event import respond_to_event

# Note: sync_google_calendar is available from api.services.syncs
# (not imported here to avoid circular imports)

# Export all functions for backward compatibility
__all__ = [
    'get_events',
    'get_upcoming_events',
    'get_all_events',
    'get_today_events',
    'get_event_by_id',
    'create_event',
    'update_event',
    'delete_event',
    'respond_to_event',
]

