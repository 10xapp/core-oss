"""
Notification service — event-driven, service-agnostic notifications.

Any backend service can fire notifications through create_notification().
The system handles fan-out, deduplication, preferences, and delivery.
"""
from .create import (
    create_notification,
    notify_subscribers,
    NotificationType,
    TYPE_TO_CATEGORY,
)
from .grouped import (
    GroupedNotificationResolution,
    GROUPED_NOTIFICATION_RESOLUTION_BY_TYPE,
    get_grouped_resolution,
    upsert_grouped_notification,
    resolve_grouped_notification,
)
from .file_edits import (
    get_file_edit_group_key,
    emit_document_edited_notification,
)
from .fetch import (
    get_notifications,
    get_unread_count,
)
from .update import (
    mark_as_read,
    mark_all_as_read,
    archive_notification,
)
from .subscriptions import (
    subscribe,
    ensure_subscription,
    unsubscribe,
    get_subscribers,
    is_subscribed,
)
from .preferences import (
    should_notify,
    get_preferences,
    update_preference,
)
from .helpers import get_actor_info

__all__ = [
    # Create
    'create_notification',
    'notify_subscribers',
    'NotificationType',
    'TYPE_TO_CATEGORY',
    'GroupedNotificationResolution',
    'GROUPED_NOTIFICATION_RESOLUTION_BY_TYPE',
    'get_grouped_resolution',
    'upsert_grouped_notification',
    'resolve_grouped_notification',
    'get_file_edit_group_key',
    'emit_document_edited_notification',

    # Fetch
    'get_notifications',
    'get_unread_count',

    # Update
    'mark_as_read',
    'mark_all_as_read',
    'archive_notification',

    # Subscriptions
    'subscribe',
    'ensure_subscription',
    'unsubscribe',
    'get_subscribers',
    'is_subscribed',

    # Preferences
    'should_notify',
    'get_preferences',
    'update_preference',

    # Helpers
    'get_actor_info',
]
