import pytest
import sys

# Import the functions first to ensure modules are loaded
from api.services.calendar.create_event import _create_google_event
from api.services.calendar.update_event import _update_google_event

# Access the actual modules from sys.modules (not shadowed by __init__.py)
create_event_module = sys.modules['api.services.calendar.create_event']
update_event_module = sys.modules['api.services.calendar.update_event']


class DummyExec:
    def __init__(self, ret):
        self._ret = ret

    def execute(self):
        return self._ret


class DummyEvents:
    def __init__(self, recorder):
        self.recorder = recorder

    def insert(self, calendarId, body, sendUpdates, **kwargs):
        self.recorder['insert'] = {
            'calendarId': calendarId,
            'sendUpdates': sendUpdates,
            'body': body,
            **kwargs,
        }
        return DummyExec({'id': 'ev123'})

    def update(self, calendarId, eventId, body, sendUpdates):
        self.recorder['update'] = {
            'calendarId': calendarId,
            'eventId': eventId,
            'sendUpdates': sendUpdates,
            'body': body,
        }
        return DummyExec({})

    def get(self, calendarId, eventId):
        # Return a minimal existing event structure
        return DummyExec({'id': eventId})


class DummyService:
    def __init__(self, recorder):
        self._events = DummyEvents(recorder)

    def events(self):
        return self._events


def test_create_event_send_updates_all_when_notify_true(monkeypatch):
    recorder = {}

    # Patch functions used inside create_event module
    monkeypatch.setattr(
        create_event_module,
        'get_google_calendar_service',
        lambda user_id, user_jwt, account_id: (DummyService(recorder), 'conn-1')
    )
    monkeypatch.setattr(
        create_event_module,
        'get_user_timezone',
        lambda service: 'UTC'
    )

    event_data = {
        'title': 'Meeting',
        'start_time': '2026-02-10T10:00:00Z',
        'end_time': '2026-02-10T11:00:00Z',
        'attendees': ['alice@example.com'],
        'notify_attendees': True,
    }

    ev_id, meeting_link, err, _tz = _create_google_event('user-1', 'jwt', event_data, None)
    assert err is None
    assert recorder['insert']['sendUpdates'] == 'all'
    # Ensure attendees are passed through
    assert 'attendees' in recorder['insert']['body']


def test_update_event_send_updates_all_when_notify_true(monkeypatch):
    recorder = {}

    # Patch functions used inside update_event module
    monkeypatch.setattr(
        update_event_module,
        'get_google_calendar_service',
        lambda user_id, user_jwt, account_id: (DummyService(recorder), 'conn-1')
    )
    monkeypatch.setattr(
        update_event_module,
        'get_user_timezone',
        lambda service: 'UTC'
    )

    event_data = {
        'title': 'Updated',
        'start_time': '2026-02-10T10:00:00Z',
        'end_time': '2026-02-10T11:00:00Z',
        'notify_attendees': True,
    }

    ok, _tz = _update_google_event(
        user_id='user-1',
        user_jwt='jwt',
        external_id='ext-1',
        event_data=event_data,
        connection_id=None,
        scope='instance',
        recurring_event_id=None,
        cutoff_start=None,
    )
    assert ok is True
    assert recorder['update']['sendUpdates'] == 'all'

