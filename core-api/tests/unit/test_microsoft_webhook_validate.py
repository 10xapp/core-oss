"""Tests for MicrosoftWebhookProvider.validate_notification."""
import pytest

from api.services.microsoft.microsoft_webhook_provider import MicrosoftWebhookProvider


@pytest.fixture
def provider():
    return MicrosoftWebhookProvider()


def test_accepts_matching_client_state(provider):
    notification = {"clientState": "abc123"}
    subscription = {"client_state": "abc123"}
    assert provider.validate_notification(notification, subscription) is True


def test_rejects_mismatched_client_state(provider):
    notification = {"clientState": "abc123"}
    subscription = {"client_state": "xyz789"}
    assert provider.validate_notification(notification, subscription) is False


def test_rejects_missing_client_state_on_notification(provider):
    notification = {}
    subscription = {"client_state": "abc123"}
    assert provider.validate_notification(notification, subscription) is False


def test_rejects_missing_client_state_on_subscription(provider):
    notification = {"clientState": "abc123"}
    subscription = {}
    assert provider.validate_notification(notification, subscription) is False


def test_constant_time_compare_handles_unequal_lengths(provider):
    # hmac.compare_digest only fails on equal-length inputs in some libraries.
    # Make sure unequal-length values are rejected without raising.
    notification = {"clientState": "short"}
    subscription = {"client_state": "much-longer-secret-value"}
    assert provider.validate_notification(notification, subscription) is False
