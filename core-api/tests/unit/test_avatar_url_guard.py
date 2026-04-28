"""Tests for the avatar URL allowlist used before downloading."""
import pytest

from api.services.auth import _is_safe_avatar_url


@pytest.mark.parametrize("url", [
    "https://lh3.googleusercontent.com/a/AAAA",
    "https://lh4.googleusercontent.com/a/BBBB",
    "https://graph.microsoft.com/v1.0/me/photo/$value",
])
def test_allows_known_provider_hosts(url):
    assert _is_safe_avatar_url(url) is True


@pytest.mark.parametrize("url", [
    "http://lh3.googleusercontent.com/a/AAAA",       # http instead of https
    "ftp://lh3.googleusercontent.com/a/AAAA",        # non-http scheme
    "https://evil.com/avatar.png",                   # not on allowlist
    "https://googleusercontent.com.evil.com/x",      # suffix-confusion
    "https://127.0.0.1/a",                           # loopback
    "https://10.0.0.1/admin",                        # rfc1918
    "https://169.254.169.254/latest/meta-data/",     # cloud metadata
    "https://[::1]/a",                               # ipv6 loopback
    "",                                              # empty
    "not a url",
])
def test_rejects_unsafe_urls(url):
    assert _is_safe_avatar_url(url) is False
