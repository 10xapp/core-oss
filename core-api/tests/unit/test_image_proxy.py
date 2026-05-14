"""Tests for HMAC-signed image proxy URL generation."""

import hmac
from hashlib import sha256
from unittest.mock import patch
from urllib.parse import urlparse, parse_qs

from lib.image_proxy import (
    SIGNATURE_WINDOW_SECONDS,
    URL_VALIDITY_SECONDS,
    generate_image_url,
    generate_file_url,
    get_signed_url_expiration,
    is_image_type,
)


SECRET = "test-secret-key-abc123"
BASE_URL = "https://img.getcore.dev"
FIXED_TIME = 1738800000.0  # A fixed timestamp, mid-hour


def _make_url(r2_key: str = "files/u/test.jpg", variant: str = "thumb") -> str:
    """Helper: generate URL with fixed time to avoid hour-boundary flakes."""
    with patch("lib.image_proxy.time.time", return_value=FIXED_TIME):
        return generate_image_url(r2_key, variant, BASE_URL, SECRET)


def _parse_params(url: str) -> dict:
    """Extract query params from a URL as a flat dict."""
    qs = parse_qs(urlparse(url).query)
    return {k: v[0] for k, v in qs.items()}


class TestIsImageType:
    def test_supported_types(self):
        for mime in ["image/jpeg", "image/png", "image/gif", "image/webp", "image/heic", "image/heif"]:
            assert is_image_type(mime) is True

    def test_non_image_types(self):
        for mime in ["application/pdf", "video/mp4", "text/plain", "application/octet-stream"]:
            assert is_image_type(mime) is False

    def test_none_and_empty(self):
        assert is_image_type(None) is False
        assert is_image_type("") is False


class TestGenerateImageUrl:
    def test_returns_empty_when_not_configured(self):
        with patch("lib.image_proxy.settings") as mock:
            mock.image_proxy_url = ""
            mock.image_proxy_secret = ""
            assert generate_image_url("files/test.jpg") == ""

    def test_url_structure(self):
        url = _make_url()
        assert url.startswith(f"{BASE_URL}/files/u/test.jpg?")
        params = _parse_params(url)
        assert params["w"] == "384"
        assert params["q"] == "75"
        assert params["f"] == "webp"
        assert "exp" in params
        assert "sig" in params

    def test_deterministic(self):
        """Same inputs at same time = same URL (CDN cache hit)."""
        assert _make_url() == _make_url()

    def test_different_keys_different_urls(self):
        assert _make_url("files/a.jpg") != _make_url("files/b.jpg")

    def test_different_variants_different_urls(self):
        thumb = _make_url(variant="thumb")
        preview = _make_url(variant="preview")
        assert thumb != preview
        assert _parse_params(thumb)["w"] == "384"
        assert _parse_params(preview)["w"] == "1200"

    def test_full_variant(self):
        url = _make_url(variant="full")
        params = _parse_params(url)
        assert params["w"] == "0"
        assert params["q"] == "100"
        assert params["f"] == "auto"

    def test_signature_roundtrip(self):
        """Verify the sig matches a manual HMAC recomputation."""
        r2_key = "files/user123/20260204/photo.jpg"
        url = _make_url(r2_key)
        params = _parse_params(url)

        message = f"{r2_key}:384:75:webp:{params['exp']}"
        expected = hmac.new(SECRET.encode(), message.encode(), sha256).hexdigest()[:32]
        assert params["sig"] == expected

    def test_expiration_is_window_rounded_with_expected_validity(self):
        url = _make_url()
        exp = int(_parse_params(url)["exp"])
        window_start = int(FIXED_TIME) // SIGNATURE_WINDOW_SECONDS * SIGNATURE_WINDOW_SECONDS
        assert exp == window_start + URL_VALIDITY_SECONDS

    def test_expiration_helper_matches_url_param(self):
        with patch("lib.image_proxy.time.time", return_value=FIXED_TIME):
            expected_exp = get_signed_url_expiration()
        exp = int(_parse_params(_make_url())["exp"])
        assert exp == expected_exp

    def test_different_secret_different_signature(self):
        with patch("lib.image_proxy.time.time", return_value=FIXED_TIME):
            url1 = generate_image_url("files/a.jpg", "thumb", BASE_URL, "secret-1")
            url2 = generate_image_url("files/a.jpg", "thumb", BASE_URL, "secret-2")
        assert _parse_params(url1)["sig"] != _parse_params(url2)["sig"]

    def test_special_chars_in_key_are_escaped(self):
        url = _make_url("files/u/my photo (1).jpg")
        parsed = urlparse(url)
        assert "my%20photo%20%281%29.jpg" in parsed.path
        # But the HMAC is computed on the raw key, not the encoded one
        params = _parse_params(url)
        message = f"files/u/my photo (1).jpg:384:75:webp:{params['exp']}"
        expected = hmac.new(SECRET.encode(), message.encode(), sha256).hexdigest()[:32]
        assert params["sig"] == expected


class TestGenerateFileUrl:
    def test_image_uses_requested_variant(self):
        with patch("lib.image_proxy.settings") as mock:
            mock.image_proxy_url = BASE_URL
            mock.image_proxy_secret = SECRET
            with patch("lib.image_proxy.time.time", return_value=FIXED_TIME):
                url = generate_file_url("files/a.jpg", "image/jpeg", "thumb")
            assert _parse_params(url)["w"] == "384"

    def test_non_image_uses_full_variant(self):
        with patch("lib.image_proxy.settings") as mock:
            mock.image_proxy_url = BASE_URL
            mock.image_proxy_secret = SECRET
            with patch("lib.image_proxy.time.time", return_value=FIXED_TIME):
                url = generate_file_url("files/a.pdf", "application/pdf", "thumb")
            assert _parse_params(url)["w"] == "0"

    def test_empty_r2_key_returns_empty(self):
        assert generate_file_url("", "image/jpeg") == ""
        assert generate_file_url("", None) == ""

    def test_returns_empty_when_not_configured(self):
        with patch("lib.image_proxy.settings") as mock:
            mock.image_proxy_url = ""
            mock.image_proxy_secret = ""
            assert generate_file_url("files/a.jpg", "image/jpeg") == ""
