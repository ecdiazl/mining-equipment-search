"""Tests para el validador de URLs anti-SSRF."""

import pytest
from src.utils.url_validator import is_safe_url, sanitize_url


class TestSSRFProtection:
    """Verifica que URLs peligrosas sean rechazadas."""

    def test_normal_https_url(self):
        assert is_safe_url("https://www.komatsu.com/specs", resolve_dns=False) is True

    def test_normal_http_url(self):
        assert is_safe_url("http://mining-technology.com/data", resolve_dns=False) is True

    def test_cloud_metadata_ipv4(self):
        assert is_safe_url("http://169.254.169.254/latest/meta-data/", resolve_dns=False) is False

    def test_cloud_metadata_hostname(self):
        assert is_safe_url("http://metadata.google.internal/computeMetadata/v1/", resolve_dns=False) is False

    def test_localhost(self):
        assert is_safe_url("http://127.0.0.1:8080/admin", resolve_dns=False) is False

    def test_localhost_name(self):
        assert is_safe_url("http://localhost/admin", resolve_dns=False) is False

    def test_private_ip_10(self):
        assert is_safe_url("http://10.0.0.1/internal", resolve_dns=False) is False

    def test_private_ip_172(self):
        assert is_safe_url("http://172.16.0.1/internal", resolve_dns=False) is False

    def test_private_ip_192(self):
        assert is_safe_url("http://192.168.1.1/admin", resolve_dns=False) is False

    def test_file_scheme(self):
        assert is_safe_url("file:///etc/passwd") is False

    def test_ftp_scheme(self):
        assert is_safe_url("ftp://example.com/file") is False

    def test_javascript_scheme(self):
        assert is_safe_url("javascript:alert(1)") is False

    def test_empty_url(self):
        assert is_safe_url("") is False

    def test_very_long_url(self):
        assert is_safe_url("https://example.com/" + "a" * 3000) is False

    def test_ipv6_loopback(self):
        assert is_safe_url("http://[::1]/admin", resolve_dns=False) is False


class TestSanitizeUrl:
    def test_add_scheme(self):
        assert sanitize_url("www.example.com/path") == "https://www.example.com/path"

    def test_strip_whitespace(self):
        assert sanitize_url("  https://example.com  ") == "https://example.com"

    def test_reject_unsafe(self):
        assert sanitize_url("http://169.254.169.254/meta") == ""

    def test_empty(self):
        assert sanitize_url("") == ""
