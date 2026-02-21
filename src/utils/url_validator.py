"""
Validador de URLs para prevenir SSRF (Server-Side Request Forgery).
Rechaza URLs que apunten a redes privadas, cloud metadata, o esquemas no-HTTP.
"""

import ipaddress
import logging
import socket
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

# Rangos de IP privados / reservados que nunca deberian ser scrapeados
_BLOCKED_NETWORKS = [
    ipaddress.ip_network("10.0.0.0/8"),
    ipaddress.ip_network("172.16.0.0/12"),
    ipaddress.ip_network("192.168.0.0/16"),
    ipaddress.ip_network("127.0.0.0/8"),
    ipaddress.ip_network("169.254.0.0/16"),   # Link-local / cloud metadata
    ipaddress.ip_network("0.0.0.0/8"),
    ipaddress.ip_network("100.64.0.0/10"),     # Carrier-grade NAT
    ipaddress.ip_network("198.18.0.0/15"),     # Benchmark testing
    ipaddress.ip_network("::1/128"),           # IPv6 loopback
    ipaddress.ip_network("fc00::/7"),          # IPv6 unique local
    ipaddress.ip_network("fe80::/10"),         # IPv6 link-local
]

# Hostnames conocidos de cloud metadata y loopback
_BLOCKED_HOSTNAMES = {
    "metadata.google.internal",
    "metadata.google.com",
    "metadata.azure.com",
    "169.254.169.254",
    "metadata",
    "localhost",
}

ALLOWED_SCHEMES = {"http", "https"}

MAX_URL_LENGTH = 2048


def is_safe_url(url: str, resolve_dns: bool = True) -> bool:
    """
    Valida que una URL sea segura para fetchear.
    Rechaza: IPs privadas, esquemas no-HTTP, cloud metadata, URLs muy largas.

    Args:
        url: URL a validar.
        resolve_dns: Si True, resuelve el hostname a IP y la valida.
    """
    if not url or len(url) > MAX_URL_LENGTH:
        return False

    try:
        parsed = urlparse(url)
    except Exception:
        return False

    # Solo HTTP/HTTPS
    if parsed.scheme.lower() not in ALLOWED_SCHEMES:
        logger.warning(f"URL rechazada (esquema no permitido): {url}")
        return False

    hostname = parsed.hostname
    if not hostname:
        return False

    # Hostnames bloqueados
    if hostname.lower() in _BLOCKED_HOSTNAMES:
        logger.warning(f"URL rechazada (cloud metadata): {url}")
        return False

    # Verificar si el hostname es directamente una IP
    try:
        ip = ipaddress.ip_address(hostname)
        if _is_private_ip(ip):
            logger.warning(f"URL rechazada (IP privada): {url}")
            return False
    except ValueError:
        # No es IP literal, es hostname — verificar via DNS si se pide
        if resolve_dns:
            if not _resolve_and_check(hostname, url):
                return False

    return True


def _is_private_ip(ip: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    """Verifica si una IP esta en rangos privados/reservados."""
    # Check IPv4-mapped IPv6 addresses (e.g. ::ffff:127.0.0.1)
    if isinstance(ip, ipaddress.IPv6Address) and ip.ipv4_mapped:
        ip = ip.ipv4_mapped
    return any(ip in network for network in _BLOCKED_NETWORKS)


def _resolve_and_check(hostname: str, url: str) -> bool:
    """Resuelve DNS y verifica que no apunte a IPs privadas."""
    try:
        results = socket.getaddrinfo(hostname, None, socket.AF_UNSPEC, socket.SOCK_STREAM)
        for family, _, _, _, sockaddr in results:
            ip_str = sockaddr[0]
            ip = ipaddress.ip_address(ip_str)
            if _is_private_ip(ip):
                logger.warning(f"URL rechazada (DNS resuelve a IP privada {ip_str}): {url}")
                return False
    except socket.gaierror:
        # DNS fail → fail-closed: reject the URL
        logger.warning(f"URL rechazada (DNS no resuelve hostname '{hostname}'): {url}")
        return False
    return True


def sanitize_url(url: str) -> str:
    """Normaliza una URL y la valida. Retorna la URL limpia o string vacio si es insegura."""
    url = url.strip()
    if not url:
        return ""
    # Forzar HTTPS si no tiene esquema
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    if is_safe_url(url):
        return url
    return ""
