"""
Confidence scoring basado en la fuente de la informacion.
Asigna niveles de confianza segun el dominio/tipo de sitio web.
"""

import logging
import re
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


# Niveles de confianza por tipo de fuente (base score)
# Se multiplica con el confidence del extractor para dar el score final
TRUST_LEVELS = {
    "manufacturer": 1.0,       # Sitio oficial del fabricante
    "spec_database": 0.9,      # Base de datos de specs (lectura, ritchie, etc.)
    "industry_publication": 0.85,  # Publicaciones de industria minera
    "dealer": 0.75,            # Distribuidores / dealers
    "pdf_brochure": 0.95,      # PDFs de brochures (casi siempre oficiales)
    "generic": 0.6,            # Fuente generica / desconocida
}

# Dominios conocidos de fabricantes de equipos mineros
_MANUFACTURER_DOMAINS = {
    # Tier 1
    "komatsu.com", "cat.com", "caterpillar.com", "liebherr.com",
    "hitachicm.com", "hitachi-c-m.com",
    # Tier 2
    "volvoce.com", "volvo.com", "deere.com", "johndeere.com",
    "komatsu-mining.com", "mining.komatsu",
    # Chinese
    "xcmg.com", "sanygroup.com", "sany.com.cn", "zoomlion.com",
    "liugong.com", "sdlg.com", "shantui.com",
    # Russian
    "belaz.by", "belaz.com",
    # Other
    "epiroc.com", "sandvik.com", "metso.com",
    "terex.com", "doosan.com", "hyundai-ce.com",
}

# Bases de datos de especificaciones tecnicas
_SPEC_DATABASE_DOMAINS = {
    "lectura.specs", "lectura-specs.com", "ritchiespecs.com",
    "specguideonline.com", "machinemarket.co.za",
    "equipmentwatch.com", "ironplanet.com",
    "mascus.com", "machinerytrader.com",
    "heavyequipments.net", "heavyequipmentguide.ca",
}

# Publicaciones de industria minera
_INDUSTRY_DOMAINS = {
    "mining.com", "miningmagazine.com", "mining-technology.com",
    "e-mj.com", "miningglobal.com", "australianmining.com.au",
    "im-mining.com", "miningweekly.com",
    "international-mining.com", "mining-journal.com",
}

# Patrones para dealers (contienen "dealer", "parts", etc.)
_DEALER_PATTERNS = [
    re.compile(r"dealer|parts|rental|used|second.?hand|pre.?owned", re.IGNORECASE),
]


def classify_source(url: str) -> str:
    """Clasifica una URL en un tipo de fuente."""
    if not url:
        return "generic"

    try:
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        # Quitar www.
        if domain.startswith("www."):
            domain = domain[4:]
        path = parsed.path.lower()

        # PDF brochure
        if path.endswith(".pdf"):
            # Si es de fabricante, es aun mejor
            if _matches_domain_set(domain, _MANUFACTURER_DOMAINS):
                return "manufacturer"
            return "pdf_brochure"

        # Fabricante oficial
        if _matches_domain_set(domain, _MANUFACTURER_DOMAINS):
            return "manufacturer"

        # Base de datos de specs
        if _matches_domain_set(domain, _SPEC_DATABASE_DOMAINS):
            return "spec_database"

        # Publicacion de industria
        if _matches_domain_set(domain, _INDUSTRY_DOMAINS):
            return "industry_publication"

        # Dealer / reseller
        for pattern in _DEALER_PATTERNS:
            if pattern.search(domain) or pattern.search(path):
                return "dealer"

    except Exception:
        logger.debug(f"Error clasificando URL: {url}", exc_info=True)

    return "generic"


def _matches_domain_set(domain: str, domain_set: set[str]) -> bool:
    """Verifica si un dominio coincide con alguno del set (incluyendo subdominios).
    Uses O(1) set lookups by walking up the domain hierarchy."""
    if domain in domain_set:
        return True
    # Walk up the domain hierarchy: sub.example.com → example.com → com
    parts = domain.split(".")
    for i in range(1, len(parts)):
        parent = ".".join(parts[i:])
        if parent in domain_set:
            return True
    return False


def compute_source_confidence(
    extraction_confidence: float,
    source_url: str,
    is_table_source: bool = False,
) -> float:
    """Calcula la confianza final combinando extraccion y fuente.

    Args:
        extraction_confidence: Confianza base del extractor (0-1).
        source_url: URL de donde se extrajo el dato.
        is_table_source: Si el dato viene de una tabla (mas confiable que texto libre).

    Returns:
        Confianza final ajustada (0-1).
    """
    source_type = classify_source(source_url)
    trust = TRUST_LEVELS.get(source_type, TRUST_LEVELS["generic"])

    # Bonus si viene de tabla estructurada
    table_bonus = 0.05 if is_table_source else 0.0

    # Score final: promedio ponderado de extraccion y trust de fuente
    # 60% extraccion, 40% fuente + bonus tabla
    final = (extraction_confidence * 0.6) + (trust * 0.4) + table_bonus

    # Clamp a [0, 1]
    return min(max(round(final, 3), 0.0), 1.0)
