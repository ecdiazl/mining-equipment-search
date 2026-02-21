"""
Extractor dedicado de curvas rimpull (fuerza de traccion vs marcha/velocidad)
desde tablas y texto semi-estructurado en PDFs y paginas web.
"""

import re
import logging
from dataclasses import dataclass, field

from src.parsers.confidence_scorer import compute_source_confidence

logger = logging.getLogger(__name__)


@dataclass
class RimpullPoint:
    """Un punto en la curva rimpull: marcha, velocidad, fuerza."""
    gear: str
    speed_kmh: float | None
    force_kn: float
    original_unit: str
    confidence: float
    source_url: str


@dataclass
class RimpullCurve:
    """Curva rimpull completa para un equipo."""
    brand: str
    model: str
    points: list[RimpullPoint] = field(default_factory=list)


# --- Normalizacion de marchas ---

GEAR_ALIASES: dict[str, str] = {
    # 1st gear
    "1st": "1st", "1st gear": "1st", "gear 1": "1st", "1": "1st",
    "1ll": "1st", "low": "1st", "first": "1st", "first gear": "1st",
    "1st.": "1st",
    # 2nd gear
    "2nd": "2nd", "2nd gear": "2nd", "gear 2": "2nd", "2": "2nd",
    "second": "2nd", "second gear": "2nd", "2nd.": "2nd",
    # 3rd gear
    "3rd": "3rd", "3rd gear": "3rd", "gear 3": "3rd", "3": "3rd",
    "third": "3rd", "third gear": "3rd", "3rd.": "3rd",
    # 4th gear
    "4th": "4th", "4th gear": "4th", "gear 4": "4th", "4": "4th",
    "fourth": "4th", "fourth gear": "4th", "4th.": "4th",
    # 5th gear
    "5th": "5th", "5th gear": "5th", "gear 5": "5th", "5": "5th",
    "fifth": "5th", "fifth gear": "5th", "5th.": "5th",
    # 6th gear
    "6th": "6th", "6th gear": "6th", "gear 6": "6th", "6": "6th",
    "sixth": "6th", "sixth gear": "6th", "6th.": "6th",
    # 7th gear
    "7th": "7th", "7th gear": "7th", "gear 7": "7th", "7": "7th",
    "seventh": "7th", "seventh gear": "7th", "7th.": "7th",
    # Direct drive
    "d": "Direct", "direct": "Direct", "direct drive": "Direct",
    # Reverse
    "r": "Reverse", "rev": "Reverse", "reverse": "Reverse",
    "reverse gear": "Reverse",
}

GEAR_ORDER: dict[str, int] = {
    "1st": 1, "2nd": 2, "3rd": 3, "4th": 4, "5th": 5,
    "6th": 6, "7th": 7, "Direct": 8, "Reverse": 9,
}

MAX_TEXT_LENGTH = 200_000  # ~200 KB guard for extract_from_text

# Pre-compiled regex patterns for text extraction
_RIMPULL_PATTERN1 = re.compile(
    r"(?:(\w+)\s*(?:gear|marcha)\s*rimpull|rimpull\s*\(?(\w+)\s*(?:gear|marcha)\)?)"
    r"[:\s]*([0-9,.]+)\s*(kN|lbf|kgf|lb)",
    re.IGNORECASE,
)
_RIMPULL_PATTERN2 = re.compile(
    r"rimpull\s*\(\s*(\w+)(?:\s*gear)?\s*\)[:\s]*([0-9,.]+)\s*(kN|lbf|kgf|lb)",
    re.IGNORECASE,
)
_RIMPULL_SECTION_PATTERN = re.compile(
    r"rimpull[^.]{0,500}?(?:(?:\d+(?:st|nd|rd|th))[:\s]+[0-9,.]+\s*(?:kN|lbf|kgf|lb)[\s,;]*)+",
    re.IGNORECASE,
)
_RIMPULL_INLINE_PATTERN = re.compile(
    r"(\d+(?:st|nd|rd|th))[:\s]+([0-9,.]+)\s*(kN|lbf|kgf|lb)",
    re.IGNORECASE,
)


def normalize_gear(raw: str) -> str:
    """Normaliza un label de marcha a formato estandar."""
    cleaned = raw.strip().lower().rstrip(".")
    return GEAR_ALIASES.get(cleaned, raw.strip())


def sort_points_by_gear(points: list[RimpullPoint]) -> list[RimpullPoint]:
    """Ordena puntos por orden logico de marcha."""
    return sorted(points, key=lambda p: GEAR_ORDER.get(p.gear, 99))


# --- Conversion de unidades ---

def _convert_to_kn(value: float, unit: str) -> float:
    """Convierte fuerza a kN desde diversas unidades."""
    unit_lower = unit.lower().strip()
    if unit_lower == "kn":
        return value
    if unit_lower in ("lbf", "lb"):
        return value / 224.809  # 1 kN = 224.809 lbf
    if unit_lower == "kgf":
        return value / 101.972  # 1 kN = 101.972 kgf
    return value


def _detect_force_unit(text: str) -> str:
    """Detecta unidad de fuerza en un texto (header, label)."""
    text_lower = text.lower()
    if "lbf" in text_lower or "lb" in text_lower:
        return "lbf"
    if "kgf" in text_lower:
        return "kgf"
    if "kn" in text_lower:
        return "kN"
    return "kN"  # default


# --- Deteccion de tabla rimpull ---

_GEAR_KEYWORDS = {"gear", "speed", "marcha", "velocidad"}
_FORCE_KEYWORDS = {"rimpull", "tractive", "drawbar", "force", "pull", "traccion", "tracción"}


def is_rimpull_table(table: list[list[str]]) -> bool:
    """Detecta si una tabla contiene datos de curva rimpull."""
    if not table or len(table) < 2:
        return False

    # Check headers (first 2 rows)
    header_text = " ".join(
        cell.lower() for row in table[:2] for cell in row if cell
    )

    has_gear = any(kw in header_text for kw in _GEAR_KEYWORDS)
    has_force = any(kw in header_text for kw in _FORCE_KEYWORDS)

    if has_gear and has_force:
        return True

    # Check if the table has gear-like values in first column and numeric values
    gear_count = 0
    for row in table[1:]:
        if row and normalize_gear(row[0]) in GEAR_ORDER:
            gear_count += 1
    if gear_count >= 2:
        # Check if other columns have numeric values
        for row in table[1:]:
            for cell in row[1:]:
                try:
                    float(cell.replace(",", "").strip())
                    return True
                except (ValueError, TypeError):
                    continue

    return False


# --- Extraccion desde tabla ---

def _find_column_indices(
    header: list[str],
) -> tuple[int | None, int | None, int | None]:
    """Encuentra indices de columnas gear, speed, force en el header."""
    gear_col = None
    speed_col = None
    force_col = None

    for i, cell in enumerate(header):
        cell_lower = cell.lower().strip()
        if any(kw in cell_lower for kw in ("gear", "marcha")):
            gear_col = i
        elif any(kw in cell_lower for kw in ("speed", "velocidad", "km/h", "kmh")):
            speed_col = i
        elif any(kw in cell_lower for kw in _FORCE_KEYWORDS):
            force_col = i

    return gear_col, speed_col, force_col


class RimpullCurveExtractor:
    """Extrae curvas rimpull desde tablas y texto."""

    def extract_from_table(
        self,
        table: list[list[str]],
        brand: str,
        model: str,
        source_url: str = "",
    ) -> RimpullCurve | None:
        """Extrae curva rimpull de una tabla detectada.

        Soporta formatos:
        - 2 cols: gear | force
        - 3 cols: gear | speed | force
        - 3 cols: gear | force | speed
        """
        if not is_rimpull_table(table):
            return None

        header = table[0]
        gear_col, speed_col, force_col = _find_column_indices(header)

        # Detect force unit from header
        header_text = " ".join(header)
        force_unit = _detect_force_unit(header_text)

        # If no explicit columns found, use positional defaults
        if gear_col is None:
            gear_col = 0
        if force_col is None:
            # Force is typically the last numeric column or the one after gear
            if speed_col is not None:
                # gear | speed | force
                remaining = [i for i in range(len(header)) if i not in (gear_col, speed_col)]
                force_col = remaining[0] if remaining else len(header) - 1
            else:
                force_col = len(header) - 1

        base_confidence = 0.9
        final_confidence = compute_source_confidence(
            base_confidence, source_url, is_table_source=True
        )

        points: list[RimpullPoint] = []
        for row in table[1:]:
            if len(row) <= max(gear_col, force_col):
                continue

            gear_raw = row[gear_col].strip()
            if not gear_raw:
                continue

            gear = normalize_gear(gear_raw)

            # Parse force
            try:
                force_raw = row[force_col].replace(",", "").strip()
                force_val = float(force_raw)
            except (ValueError, TypeError, IndexError):
                continue

            # Convert to kN
            force_kn = _convert_to_kn(force_val, force_unit)

            # Parse speed if available
            speed_kmh = None
            if speed_col is not None and speed_col < len(row):
                try:
                    speed_kmh = float(row[speed_col].replace(",", "").strip())
                except (ValueError, TypeError):
                    pass

            points.append(RimpullPoint(
                gear=gear,
                speed_kmh=speed_kmh,
                force_kn=round(force_kn, 2),
                original_unit=force_unit,
                confidence=final_confidence,
                source_url=source_url,
            ))

        if len(points) < 2:
            return None

        points = sort_points_by_gear(points)
        logger.info(
            f"Extraidos {len(points)} puntos rimpull de tabla para {brand} {model}"
        )
        return RimpullCurve(brand=brand, model=model, points=points)

    def extract_from_text(
        self,
        text: str,
        brand: str,
        model: str,
        source_url: str = "",
    ) -> list[RimpullCurve]:
        """Extrae curvas rimpull desde texto semi-estructurado.

        Patrones soportados:
        - "1st gear rimpull: 950 kN"
        - "Rimpull (1st gear): 950 kN"
        - "1st: 950 kN, 2nd: 750 kN, 3rd: 550 kN"
        """
        if not text:
            return []

        # Guard against extremely large texts
        if len(text) > MAX_TEXT_LENGTH:
            logger.warning(
                f"Texto rimpull truncado de {len(text)} a {MAX_TEXT_LENGTH} chars para {brand} {model}"
            )
            text = text[:MAX_TEXT_LENGTH]

        base_confidence = 0.8
        final_confidence = compute_source_confidence(
            base_confidence, source_url, is_table_source=False
        )

        points: list[RimpullPoint] = []

        # Pattern 1: "1st gear rimpull: 950 kN" / "Rimpull 1st gear: 950 kN"
        for m in _RIMPULL_PATTERN1.finditer(text):
            gear_raw = m.group(1) or m.group(2)
            gear = normalize_gear(gear_raw)
            try:
                force_val = float(m.group(3).replace(",", ""))
            except (ValueError, TypeError):
                continue
            unit = m.group(4)
            force_kn = _convert_to_kn(force_val, unit)
            points.append(RimpullPoint(
                gear=gear, speed_kmh=None, force_kn=round(force_kn, 2),
                original_unit=unit, confidence=final_confidence,
                source_url=source_url,
            ))

        # Pattern 2: "Rimpull (1st): 950 kN" / "Rimpull (1st gear): 950 kN"
        for m in _RIMPULL_PATTERN2.finditer(text):
            gear = normalize_gear(m.group(1))
            try:
                force_val = float(m.group(2).replace(",", ""))
            except (ValueError, TypeError):
                continue
            unit = m.group(3)
            force_kn = _convert_to_kn(force_val, unit)
            # Avoid duplicates from pattern1
            if not any(p.gear == gear and abs(p.force_kn - force_kn) < 0.1 for p in points):
                points.append(RimpullPoint(
                    gear=gear, speed_kmh=None, force_kn=round(force_kn, 2),
                    original_unit=unit, confidence=final_confidence,
                    source_url=source_url,
                ))

        # Pattern 3: "1st: 950 kN, 2nd: 750 kN, 3rd: 550 kN" (inline list near rimpull context)
        for section_match in _RIMPULL_SECTION_PATTERN.finditer(text):
            section = section_match.group(0)
            for m in _RIMPULL_INLINE_PATTERN.finditer(section):
                gear = normalize_gear(m.group(1))
                try:
                    force_val = float(m.group(2).replace(",", ""))
                except (ValueError, TypeError):
                    continue
                unit = m.group(3)
                force_kn = _convert_to_kn(force_val, unit)
                if not any(p.gear == gear and abs(p.force_kn - force_kn) < 0.1 for p in points):
                    points.append(RimpullPoint(
                        gear=gear, speed_kmh=None, force_kn=round(force_kn, 2),
                        original_unit=unit, confidence=final_confidence,
                        source_url=source_url,
                    ))

        if len(points) < 2:
            return []

        points = sort_points_by_gear(points)
        logger.info(
            f"Extraidos {len(points)} puntos rimpull de texto para {brand} {model}"
        )
        return [RimpullCurve(brand=brand, model=model, points=points)]
