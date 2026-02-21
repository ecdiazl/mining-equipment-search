"""
Extractor de especificaciones tecnicas desde texto y tablas scrapeadas.
Utiliza patrones regex y NLP para identificar valores tecnicos.
"""

import re
import logging
from dataclasses import dataclass, field

from src.parsers.confidence_scorer import compute_source_confidence

logger = logging.getLogger(__name__)


@dataclass
class TechnicalSpec:
    """Especificacion tecnica extraida."""
    brand: str
    model: str
    parameter: str
    value: str
    unit: str
    source_url: str
    confidence: float = 0.0


@dataclass
class EquipmentProfile:
    """Perfil completo de un equipo con todas sus specs."""
    brand: str
    model: str
    equipment_type: str
    specs: list[TechnicalSpec] = field(default_factory=list)
    raw_sources: list[str] = field(default_factory=list)


# Limite de texto para evitar ReDoS en paginas muy grandes
MAX_TEXT_LENGTH = 200_000  # ~200 KB

# Patrones regex para extraer especificaciones tecnicas comunes
_SPEC_PATTERNS_RAW = {
    "peso_operativo": {
        "patterns": [
            r"(?:operating|gross|service)\s*weight[:\s]*(?:of\s+)?([0-9,.]+)\s*(kg|ton|tonnes|t|lb)",
            r"peso\s*operativo[:\s]*([0-9,.]+)\s*(kg|ton|t)",
            r"weight[:\s]*([0-9,.]+)\s*(kg|metric\s*ton|ton)",
        ],
        "unit_normalize": {"kg": "kg", "ton": "ton", "tonnes": "ton", "t": "ton", "lb": "lb"},
    },
    "potencia_motor": {
        "patterns": [
            r"(?:engine|motor|gross)\s*(?:power|output|rating)[:\s]*([0-9,.]+)\s*(hp|kw|kW|HP|PS)",
            r"potencia[:\s]*([0-9,.]+)\s*(hp|kw|HP|cv)",
            r"([0-9,.]+)\s*(hp|HP|kW)\s*(?:@|at)\s*[0-9]+\s*rpm",
        ],
        "unit_normalize": {"hp": "hp", "HP": "hp", "kw": "kW", "kW": "kW", "PS": "hp", "cv": "hp"},
    },
    "capacidad_balde": {
        "patterns": [
            r"bucket\s*capacity[:\s]*([0-9,.]+)\s*(?:-\s*[0-9,.]+\s*)?(m3|m²|yd3|cu\.?\s*m|m\xb3)",
            r"capacidad\s*(?:de\s*)?balde[:\s]*([0-9,.]+)\s*(m3|m²)",
            r"(?:heaped|struck)\s*capacity[:\s]*([0-9,.]+)\s*(m3|yd3|m\xb3)",
        ],
        "unit_normalize": {"m3": "m3", "m²": "m3", "yd3": "yd3", "cu m": "m3", "m\xb3": "m3"},
    },
    "capacidad_carga": {
        "patterns": [
            r"(?:payload|load)\s*capacity[:\s]*([0-9,.]+)\s*(ton|tonnes|t|kg|metric\s*ton)",
            r"capacidad\s*(?:de\s*)?carga[:\s]*([0-9,.]+)\s*(ton|t|kg)",
            r"(?:rated|nominal)\s*payload[:\s]*([0-9,.]+)\s*(ton|tonnes|t)",
        ],
        "unit_normalize": {"ton": "ton", "tonnes": "ton", "t": "ton", "kg": "kg"},
    },
    "modelo_motor": {
        "patterns": [
            r"(?:engine|motor)\s*(?:model|type)?[:\s]*((?:Cummins|MTU|Liebherr|Komatsu|Weichai|Shangchai|YuChai|QSK|QST|SAA|WP|SC|YC)\s*[A-Z0-9\-]+)",
        ],
        "unit_normalize": {},
    },
    "torque": {
        "patterns": [
            r"(?:max|maximum|peak)?\s*torque[:\s]*([0-9,.]+)\s*(Nm|N[·.]m|kN[·.]m|lb[·\-\s]?ft)",
            r"par\s*(?:motor|maximo)?[:\s]*([0-9,.]+)\s*(Nm|N[·.]m|kN[·.]m)",
        ],
        "unit_normalize": {"Nm": "Nm", "N·m": "Nm", "N.m": "Nm", "kN·m": "kNm", "kN.m": "kNm", "lb-ft": "lb-ft", "lb ft": "lb-ft", "lb·ft": "lb-ft"},
    },
    "cilindrada": {
        "patterns": [
            r"(?:displacement|engine\s*displacement)[:\s]*([0-9,.]+)\s*(L|l|liters?|litres?|cc|cm3)",
            r"cilindrada[:\s]*([0-9,.]+)\s*(L|l|litros?|cc|cm3)",
        ],
        "unit_normalize": {"L": "L", "l": "L", "liters": "L", "liter": "L", "litres": "L", "litre": "L", "litros": "L", "litro": "L", "cc": "cc", "cm3": "cc"},
    },
    "fuerza_excavacion": {
        "patterns": [
            r"(?:digging|breakout|bucket)\s*force[:\s]*([0-9,.]+)\s*(kN|kgf|lbf|tf)",
            r"fuerza\s*(?:de\s*)?excavaci[oó]n[:\s]*([0-9,.]+)\s*(kN|kgf|tf)",
        ],
        "unit_normalize": {"kN": "kN", "kgf": "kgf", "lbf": "lbf", "tf": "tf"},
    },
    "presion_hidraulica": {
        "patterns": [
            r"(?:hydraulic|system)\s*pressure[:\s]*([0-9,.]+)\s*(bar|psi|MPa|kPa)",
            r"presi[oó]n\s*hidr[aá]ulica[:\s]*([0-9,.]+)\s*(bar|psi|MPa|kPa)",
        ],
        "unit_normalize": {"bar": "bar", "psi": "psi", "MPa": "MPa", "kPa": "kPa"},
    },
    "velocidad_maxima": {
        "patterns": [
            r"(?:max|maximum|top)\s*speed[:\s]*([0-9,.]+)\s*(km/h|mph|kmh)",
            r"velocidad\s*m[aá]xima[:\s]*([0-9,.]+)\s*(km/h|kmh)",
        ],
        "unit_normalize": {"km/h": "km/h", "kmh": "km/h", "mph": "mph"},
    },
    "consumo_combustible": {
        "patterns": [
            r"fuel\s*consumption[:\s]*([0-9,.]+)\s*(l/h|gal/h|L/h)",
            r"consumo\s*(?:de\s*)?combustible[:\s]*([0-9,.]+)\s*(l/h|L/h)",
        ],
        "unit_normalize": {"l/h": "L/h", "L/h": "L/h", "gal/h": "gal/h"},
    },
    # === NUEVOS PARAMETROS ===
    "capacidad_tanque": {
        "patterns": [
            r"fuel\s*tank\s*capacity[:\s]*([0-9,.]+)\s*(L|l|liters?|litres?|gal|gallons?)",
            r"tank\s*capacity[:\s]*([0-9,.]+)\s*(L|l|liters?|gal)",
            r"capacidad\s*(?:del?\s*)?tanque[:\s]*([0-9,.]+)\s*(L|l|litros?|gal)",
        ],
        "unit_normalize": {"L": "L", "l": "L", "liters": "L", "liter": "L", "litres": "L", "litre": "L", "litros": "L", "litro": "L", "gal": "gal", "gallons": "gal", "gallon": "gal"},
    },
    "norma_emisiones": {
        "patterns": [
            r"(?:emission|emissions)\s*(?:standard|level|tier|stage|norm)[:\s]*((?:Tier|Stage|EU Stage|EPA Tier|China|CHINA)\s*[IViv0-9]+(?:\s*[A-Za-z]*)?)",
            r"(?:Tier|Stage|EU\s*Stage|EPA\s*Tier)\s*([IViv0-9]+(?:\s*(?:Final|Interim|A|B|C))?)",
        ],
        "unit_normalize": {},
    },
    "tipo_transmision": {
        "patterns": [
            r"transmission\s*(?:type)?[:\s]*((?:electric|mechanical|hydrostatic|hydrodynamic|automatic|manual|planetary)\s*(?:drive|transmission)?(?:\s\w{1,30})?)",
            r"transmisi[oó]n[:\s]*((?:el[eé]ctrica|mec[aá]nica|hidrost[aá]tica|hidrodinámica|autom[aá]tica|planetaria)(?:\s\w{1,30})?)",
        ],
        "unit_normalize": {},
    },
    "tamano_neumaticos": {
        "patterns": [
            r"(?:tire|tyre)\s*size[:\s]*(\d{2,3}[./]\d{2,3}[\s\-]*R?\s*\d{2,3})",
            r"(?:tire|tyre)s?[:\s]*(\d{2,3}\.\d{2}[\s\-]*R?\s*\d{2,3})",
            r"neum[aá]ticos?[:\s]*(\d{2,3}[./]\d{2,3}[\s\-]*R?\s*\d{2,3})",
        ],
        "unit_normalize": {},
    },
    "profundidad_excavacion": {
        "patterns": [
            r"(?:max|maximum)?\s*(?:digging|excavation)\s*depth[:\s]*([0-9,.]+)\s*(m|mm|ft|feet)",
            r"profundidad\s*(?:de\s*)?excavaci[oó]n[:\s]*([0-9,.]+)\s*(m|mm)",
        ],
        "unit_normalize": {"m": "m", "mm": "mm", "ft": "ft", "feet": "ft"},
    },
    "alcance_max": {
        "patterns": [
            r"(?:max|maximum)?\s*(?:digging|reach)\s*(?:radius|reach|range)[:\s]*([0-9,.]+)\s*(m|mm|ft|feet)",
            r"alcance\s*m[aá]ximo[:\s]*([0-9,.]+)\s*(m|mm)",
        ],
        "unit_normalize": {"m": "m", "mm": "mm", "ft": "ft", "feet": "ft"},
    },
    "altura_descarga": {
        "patterns": [
            r"(?:dump|dumping|discharge|loading)\s*height[:\s]*([0-9,.]+)\s*(m|mm|ft|feet)",
            r"(?:dump|loading)\s*clearance[:\s]*([0-9,.]+)\s*(m|mm|ft)",
            r"altura\s*(?:de\s*)?descarga[:\s]*([0-9,.]+)\s*(m|mm)",
        ],
        "unit_normalize": {"m": "m", "mm": "mm", "ft": "ft", "feet": "ft"},
    },
    "presion_suelo": {
        "patterns": [
            r"ground\s*pressure[:\s]*([0-9,.]+)\s*(kPa|bar|psi|kg/cm2|kg/cm²)",
            r"presi[oó]n\s*(?:al|sobre\s*el|de)?\s*suelo[:\s]*([0-9,.]+)\s*(kPa|bar|psi|kg/cm2)",
        ],
        "unit_normalize": {"kPa": "kPa", "bar": "bar", "psi": "psi", "kg/cm2": "kg/cm2", "kg/cm²": "kg/cm2"},
    },
    "velocidad_giro": {
        "patterns": [
            r"(?:swing|slew)\s*speed[:\s]*([0-9,.]+)\s*(rpm|r/min|°/s|deg/s)",
            r"velocidad\s*(?:de\s*)?giro[:\s]*([0-9,.]+)\s*(rpm|r/min|°/s)",
        ],
        "unit_normalize": {"rpm": "rpm", "r/min": "rpm", "°/s": "°/s", "deg/s": "°/s"},
    },
    "pendiente_maxima": {
        "patterns": [
            r"(?:max|maximum)?\s*(?:grade|gradeability|slope)[:\s]*([0-9,.]+)\s*(%|percent|degrees?|°)",
            r"pendiente\s*m[aá]xima[:\s]*([0-9,.]+)\s*(%|grados?|°)",
        ],
        "unit_normalize": {"%": "%", "percent": "%", "degrees": "°", "degree": "°", "°": "°", "grados": "°", "grado": "°"},
    },
    "radio_giro": {
        "patterns": [
            r"(?:turning|swing)\s*radius[:\s]*([0-9,.]+)\s*(m|mm|ft|feet)",
            r"radio\s*(?:de\s*)?giro[:\s]*([0-9,.]+)\s*(m|mm)",
        ],
        "unit_normalize": {"m": "m", "mm": "mm", "ft": "ft", "feet": "ft"},
    },
    "ancho_total": {
        "patterns": [
            r"(?:overall|total|machine)\s*width[:\s]*([0-9,.]+)\s*(m|mm|ft|feet|in)",
            r"ancho\s*(?:total|general)?[:\s]*([0-9,.]+)\s*(m|mm)",
        ],
        "unit_normalize": {"m": "m", "mm": "mm", "ft": "ft", "feet": "ft", "in": "in"},
    },
    "largo_total": {
        "patterns": [
            r"(?:overall|total|machine)\s*length[:\s]*([0-9,.]+)\s*(m|mm|ft|feet|in)",
            r"largo\s*(?:total|general)?[:\s]*([0-9,.]+)\s*(m|mm)",
        ],
        "unit_normalize": {"m": "m", "mm": "mm", "ft": "ft", "feet": "ft", "in": "in"},
    },
    "altura_total": {
        "patterns": [
            r"(?:overall|total|machine)\s*height[:\s]*([0-9,.]+)\s*(m|mm|ft|feet|in)",
            r"altura\s*(?:total|general)?[:\s]*([0-9,.]+)\s*(m|mm)",
        ],
        "unit_normalize": {"m": "m", "mm": "mm", "ft": "ft", "feet": "ft", "in": "in"},
    },
    "caudal_hidraulico": {
        "patterns": [
            r"(?:hydraulic|main\s*pump)\s*flow[:\s]*([0-9,.]+)\s*(L/min|l/min|gpm|gal/min)",
            r"caudal\s*hidr[aá]ulico[:\s]*([0-9,.]+)\s*(L/min|l/min)",
        ],
        "unit_normalize": {"L/min": "L/min", "l/min": "L/min", "gpm": "gpm", "gal/min": "gpm"},
    },
    "capacidad_elevacion": {
        "patterns": [
            r"(?:lifting|lift)\s*capacity[:\s]*([0-9,.]+)\s*(ton|tonnes?|t|kg|metric\s*ton)",
            r"capacidad\s*(?:de\s*)?(?:elevaci[oó]n|izaje|levante)[:\s]*([0-9,.]+)\s*(ton|t|kg)",
        ],
        "unit_normalize": {"ton": "ton", "tonnes": "ton", "tonne": "ton", "t": "ton", "kg": "kg"},
    },
    "numero_cilindros": {
        "patterns": [
            r"(?:number\s*of\s*)?cylinders?[:\s]*(\d{1,2})\s*(?:cylinder|cyl)?",
            r"(\d{1,2})\s*(?:\-\s*)?cylinders?",
            r"n[uú]mero\s*(?:de\s*)?cilindros[:\s]*(\d{1,2})",
        ],
        "unit_normalize": {},
    },
    "tipo_rodamiento": {
        "patterns": [
            r"(?:undercarriage|track)\s*(?:type|system)?[:\s]*((?:single|double|triple)\s*(?:grouser|track)\s*(?:shoe)?(?:\s*\w+)?)",
            r"track\s*(?:shoe)?\s*(?:width|type)[:\s]*([0-9]+\s*mm)",
        ],
        "unit_normalize": {},
    },
    "voltaje_sistema": {
        "patterns": [
            r"(?:system|electrical|battery)\s*voltage[:\s]*([0-9,.]+)\s*(V|volt|volts?)",
            r"voltaje\s*(?:del?\s*)?sistema[:\s]*([0-9,.]+)\s*(V|volt)",
        ],
        "unit_normalize": {"V": "V", "volt": "V", "volts": "V"},
    },
    "peso_vacio": {
        "patterns": [
            r"(?:empty|tare|dry|shipping)\s*weight[:\s]*([0-9,.]+)\s*(kg|ton|tonnes?|t|lb)",
            r"peso\s*(?:en\s*)?vac[ií]o[:\s]*([0-9,.]+)\s*(kg|ton|t)",
        ],
        "unit_normalize": {"kg": "kg", "ton": "ton", "tonnes": "ton", "tonne": "ton", "t": "ton", "lb": "lb"},
    },
    "ancho_zapata": {
        "patterns": [
            r"(?:track|shoe)\s*width[:\s]*([0-9,.]+)\s*(mm|in|cm)",
            r"ancho\s*(?:de\s*)?(?:zapata|cadena)[:\s]*([0-9,.]+)\s*(mm|cm)",
        ],
        "unit_normalize": {"mm": "mm", "in": "in", "cm": "cm"},
    },
    "capacidad_cucharon": {
        "patterns": [
            r"(?:dipper|shovel|front\s*shovel)\s*capacity[:\s]*([0-9,.]+)\s*(m3|m²|yd3|m\xb3)",
            r"capacidad\s*(?:del?\s*)?cuchar[oó]n[:\s]*([0-9,.]+)\s*(m3|m²|yd3|m\xb3)",
        ],
        "unit_normalize": {"m3": "m3", "m²": "m3", "yd3": "yd3", "m\xb3": "m3"},
    },
    # Rimpull / Traccion (equipos de transporte)
    "rimpull_maximo": {
        "patterns": [
            r"(?:max|maximum|peak)?\s*rimpull[:\s]*([0-9,.]+)\s*(kN|kgf|lbf|lb)",
            r"(?:max|maximum)?\s*(?:tractive|drawbar)\s*(?:effort|force|pull)[:\s]*([0-9,.]+)\s*(kN|kgf|lbf|lb)",
            r"rimpull\s*(?:m[aá]ximo)?[:\s]*([0-9,.]+)\s*(kN|kgf|lbf)",
            r"(?:1st|first|low)\s*gear\s*rimpull[:\s]*([0-9,.]+)\s*(kN|kgf|lbf)",
        ],
        "unit_normalize": {"kN": "kN", "kgf": "kgf", "lbf": "lbf", "lb": "lbf"},
    },
    "velocidad_retardo": {
        "patterns": [
            r"(?:retarder|retarding)\s*(?:speed|capacity)[:\s]*([0-9,.]+)\s*(km/h|mph)",
            r"(?:dynamic\s*)?braking\s*(?:capacity|power|force)[:\s]*([0-9,.]+)\s*(kW|hp|kN)",
        ],
        "unit_normalize": {"km/h": "km/h", "mph": "mph", "kW": "kW", "hp": "hp", "kN": "kN"},
    },
    "numero_marchas": {
        "patterns": [
            r"(?:forward\s*)?(?:gears?|speeds?)[:\s]*(\d{1,2})\s*(?:forward|fwd)",
            r"(\d{1,2})\s*(?:forward|fwd)\s*/\s*\d{1,2}\s*(?:reverse|rev)",
            r"transmission[:\s]*(\d{1,2})\s*(?:speed|gear)",
        ],
        "unit_normalize": {},
    },
}

# Pre-compilar todos los patrones regex al importar el modulo
SPEC_PATTERNS: dict[str, dict] = {}
for _param_name, _config in _SPEC_PATTERNS_RAW.items():
    SPEC_PATTERNS[_param_name] = {
        "compiled": [re.compile(p, re.IGNORECASE) for p in _config["patterns"]],
        "unit_normalize": _config["unit_normalize"],
    }


# Pre-compiled regex for splitting value/unit
_SPLIT_VALUE_UNIT_RE = re.compile(r"([0-9,.]+)\s*(.*)")


def sanitize_scraped_text(text: str) -> str:
    """Remove control characters (except newline/tab) from scraped text."""
    return re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]", "", text)


class SpecExtractor:
    """Extrae especificaciones tecnicas de texto usando patrones regex."""

    def extract_from_text(
        self, text: str, brand: str, model: str, source_url: str = ""
    ) -> list[TechnicalSpec]:
        """Extrae todas las specs reconocibles del texto."""
        if not text:
            return []

        # Truncar textos muy grandes para evitar lentitud en regex
        if len(text) > MAX_TEXT_LENGTH:
            logger.warning(
                f"Texto truncado de {len(text)} a {MAX_TEXT_LENGTH} chars para {brand} {model}"
            )
            text = text[:MAX_TEXT_LENGTH]

        specs = []

        for param_name, config in SPEC_PATTERNS.items():
            for compiled_re in config["compiled"]:
                for match in compiled_re.finditer(text):
                    value = match.group(1).replace(",", "")
                    raw_unit = match.group(2) if match.lastindex >= 2 else ""
                    unit = config["unit_normalize"].get(raw_unit, raw_unit)

                    base_confidence = 0.8
                    final_confidence = compute_source_confidence(
                        base_confidence, source_url, is_table_source=False
                    )
                    specs.append(TechnicalSpec(
                        brand=brand,
                        model=model,
                        parameter=param_name,
                        value=value,
                        unit=unit,
                        source_url=source_url,
                        confidence=final_confidence,
                    ))

        logger.info(f"Extraidas {len(specs)} specs de texto para {brand} {model}")
        return specs

    def extract_from_table(
        self, table: list[list[str]], brand: str, model: str, source_url: str = ""
    ) -> list[TechnicalSpec]:
        """Extrae specs de tablas HTML/PDF.

        Soporta multiples formatos:
        - 2 columnas: parametro | valor
        - 3 columnas: parametro | valor | unidad
        - 3+ columnas con header de unidades: parametro | valor (unidad en header)
        """
        specs = []
        if not table or len(table) < 1:
            return specs

        # Detectar si la primera fila es header
        header_row = table[0] if self._is_header_row(table[0]) else None
        data_rows = table[1:] if header_row else table

        # Detectar columna de unidades en el header
        unit_col_idx = self._find_unit_column(header_row) if header_row else None

        for row in data_rows:
            if len(row) < 2:
                continue

            param_raw = row[0].strip().lower()
            if not param_raw:
                continue

            param_name = self._map_table_param(param_raw)
            if not param_name:
                continue

            value_raw = row[1].strip()
            unit = ""

            # Formato 3+ columnas: parametro | valor | unidad
            if len(row) >= 3 and unit_col_idx is not None and unit_col_idx < len(row):
                unit = row[unit_col_idx].strip()
                value, extra_unit = self._split_value_unit(value_raw)
                if not unit and extra_unit:
                    unit = extra_unit
            elif len(row) >= 3 and row[2].strip() and self._looks_like_unit(row[2].strip()):
                value, _ = self._split_value_unit(value_raw)
                unit = row[2].strip()
            else:
                value, unit = self._split_value_unit(value_raw)

            base_confidence = 0.9
            final_confidence = compute_source_confidence(
                base_confidence, source_url, is_table_source=True
            )
            specs.append(TechnicalSpec(
                brand=brand,
                model=model,
                parameter=param_name,
                value=value,
                unit=unit,
                source_url=source_url,
                confidence=final_confidence,
            ))

        logger.info(f"Extraidas {len(specs)} specs de tabla para {brand} {model}")
        return specs

    def _is_header_row(self, row: list[str]) -> bool:
        """Detecta si una fila parece ser header (palabras clave en celdas)."""
        header_keywords = {
            "parameter", "specification", "spec", "feature", "item",
            "value", "unit", "units", "model", "description",
            "parametro", "valor", "unidad", "especificacion",
        }
        text = " ".join(cell.strip().lower() for cell in row)
        return any(kw in text for kw in header_keywords)

    def _find_unit_column(self, header: list[str]) -> int | None:
        """Encuentra el indice de la columna de unidades en el header."""
        unit_keywords = {"unit", "units", "uom", "unidad", "unidades"}
        for i, cell in enumerate(header):
            if cell.strip().lower() in unit_keywords:
                return i
        return None

    def _looks_like_unit(self, text: str) -> bool:
        """Verifica si un texto parece ser una unidad de medida."""
        known_units = {
            "kg", "ton", "t", "lb", "hp", "kw", "kW", "m", "mm", "ft",
            "m3", "yd3", "km/h", "mph", "L/h", "l/h", "kN", "bar", "psi",
            "MPa", "kPa", "rpm", "L", "gal", "Nm", "V", "%", "°", "°/s",
            "L/min", "gpm", "in", "cm", "lbf", "kgf",
        }
        return text in known_units or len(text) <= 6

    def _map_table_param(self, raw_param: str) -> str:
        """Mapea nombre de parametro de tabla a nombre estandar."""
        for key, value in _TABLE_PARAM_MAPPING:
            if key in raw_param:
                return value
        return ""

    def _split_value_unit(self, raw: str) -> tuple[str, str]:
        """Separa valor numerico de su unidad."""
        match = _SPLIT_VALUE_UNIT_RE.match(raw)
        if match:
            return match.group(1).replace(",", ""), match.group(2).strip()
        return raw, ""


# Pre-sorted table parameter mapping (longest keys first for specificity)
_TABLE_PARAM_MAPPING_DICT = {
            # Peso
            "operating weight": "peso_operativo",
            "gross weight": "peso_operativo",
            "service weight": "peso_operativo",
            "empty weight": "peso_vacio",
            "tare weight": "peso_vacio",
            "shipping weight": "peso_vacio",
            "dry weight": "peso_vacio",
            # Motor
            "engine power": "potencia_motor",
            "net power": "potencia_motor",
            "gross power": "potencia_motor",
            "rated power": "potencia_motor",
            "engine model": "modelo_motor",
            "engine type": "modelo_motor",
            "engine": "modelo_motor",
            "torque": "torque",
            "max torque": "torque",
            "maximum torque": "torque",
            "displacement": "cilindrada",
            "engine displacement": "cilindrada",
            "number of cylinders": "numero_cilindros",
            "cylinders": "numero_cilindros",
            "emission": "norma_emisiones",
            "emissions": "norma_emisiones",
            "emission standard": "norma_emisiones",
            # Capacidades
            "bucket capacity": "capacidad_balde",
            "heaped capacity": "capacidad_balde",
            "struck capacity": "capacidad_balde",
            "dipper capacity": "capacidad_cucharon",
            "shovel capacity": "capacidad_cucharon",
            "payload": "capacidad_carga",
            "load capacity": "capacidad_carga",
            "rated payload": "capacidad_carga",
            "lifting capacity": "capacidad_elevacion",
            "lift capacity": "capacidad_elevacion",
            "fuel tank": "capacidad_tanque",
            "fuel tank capacity": "capacidad_tanque",
            "tank capacity": "capacidad_tanque",
            # Velocidades
            "max speed": "velocidad_maxima",
            "maximum speed": "velocidad_maxima",
            "top speed": "velocidad_maxima",
            "travel speed": "velocidad_maxima",
            "swing speed": "velocidad_giro",
            "slewing speed": "velocidad_giro",
            "swing rate": "velocidad_giro",
            # Dimensiones
            "overall width": "ancho_total",
            "machine width": "ancho_total",
            "width": "ancho_total",
            "overall length": "largo_total",
            "machine length": "largo_total",
            "length": "largo_total",
            "overall height": "altura_total",
            "machine height": "altura_total",
            "height": "altura_total",
            "dump height": "altura_descarga",
            "dumping height": "altura_descarga",
            "loading height": "altura_descarga",
            "discharge height": "altura_descarga",
            "digging depth": "profundidad_excavacion",
            "max digging depth": "profundidad_excavacion",
            "excavation depth": "profundidad_excavacion",
            "digging reach": "alcance_max",
            "max reach": "alcance_max",
            "maximum reach": "alcance_max",
            "reach": "alcance_max",
            "turning radius": "radio_giro",
            "min turning radius": "radio_giro",
            "track shoe width": "ancho_zapata",
            "shoe width": "ancho_zapata",
            # Hidraulica y fuerzas
            "digging force": "fuerza_excavacion",
            "breakout force": "fuerza_excavacion",
            "bucket force": "fuerza_excavacion",
            "hydraulic pressure": "presion_hidraulica",
            "system pressure": "presion_hidraulica",
            "hydraulic flow": "caudal_hidraulico",
            "pump flow": "caudal_hidraulico",
            "main pump flow": "caudal_hidraulico",
            # Otros
            "fuel consumption": "consumo_combustible",
            "ground pressure": "presion_suelo",
            "max gradeability": "pendiente_maxima",
            "gradeability": "pendiente_maxima",
            "grade": "pendiente_maxima",
            "max grade": "pendiente_maxima",
            "transmission": "tipo_transmision",
            "transmission type": "tipo_transmision",
            "tire size": "tamano_neumaticos",
            "tyre size": "tamano_neumaticos",
            "undercarriage": "tipo_rodamiento",
            "track type": "tipo_rodamiento",
            "system voltage": "voltaje_sistema",
            "voltage": "voltaje_sistema",
            "electrical system": "voltaje_sistema",
            # Rimpull / Traccion
            "rimpull": "rimpull_maximo",
            "max rimpull": "rimpull_maximo",
            "maximum rimpull": "rimpull_maximo",
            "tractive effort": "rimpull_maximo",
            "tractive force": "rimpull_maximo",
            "drawbar pull": "rimpull_maximo",
            "retarder": "velocidad_retardo",
            "retarder speed": "velocidad_retardo",
            "dynamic braking": "velocidad_retardo",
            "forward gears": "numero_marchas",
            "forward speeds": "numero_marchas",
            "number of gears": "numero_marchas",
}

# Pre-sorted: longest keys first so "dump height" matches before "height"
_TABLE_PARAM_MAPPING = sorted(
    _TABLE_PARAM_MAPPING_DICT.items(), key=lambda x: len(x[0]), reverse=True
)
del _TABLE_PARAM_MAPPING_DICT  # free the dict; only the sorted list is used


# Rangos esperados para validacion (min, max) en unidades estandar
VALID_RANGES = {
    "peso_operativo": {"ton": (10, 1500), "kg": (10000, 1500000), "lb": (22000, 3300000)},
    "potencia_motor": {"hp": (50, 5000), "kW": (37, 3728)},
    "capacidad_balde": {"m3": (1, 65), "yd3": (1.3, 85)},
    "capacidad_carga": {"ton": (20, 500), "kg": (20000, 500000)},
    "velocidad_maxima": {"km/h": (5, 70), "mph": (3, 45)},
    "consumo_combustible": {"L/h": (10, 1000), "gal/h": (2.6, 264)},
    "torque": {"Nm": (100, 30000), "kNm": (0.1, 30), "lb-ft": (74, 22000)},
    "cilindrada": {"L": (3, 120), "cc": (3000, 120000)},
    "fuerza_excavacion": {"kN": (50, 3000), "kgf": (5000, 300000), "tf": (5, 300)},
    "presion_hidraulica": {"bar": (100, 600), "psi": (1450, 8700), "MPa": (10, 60)},
    # Nuevos parametros
    "capacidad_tanque": {"L": (100, 10000), "gal": (26, 2640)},
    "profundidad_excavacion": {"m": (1, 25), "mm": (1000, 25000), "ft": (3, 82)},
    "alcance_max": {"m": (3, 30), "mm": (3000, 30000), "ft": (10, 100)},
    "altura_descarga": {"m": (2, 25), "mm": (2000, 25000), "ft": (6, 82)},
    "presion_suelo": {"kPa": (20, 300), "bar": (0.2, 3), "psi": (3, 44), "kg/cm2": (0.2, 3)},
    "velocidad_giro": {"rpm": (1, 15), "°/s": (1, 90)},
    "pendiente_maxima": {"%": (10, 70), "°": (5, 35)},
    "radio_giro": {"m": (3, 30), "mm": (3000, 30000), "ft": (10, 100)},
    "ancho_total": {"m": (2, 15), "mm": (2000, 15000), "ft": (6, 50)},
    "largo_total": {"m": (3, 25), "mm": (3000, 25000), "ft": (10, 82)},
    "altura_total": {"m": (2, 15), "mm": (2000, 15000), "ft": (6, 50)},
    "caudal_hidraulico": {"L/min": (50, 5000), "gpm": (13, 1320)},
    "capacidad_elevacion": {"ton": (5, 500), "kg": (5000, 500000)},
    "peso_vacio": {"ton": (8, 1200), "kg": (8000, 1200000), "lb": (17600, 2640000)},
    "ancho_zapata": {"mm": (300, 1500), "in": (12, 60)},
    "capacidad_cucharon": {"m3": (1, 65), "yd3": (1.3, 85)},
    "voltaje_sistema": {"V": (12, 1200)},
    "rimpull_maximo": {"kN": (50, 2000), "kgf": (5000, 200000), "lbf": (11000, 450000)},
}


def validate_spec(spec: TechnicalSpec) -> TechnicalSpec:
    """Valida que el valor este dentro de rangos razonables para equipos mineros.
    Si esta fuera de rango, reduce confidence a 0.3 en lugar de eliminar."""
    if spec.parameter not in VALID_RANGES:
        return spec

    ranges = VALID_RANGES[spec.parameter]
    if spec.unit not in ranges:
        return spec

    try:
        val = float(spec.value)
    except (ValueError, TypeError):
        return spec

    lo, hi = ranges[spec.unit]
    if not (lo <= val <= hi):
        logger.warning(
            f"Spec fuera de rango: {spec.parameter}={spec.value} {spec.unit} "
            f"(esperado {lo}-{hi}) para {spec.brand} {spec.model}"
        )
        spec.confidence = 0.3

    return spec


def normalize_spec(spec: TechnicalSpec) -> TechnicalSpec:
    """Normaliza unidades a estandar (ej: kg → ton para peso_operativo)."""
    try:
        val = float(spec.value)
    except (ValueError, TypeError):
        return spec

    # kg → ton para parametros de peso
    kg_to_ton_params = {"peso_operativo", "capacidad_carga", "capacidad_elevacion", "peso_vacio"}
    if spec.parameter in kg_to_ton_params and spec.unit == "kg":
        spec.value = str(round(val / 1000, 2))
        spec.unit = "ton"
    # mm → m para dimensiones
    elif spec.parameter in {"profundidad_excavacion", "alcance_max", "altura_descarga",
                            "radio_giro", "ancho_total", "largo_total", "altura_total"} and spec.unit == "mm":
        spec.value = str(round(val / 1000, 3))
        spec.unit = "m"

    return spec


def build_equipment_profile(
    brand: str,
    model: str,
    equipment_type: str,
    text_specs: list[TechnicalSpec],
    table_specs: list[TechnicalSpec],
) -> EquipmentProfile:
    """Construye perfil consolidado, priorizando specs de tabla sobre texto.
    Aplica normalizacion y validacion, y al deduplicar prefiere la spec
    con mayor confidence que pase validacion."""
    profile = EquipmentProfile(
        brand=brand, model=model, equipment_type=equipment_type
    )

    # Normalizar y validar todas las specs
    all_specs = []
    for spec in table_specs + text_specs:
        spec = normalize_spec(spec)
        spec = validate_spec(spec)
        all_specs.append(spec)

    # Deduplicar: por cada parametro, elegir la spec con mayor confidence
    best_by_param: dict[str, TechnicalSpec] = {}
    for spec in all_specs:
        existing = best_by_param.get(spec.parameter)
        if existing is None or spec.confidence > existing.confidence:
            best_by_param[spec.parameter] = spec

    profile.specs = list(best_by_param.values())
    return profile
