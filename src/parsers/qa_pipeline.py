"""
Pipeline de QA (Quality Assurance) post-extraccion.
Valida, limpia y filtra specs extraidas para eliminar datos inutilizables.
"""

import re
import logging
from dataclasses import dataclass, field

from src.parsers.spec_extractor import TechnicalSpec, VALID_RANGES

logger = logging.getLogger(__name__)


@dataclass
class QAResult:
    """Resultado del QA de una spec."""
    spec: TechnicalSpec
    passed: bool
    issues: list[str] = field(default_factory=list)


# Patrones de placeholders y valores vacios
_PLACEHOLDER_PATTERNS = [
    re.compile(r"^[-–—]+$"),                    # Solo guiones
    re.compile(r"^[nN]\/?[aA]$"),               # N/A, n/a
    re.compile(r"^(tbd|tba|pending|ask)$", re.I),  # TBD, etc
    re.compile(r"^\*+$"),                        # Solo asteriscos
    re.compile(r"^\.+$"),                        # Solo puntos
    re.compile(r"^(na|none|null|nil)$", re.I),   # none, null, etc
    re.compile(r"^0+(\.0+)?$"),                  # Cero (0, 0.0, 00)
    re.compile(r"^\s*$"),                        # Espacios en blanco
    re.compile(r"^(contact|consult|available)", re.I),  # "contact dealer"
    re.compile(r"^(option|optional|standard)$", re.I),  # No es un valor
]

# Combinaciones de parametros con restricciones fisicas
# (param_a, param_b, regla): param_a debe ser menor/mayor que param_b
PHYSICAL_CONSTRAINTS = [
    ("peso_vacio", "peso_operativo", "less_than"),  # Peso vacio < peso operativo
]

# Parametros minimos esperados para un equipo completo
CORE_PARAMS_EXCAVADORA = {"peso_operativo", "potencia_motor", "capacidad_balde"}
CORE_PARAMS_CAMION = {"peso_operativo", "potencia_motor", "capacidad_carga"}
CORE_PARAMS_GENERIC = {"peso_operativo", "potencia_motor"}


def qa_single_spec(spec: TechnicalSpec) -> QAResult:
    """Valida una spec individual.

    Checks:
    1. Valor no vacio
    2. Valor no es placeholder
    3. Valor numerico valido (si se espera numerico)
    4. Valor dentro de rango razonable
    5. Unidad presente para parametros numericos
    """
    issues = []

    # 1. Valor vacio
    if not spec.value or not spec.value.strip():
        issues.append("valor_vacio")
        return QAResult(spec=spec, passed=False, issues=issues)

    value = spec.value.strip()

    # 2. Placeholder
    for pattern in _PLACEHOLDER_PATTERNS:
        if pattern.match(value):
            issues.append(f"placeholder: '{value}'")
            return QAResult(spec=spec, passed=False, issues=issues)

    # 3. Parametros que esperan valores numericos
    non_numeric_params = {"modelo_motor", "norma_emisiones", "tipo_transmision",
                          "tamano_neumaticos", "tipo_rodamiento"}
    if spec.parameter not in non_numeric_params:
        try:
            float(value.replace(",", ""))
        except (ValueError, TypeError):
            issues.append(f"no_numerico: '{value}'")
            return QAResult(spec=spec, passed=False, issues=issues)

    # 4. Rango valido
    if spec.parameter in VALID_RANGES and spec.unit:
        ranges = VALID_RANGES[spec.parameter]
        if spec.unit in ranges:
            try:
                val = float(value.replace(",", ""))
                lo, hi = ranges[spec.unit]
                if val < lo * 0.5 or val > hi * 2.0:
                    issues.append(f"fuera_de_rango: {val} {spec.unit} (esperado {lo}-{hi})")
            except (ValueError, TypeError):
                pass

    # 5. Unidad faltante para parametros numericos
    if spec.parameter not in non_numeric_params and not spec.unit:
        issues.append("unidad_faltante")
        # No rechazar, solo advertir (puede inferirse)

    passed = not any(
        issue.startswith(("valor_vacio", "placeholder", "no_numerico", "fuera_de_rango"))
        for issue in issues
    )
    return QAResult(spec=spec, passed=passed, issues=issues)


def qa_equipment_specs(
    specs: list[TechnicalSpec], equipment_type: str = ""
) -> tuple[list[TechnicalSpec], dict]:
    """Ejecuta QA sobre todas las specs de un equipo.

    Args:
        specs: Lista de specs extraidas.
        equipment_type: Tipo de equipo (para seleccionar core params).

    Returns:
        Tupla de (specs_validas, reporte_qa).
    """
    valid_specs = []
    rejected = []
    warnings = []

    for spec in specs:
        result = qa_single_spec(spec)
        if result.passed:
            valid_specs.append(spec)
            if result.issues:
                warnings.append({"spec": f"{spec.parameter}={spec.value}", "issues": result.issues})
        else:
            rejected.append({"spec": f"{spec.parameter}={spec.value}", "issues": result.issues})

    # Verificar restricciones fisicas entre parametros
    spec_by_param = {s.parameter: s for s in valid_specs}
    constraint_warnings = _check_physical_constraints(spec_by_param)
    warnings.extend(constraint_warnings)

    # Completeness scoring
    et_lower = equipment_type.lower() if equipment_type else ""
    if "excav" in et_lower or "pala" in et_lower or "shovel" in et_lower:
        core_params = CORE_PARAMS_EXCAVADORA
    elif "cami" in et_lower or "truck" in et_lower or "haul" in et_lower:
        core_params = CORE_PARAMS_CAMION
    else:
        core_params = CORE_PARAMS_GENERIC

    found_core = core_params & set(spec_by_param.keys())
    completeness = len(found_core) / len(core_params) if core_params else 0.0

    report = {
        "total_input": len(specs),
        "total_valid": len(valid_specs),
        "total_rejected": len(rejected),
        "rejection_rate": round(len(rejected) / len(specs) * 100, 1) if specs else 0,
        "completeness": round(completeness, 2),
        "missing_core_params": list(core_params - found_core),
        "rejected": rejected,
        "warnings": warnings,
    }

    if rejected:
        logger.info(
            f"QA: {len(valid_specs)}/{len(specs)} specs pasaron "
            f"({report['rejection_rate']}% rechazadas)"
        )

    return valid_specs, report


def qa_rimpull_curve(curve) -> tuple:
    """Valida una curva rimpull completa.

    Args:
        curve: RimpullCurve con puntos a validar.

    Returns:
        Tupla de (curva_validada_o_None, reporte_qa).
        Si la curva no pasa QA, retorna (None, reporte).
    """
    from src.parsers.rimpull_extractor import RimpullCurve, GEAR_ORDER

    issues = []
    valid_points = []

    for point in curve.points:
        # Detectar placeholders
        if point.force_kn == 0:
            issues.append(f"placeholder: fuerza=0 en {point.gear}")
            continue

        # Fuerza en rango razonable: 50-2000 kN
        # Tolerancia 2x vs spec_extractor ranges (rimpull_maximo 50-2000 kN)
        # to accommodate multi-gear curves where individual points may vary
        if not (50 <= point.force_kn <= 2000):
            issues.append(
                f"fuera_de_rango: fuerza={point.force_kn} kN en {point.gear} (esperado 50-2000)"
            )
            continue

        # Velocidad en rango: 0-80 km/h (si disponible)
        if point.speed_kmh is not None:
            if not (0 < point.speed_kmh <= 80):
                issues.append(
                    f"fuera_de_rango: velocidad={point.speed_kmh} km/h en {point.gear} (esperado 0-80)"
                )
                continue

        valid_points.append(point)

    # Minimo 2 puntos validos
    if len(valid_points) < 2:
        issues.append(f"insuficientes_puntos: {len(valid_points)} (minimo 2)")
        report = {
            "passed": False,
            "total_input": len(curve.points),
            "total_valid": len(valid_points),
            "issues": issues,
        }
        return None, report

    # Monotonicity check: fuerza debe decrecer a medida que sube la marcha
    monotonicity_violations = []
    sorted_points = sorted(valid_points, key=lambda p: GEAR_ORDER.get(p.gear, 99))
    # Only check among non-Reverse gears
    forward_points = [p for p in sorted_points if p.gear != "Reverse"]
    for i in range(1, len(forward_points)):
        if forward_points[i].force_kn > forward_points[i - 1].force_kn:
            monotonicity_violations.append(
                f"monotonicity: {forward_points[i].gear}={forward_points[i].force_kn} kN "
                f"> {forward_points[i-1].gear}={forward_points[i-1].force_kn} kN"
            )

    if monotonicity_violations:
        issues.extend(monotonicity_violations)
        # Monotonicity violation is a warning, not a rejection
        logger.warning(
            f"Curva rimpull {curve.brand} {curve.model}: violacion de monotonicity"
        )

    validated_curve = RimpullCurve(
        brand=curve.brand,
        model=curve.model,
        points=sorted_points,
    )

    report = {
        "passed": True,
        "total_input": len(curve.points),
        "total_valid": len(valid_points),
        "issues": issues,
    }
    return validated_curve, report


def _check_physical_constraints(
    spec_by_param: dict[str, TechnicalSpec],
) -> list[dict]:
    """Verifica restricciones fisicas entre pares de parametros."""
    warnings = []
    for param_a, param_b, rule in PHYSICAL_CONSTRAINTS:
        if param_a not in spec_by_param or param_b not in spec_by_param:
            continue
        spec_a = spec_by_param[param_a]
        spec_b = spec_by_param[param_b]

        # Solo comparar si tienen la misma unidad
        if spec_a.unit != spec_b.unit:
            continue

        try:
            val_a = float(spec_a.value.replace(",", ""))
            val_b = float(spec_b.value.replace(",", ""))
        except (ValueError, TypeError):
            continue

        if rule == "less_than" and val_a >= val_b:
            warnings.append({
                "constraint": f"{param_a} < {param_b}",
                "issue": f"{param_a}={val_a} >= {param_b}={val_b}",
            })

    return warnings
