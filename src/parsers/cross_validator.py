"""
Validacion cruzada multi-fuente para especificaciones tecnicas.
Compara valores del mismo parametro extraidos de diferentes fuentes
para detectar conflictos, outliers y generar confianza por consenso.
"""

import logging
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class SpecCandidate:
    """Un valor candidato para un parametro de un equipo."""
    parameter: str
    value: str
    unit: str
    confidence: float
    source_url: str

    @property
    def numeric_value(self) -> float | None:
        try:
            return float(self.value.replace(",", ""))
        except (ValueError, TypeError):
            return None


@dataclass
class ValidationResult:
    """Resultado de la validacion cruzada para un parametro."""
    parameter: str
    best_value: str
    best_unit: str
    final_confidence: float
    source_count: int
    consensus: bool   # True si las fuentes coinciden
    sources: list[str] = field(default_factory=list)
    conflict_detail: str = ""


# Tolerancia para considerar dos valores "iguales" (porcentaje de diferencia)
TOLERANCE_PERCENT = {
    "peso_operativo": 5.0,
    "potencia_motor": 3.0,
    "capacidad_balde": 8.0,
    "capacidad_carga": 5.0,
    "capacidad_cucharon": 8.0,
    "velocidad_maxima": 5.0,
    "consumo_combustible": 10.0,
    "torque": 5.0,
    "cilindrada": 2.0,
    "fuerza_excavacion": 5.0,
    "presion_hidraulica": 3.0,
    "capacidad_tanque": 5.0,
    "rimpull_maximo": 5.0,
}
DEFAULT_TOLERANCE = 10.0


def validate_across_sources(
    candidates: list[SpecCandidate],
) -> ValidationResult | None:
    """Valida un conjunto de candidatos para el mismo parametro/equipo.

    Si hay consenso (valores similares), aumenta la confianza.
    Si hay conflicto (valores muy diferentes), marca la discrepancia
    y elige el valor con mayor confianza.

    Args:
        candidates: Lista de SpecCandidate con el mismo parameter para el mismo equipo.

    Returns:
        ValidationResult con el mejor valor y confianza ajustada, o None si no hay candidatos.
    """
    if not candidates:
        return None

    param = candidates[0].parameter

    # Si solo hay 1 fuente, retornar tal cual
    if len(candidates) == 1:
        c = candidates[0]
        return ValidationResult(
            parameter=param,
            best_value=c.value,
            best_unit=c.unit,
            final_confidence=c.confidence,
            source_count=1,
            consensus=True,
            sources=[c.source_url],
        )

    # Separar candidatos numericos de no-numericos
    numeric = [(c, c.numeric_value) for c in candidates if c.numeric_value is not None]
    non_numeric = [c for c in candidates if c.numeric_value is None]

    if not numeric:
        # Solo hay valores no numericos — elegir el de mayor confidence
        best = max(non_numeric, key=lambda c: c.confidence)
        return ValidationResult(
            parameter=param,
            best_value=best.value,
            best_unit=best.unit,
            final_confidence=best.confidence,
            source_count=len(non_numeric),
            consensus=len(set(c.value.lower() for c in non_numeric)) == 1,
            sources=[c.source_url for c in non_numeric],
        )

    # Agrupar valores numericos en clusters de consenso
    tolerance = TOLERANCE_PERCENT.get(param, DEFAULT_TOLERANCE)
    clusters = _cluster_values(numeric, tolerance)

    # El cluster mas grande es el consenso
    main_cluster = max(clusters, key=len)
    outliers = [item for cluster in clusters if cluster is not main_cluster for item in cluster]

    # Elegir el mejor del cluster principal (mayor confidence)
    best_candidate, best_val = max(main_cluster, key=lambda x: x[0].confidence)

    # Calcular confianza ajustada
    n_sources = len(candidates)
    n_agree = len(main_cluster)
    consensus_ratio = n_agree / n_sources

    # Bonus por consenso: +0.1 si todas coinciden, proporcional si no
    consensus_bonus = 0.1 * consensus_ratio if n_sources > 1 else 0.0
    final_conf = min(best_candidate.confidence + consensus_bonus, 1.0)

    # Penalizar si hay conflicto significativo
    conflict_detail = ""
    if outliers:
        outlier_vals = [f"{v:.1f}" for _, v in outliers]
        conflict_detail = (
            f"Conflicto: {n_agree}/{n_sources} fuentes coinciden en ~{best_val:.1f}. "
            f"Outliers: {', '.join(outlier_vals)}"
        )
        logger.warning(f"{param}: {conflict_detail}")
        # Reducir confidence si hay conflicto
        final_conf = min(final_conf, 0.85)

    return ValidationResult(
        parameter=param,
        best_value=best_candidate.value,
        best_unit=best_candidate.unit,
        final_confidence=round(final_conf, 3),
        source_count=n_sources,
        consensus=len(outliers) == 0,
        sources=[c.source_url for c, _ in main_cluster],
        conflict_detail=conflict_detail,
    )


def _cluster_values(
    items: list[tuple[SpecCandidate, float]], tolerance_pct: float
) -> list[list[tuple[SpecCandidate, float]]]:
    """Agrupa valores numericos en clusters basados en tolerancia porcentual."""
    if not items:
        return []

    # Ordenar por valor
    sorted_items = sorted(items, key=lambda x: x[1])
    clusters: list[list[tuple[SpecCandidate, float]]] = [[sorted_items[0]]]

    for item in sorted_items[1:]:
        _, val = item
        # Comparar con el promedio del cluster actual
        cluster_avg = sum(v for _, v in clusters[-1]) / len(clusters[-1])
        if cluster_avg > 0:
            diff_pct = abs(val - cluster_avg) / cluster_avg * 100
        else:
            diff_pct = 100  # Si el promedio es 0, tratar como diferente

        if diff_pct <= tolerance_pct:
            clusters[-1].append(item)
        else:
            clusters.append([item])

    return clusters


def cross_validate_rimpull_curves(curves: list) -> "RimpullCurve | None":
    """Valida cruzadamente curvas rimpull de multiples fuentes.

    Para cada marcha presente en alguna fuente, agrupa los valores de fuerza,
    aplica tolerancia del 10% para consenso, y elige el valor con mayor confidence.

    Args:
        curves: Lista de RimpullCurve del mismo equipo (de distintas fuentes).

    Returns:
        RimpullCurve consolidada, o None si no hay datos.
    """
    from src.parsers.rimpull_extractor import RimpullCurve, RimpullPoint, normalize_gear, sort_points_by_gear

    if not curves:
        return None

    if len(curves) == 1:
        return curves[0]

    brand = curves[0].brand
    model = curves[0].model

    # Group all points by normalized gear
    by_gear: dict[str, list[RimpullPoint]] = {}
    for curve in curves:
        for point in curve.points:
            gear = normalize_gear(point.gear)
            by_gear.setdefault(gear, []).append(point)

    consolidated_points: list[RimpullPoint] = []
    tolerance_pct = 10.0

    for gear, points in by_gear.items():
        if len(points) == 1:
            consolidated_points.append(points[0])
            continue

        # Cluster force values with tolerance
        sorted_pts = sorted(points, key=lambda p: p.force_kn)
        clusters: list[list[RimpullPoint]] = [[sorted_pts[0]]]

        for pt in sorted_pts[1:]:
            cluster_avg = sum(p.force_kn for p in clusters[-1]) / len(clusters[-1])
            if cluster_avg > 0:
                diff_pct = abs(pt.force_kn - cluster_avg) / cluster_avg * 100
            else:
                diff_pct = 100
            if diff_pct <= tolerance_pct:
                clusters[-1].append(pt)
            else:
                clusters.append([pt])

        # Pick the largest cluster, then best confidence within it
        main_cluster = max(clusters, key=len)
        best_point = max(main_cluster, key=lambda p: p.confidence)

        # Consensus bonus
        n_sources = len(points)
        n_agree = len(main_cluster)
        consensus_bonus = 0.1 * (n_agree / n_sources) if n_sources > 1 else 0.0
        adjusted_conf = min(best_point.confidence + consensus_bonus, 1.0)

        if len(clusters) > 1:
            adjusted_conf = min(adjusted_conf, 0.85)
            outlier_vals = [
                p.force_kn for c in clusters if c is not main_cluster for p in c
            ]
            logger.warning(
                f"Rimpull {gear}: {n_agree}/{n_sources} fuentes coinciden "
                f"en ~{best_point.force_kn:.1f} kN. Outliers: {outlier_vals}"
            )

        # Average speed from agreeing sources
        speeds = [p.speed_kmh for p in main_cluster if p.speed_kmh is not None]
        avg_speed = sum(speeds) / len(speeds) if speeds else None

        consolidated_points.append(RimpullPoint(
            gear=gear,
            speed_kmh=round(avg_speed, 1) if avg_speed is not None else None,
            force_kn=round(best_point.force_kn, 2),
            original_unit=best_point.original_unit,
            confidence=round(adjusted_conf, 3),
            source_url=best_point.source_url,
        ))

    if len(consolidated_points) < 2:
        return None

    consolidated_points = sort_points_by_gear(consolidated_points)
    return RimpullCurve(brand=brand, model=model, points=consolidated_points)


def cross_validate_equipment_specs(
    all_specs: list[SpecCandidate],
) -> list[ValidationResult]:
    """Valida cruzadamente todas las specs de un equipo.

    Agrupa candidatos por parametro y aplica validacion cruzada a cada grupo.

    Args:
        all_specs: Lista de todos los SpecCandidate para un equipo (de multiples fuentes).

    Returns:
        Lista de ValidationResult, uno por parametro.
    """
    # Agrupar por parametro
    by_param: dict[str, list[SpecCandidate]] = {}
    for spec in all_specs:
        by_param.setdefault(spec.parameter, []).append(spec)

    results = []
    for param, candidates in by_param.items():
        result = validate_across_sources(candidates)
        if result:
            results.append(result)

    return results
