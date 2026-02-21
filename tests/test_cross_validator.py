"""Tests para validacion cruzada multi-fuente."""

from src.parsers.cross_validator import (
    SpecCandidate,
    validate_across_sources,
    cross_validate_equipment_specs,
)


class TestValidateAcrossSources:
    def test_single_source(self):
        candidates = [
            SpecCandidate("peso_operativo", "700", "ton", 0.8, "https://a.com"),
        ]
        result = validate_across_sources(candidates)
        assert result is not None
        assert result.best_value == "700"
        assert result.source_count == 1
        assert result.consensus is True

    def test_two_agreeing_sources(self):
        candidates = [
            SpecCandidate("peso_operativo", "700", "ton", 0.8, "https://a.com"),
            SpecCandidate("peso_operativo", "702", "ton", 0.85, "https://b.com"),
        ]
        result = validate_across_sources(candidates)
        assert result is not None
        assert result.consensus is True
        assert result.source_count == 2
        # Confidence boosted by consensus
        assert result.final_confidence > 0.85

    def test_conflicting_sources(self):
        candidates = [
            SpecCandidate("peso_operativo", "700", "ton", 0.8, "https://a.com"),
            SpecCandidate("peso_operativo", "500", "ton", 0.6, "https://b.com"),
        ]
        result = validate_across_sources(candidates)
        assert result is not None
        assert result.consensus is False
        assert "Conflicto" in result.conflict_detail

    def test_three_sources_majority_wins(self):
        candidates = [
            SpecCandidate("peso_operativo", "700", "ton", 0.8, "https://a.com"),
            SpecCandidate("peso_operativo", "702", "ton", 0.75, "https://b.com"),
            SpecCandidate("peso_operativo", "300", "ton", 0.6, "https://c.com"),  # outlier
        ]
        result = validate_across_sources(candidates)
        assert result is not None
        # The majority (700, 702) should win over the outlier (300)
        assert float(result.best_value) > 600
        assert result.source_count == 3

    def test_non_numeric_values(self):
        candidates = [
            SpecCandidate("modelo_motor", "Cummins QSK60", "", 0.8, "https://a.com"),
            SpecCandidate("modelo_motor", "Cummins QSK60", "", 0.7, "https://b.com"),
        ]
        result = validate_across_sources(candidates)
        assert result is not None
        assert result.best_value == "Cummins QSK60"
        assert result.consensus is True

    def test_empty_candidates(self):
        result = validate_across_sources([])
        assert result is None


class TestCrossValidateEquipmentSpecs:
    def test_multiple_params(self):
        specs = [
            SpecCandidate("peso_operativo", "700", "ton", 0.8, "https://a.com"),
            SpecCandidate("peso_operativo", "702", "ton", 0.85, "https://b.com"),
            SpecCandidate("potencia_motor", "2500", "hp", 0.9, "https://a.com"),
        ]
        results = cross_validate_equipment_specs(specs)
        assert len(results) == 2
        params = {r.parameter for r in results}
        assert "peso_operativo" in params
        assert "potencia_motor" in params
