"""Tests para el pipeline de QA post-extraccion."""

from src.parsers.spec_extractor import TechnicalSpec
from src.parsers.qa_pipeline import qa_single_spec, qa_equipment_specs


class TestQASingleSpec:
    def test_valid_spec_passes(self):
        spec = TechnicalSpec("CAT", "797F", "peso_operativo", "623", "ton", "https://cat.com", 0.9)
        result = qa_single_spec(spec)
        assert result.passed is True
        assert len(result.issues) == 0

    def test_empty_value_fails(self):
        spec = TechnicalSpec("CAT", "797F", "peso_operativo", "", "ton", "", 0.5)
        result = qa_single_spec(spec)
        assert result.passed is False
        assert "valor_vacio" in result.issues

    def test_placeholder_na_fails(self):
        spec = TechnicalSpec("CAT", "797F", "peso_operativo", "N/A", "ton", "", 0.5)
        result = qa_single_spec(spec)
        assert result.passed is False
        assert any("placeholder" in i for i in result.issues)

    def test_placeholder_dash_fails(self):
        spec = TechnicalSpec("CAT", "797F", "peso_operativo", "---", "ton", "", 0.5)
        result = qa_single_spec(spec)
        assert result.passed is False

    def test_placeholder_tbd_fails(self):
        spec = TechnicalSpec("CAT", "797F", "peso_operativo", "TBD", "ton", "", 0.5)
        result = qa_single_spec(spec)
        assert result.passed is False

    def test_non_numeric_for_numeric_param_fails(self):
        spec = TechnicalSpec("CAT", "797F", "peso_operativo", "heavy", "ton", "", 0.5)
        result = qa_single_spec(spec)
        assert result.passed is False
        assert any("no_numerico" in i for i in result.issues)

    def test_non_numeric_param_accepts_text(self):
        spec = TechnicalSpec("CAT", "797F", "modelo_motor", "Cummins QSK60", "", "", 0.8)
        result = qa_single_spec(spec)
        assert result.passed is True

    def test_norma_emisiones_accepts_text(self):
        spec = TechnicalSpec("CAT", "797F", "norma_emisiones", "Tier 4 Final", "", "", 0.8)
        result = qa_single_spec(spec)
        assert result.passed is True

    def test_out_of_range_extreme_fails(self):
        # 0.001 ton is way below 10 ton minimum * 0.5 = 5 ton
        spec = TechnicalSpec("CAT", "797F", "peso_operativo", "0.001", "ton", "", 0.5)
        result = qa_single_spec(spec)
        assert result.passed is False

    def test_zero_value_rejected(self):
        spec = TechnicalSpec("CAT", "797F", "peso_operativo", "0", "ton", "", 0.5)
        result = qa_single_spec(spec)
        assert result.passed is False

    def test_missing_unit_warns_but_passes(self):
        spec = TechnicalSpec("CAT", "797F", "peso_operativo", "623", "", "", 0.8)
        result = qa_single_spec(spec)
        assert result.passed is True
        assert any("unidad_faltante" in i for i in result.issues)


class TestQAEquipmentSpecs:
    def test_filters_bad_specs(self):
        specs = [
            TechnicalSpec("CAT", "797F", "peso_operativo", "623", "ton", "", 0.9),
            TechnicalSpec("CAT", "797F", "potencia_motor", "N/A", "hp", "", 0.5),
            TechnicalSpec("CAT", "797F", "capacidad_carga", "400", "ton", "", 0.8),
            TechnicalSpec("CAT", "797F", "velocidad_maxima", "", "km/h", "", 0.3),
        ]
        valid, report = qa_equipment_specs(specs)
        assert len(valid) == 2
        assert report["total_rejected"] == 2
        assert report["total_valid"] == 2

    def test_completeness_scoring_truck(self):
        specs = [
            TechnicalSpec("CAT", "797F", "peso_operativo", "623", "ton", "", 0.9),
            TechnicalSpec("CAT", "797F", "potencia_motor", "3400", "hp", "", 0.9),
            TechnicalSpec("CAT", "797F", "capacidad_carga", "400", "ton", "", 0.8),
        ]
        valid, report = qa_equipment_specs(specs, equipment_type="Camion Minero")
        assert report["completeness"] == 1.0
        assert len(report["missing_core_params"]) == 0

    def test_completeness_scoring_partial(self):
        specs = [
            TechnicalSpec("CAT", "797F", "peso_operativo", "623", "ton", "", 0.9),
        ]
        valid, report = qa_equipment_specs(specs, equipment_type="Camion Minero")
        assert report["completeness"] < 1.0
        assert len(report["missing_core_params"]) > 0

    def test_physical_constraint_warning(self):
        specs = [
            TechnicalSpec("CAT", "797F", "peso_vacio", "700", "ton", "", 0.9),
            TechnicalSpec("CAT", "797F", "peso_operativo", "500", "ton", "", 0.9),
        ]
        valid, report = qa_equipment_specs(specs)
        # peso_vacio > peso_operativo should generate a warning
        assert len(report["warnings"]) >= 1
