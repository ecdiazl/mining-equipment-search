"""Tests para el extractor de especificaciones tecnicas — originales + nuevos parametros."""

import pytest
from src.parsers.spec_extractor import (
    SpecExtractor,
    TechnicalSpec,
    validate_spec,
    normalize_spec,
    build_equipment_profile,
    SPEC_PATTERNS,
)


@pytest.fixture
def extractor():
    return SpecExtractor()


class TestSpecExtractor:
    def test_extract_operating_weight(self, extractor):
        text = "The Komatsu PC8000 has an operating weight of 780 ton."
        specs = extractor.extract_from_text(text, "Komatsu", "PC8000")
        weights = [s for s in specs if s.parameter == "peso_operativo"]
        assert len(weights) >= 1
        assert weights[0].value == "780"
        assert weights[0].unit == "ton"

    def test_extract_engine_power(self, extractor):
        text = "Engine power: 2500 hp at 1800 rpm"
        specs = extractor.extract_from_text(text, "Hitachi", "EX5600")
        powers = [s for s in specs if s.parameter == "potencia_motor"]
        assert len(powers) >= 1
        assert powers[0].value == "2500"

    def test_extract_payload(self, extractor):
        text = "Payload capacity: 360 tonnes"
        specs = extractor.extract_from_text(text, "Liebherr", "T 284")
        payloads = [s for s in specs if s.parameter == "capacidad_carga"]
        assert len(payloads) >= 1
        assert payloads[0].value == "360"

    def test_extract_bucket_capacity(self, extractor):
        text = "Bucket capacity: 42.0 m3 heaped"
        specs = extractor.extract_from_text(text, "XCMG", "XE7000")
        buckets = [s for s in specs if s.parameter == "capacidad_balde"]
        assert len(buckets) >= 1
        assert buckets[0].value == "42.0"

    def test_extract_from_table(self, extractor):
        table = [
            ["Operating Weight", "780 ton"],
            ["Engine Power", "2 x 1650 kW"],
            ["Bucket Capacity", "42 m3"],
        ]
        specs = extractor.extract_from_table(table, "Komatsu", "PC8000")
        assert len(specs) >= 2

    def test_no_false_positives(self, extractor):
        text = "Contact us for more information about our mining solutions."
        specs = extractor.extract_from_text(text, "SANY", "SKT90S")
        assert len(specs) == 0

    def test_spanish_patterns(self, extractor):
        text = "Peso operativo: 350 ton. Potencia: 1500 hp."
        specs = extractor.extract_from_text(text, "BELAZ", "75306")
        assert len(specs) >= 2


class TestNewParameters:
    """Verificar que los 20+ nuevos parametros se extraen correctamente."""

    def test_total_params_count(self):
        """Deben existir al menos 30 parametros (11 originales + 20+ nuevos)."""
        assert len(SPEC_PATTERNS) >= 30

    def test_capacidad_tanque(self, extractor):
        text = "Fuel tank capacity: 3,500 L"
        specs = extractor.extract_from_text(text, "CAT", "797F", "https://cat.com")
        found = [s for s in specs if s.parameter == "capacidad_tanque"]
        assert len(found) >= 1
        assert found[0].value == "3500"
        assert found[0].unit == "L"

    def test_norma_emisiones(self, extractor):
        text = "Emission standard: Tier 4 Final"
        specs = extractor.extract_from_text(text, "CAT", "797F", "https://cat.com")
        found = [s for s in specs if s.parameter == "norma_emisiones"]
        assert len(found) >= 1

    def test_profundidad_excavacion(self, extractor):
        text = "Maximum digging depth: 8.5 m"
        specs = extractor.extract_from_text(text, "XCMG", "XE7000", "https://xcmg.com")
        found = [s for s in specs if s.parameter == "profundidad_excavacion"]
        assert len(found) >= 1
        assert found[0].value == "8.5"

    def test_altura_descarga(self, extractor):
        text = "Dump height: 7.2 m at full lift"
        specs = extractor.extract_from_text(text, "CAT", "793F", "https://cat.com")
        found = [s for s in specs if s.parameter == "altura_descarga"]
        assert len(found) >= 1

    def test_presion_suelo(self, extractor):
        text = "Ground pressure: 120 kPa"
        specs = extractor.extract_from_text(text, "XCMG", "XE7000", "https://xcmg.com")
        found = [s for s in specs if s.parameter == "presion_suelo"]
        assert len(found) >= 1
        assert found[0].unit == "kPa"

    def test_velocidad_giro(self, extractor):
        text = "Swing speed: 4.5 rpm"
        specs = extractor.extract_from_text(text, "XCMG", "XE7000", "https://xcmg.com")
        found = [s for s in specs if s.parameter == "velocidad_giro"]
        assert len(found) >= 1

    def test_rimpull_maximo(self, extractor):
        text = "Maximum rimpull: 950 kN at first gear"
        specs = extractor.extract_from_text(text, "CAT", "797F", "https://cat.com")
        found = [s for s in specs if s.parameter == "rimpull_maximo"]
        assert len(found) >= 1
        assert found[0].unit == "kN"

    def test_dimensiones(self, extractor):
        text = "Overall width: 9.1 m. Overall length: 15.6 m. Overall height: 7.7 m."
        specs = extractor.extract_from_text(text, "CAT", "797F", "https://cat.com")
        params = {s.parameter for s in specs}
        assert "ancho_total" in params
        assert "largo_total" in params
        assert "altura_total" in params

    def test_voltaje_sistema(self, extractor):
        text = "System voltage: 24 V"
        specs = extractor.extract_from_text(text, "CAT", "793F", "https://cat.com")
        found = [s for s in specs if s.parameter == "voltaje_sistema"]
        assert len(found) >= 1

    def test_caudal_hidraulico(self, extractor):
        text = "Hydraulic flow: 1200 L/min at main pump"
        specs = extractor.extract_from_text(text, "XCMG", "XE7000", "https://xcmg.com")
        found = [s for s in specs if s.parameter == "caudal_hidraulico"]
        assert len(found) >= 1

    def test_pendiente_maxima(self, extractor):
        text = "Maximum grade: 12 %"
        specs = extractor.extract_from_text(text, "CAT", "797F", "https://cat.com")
        found = [s for s in specs if s.parameter == "pendiente_maxima"]
        assert len(found) >= 1


class TestTableExtractionEnhanced:
    """Tests para extraccion de tablas mejorada."""

    def test_three_column_table(self, extractor):
        table = [
            ["Parameter", "Value", "Unit"],
            ["Operating Weight", "700", "ton"],
            ["Engine Power", "2500", "hp"],
        ]
        specs = extractor.extract_from_table(table, "XCMG", "XE7000", "https://xcmg.com")
        assert len(specs) == 2
        params = {s.parameter for s in specs}
        assert "peso_operativo" in params
        assert "potencia_motor" in params

    def test_two_column_table(self, extractor):
        table = [
            ["Operating weight", "700 ton"],
            ["Engine power", "2500 hp"],
        ]
        specs = extractor.extract_from_table(table, "XCMG", "XE7000", "https://xcmg.com")
        assert len(specs) == 2

    def test_header_detection(self, extractor):
        table = [
            ["Specification", "Value"],
            ["Payload", "400 ton"],
        ]
        specs = extractor.extract_from_table(table, "CAT", "797F", "https://cat.com")
        assert len(specs) == 1
        assert specs[0].parameter == "capacidad_carga"

    def test_rimpull_in_table(self, extractor):
        table = [
            ["Max rimpull", "950 kN"],
            ["Top speed", "65 km/h"],
        ]
        specs = extractor.extract_from_table(table, "CAT", "797F", "https://cat.com")
        params = {s.parameter for s in specs}
        assert "rimpull_maximo" in params
        assert "velocidad_maxima" in params

    def test_new_params_in_table(self, extractor):
        table = [
            ["Fuel tank capacity", "3500 L"],
            ["Ground pressure", "120 kPa"],
            ["Dump height", "7.2 m"],
            ["Swing speed", "4.5 rpm"],
        ]
        specs = extractor.extract_from_table(table, "XCMG", "XE7000", "https://xcmg.com")
        params = {s.parameter for s in specs}
        assert "capacidad_tanque" in params
        assert "presion_suelo" in params
        assert "altura_descarga" in params
        assert "velocidad_giro" in params


class TestNormalization:
    def test_mm_to_m_conversion(self):
        spec = TechnicalSpec("X", "M", "profundidad_excavacion", "8500", "mm", "", 0.8)
        normalized = normalize_spec(spec)
        assert normalized.value == "8.5"
        assert normalized.unit == "m"

    def test_kg_to_ton_for_new_params(self):
        spec = TechnicalSpec("X", "M", "peso_vacio", "500000", "kg", "", 0.8)
        normalized = normalize_spec(spec)
        assert normalized.value == "500.0"
        assert normalized.unit == "ton"


class TestValidRanges:
    def test_rimpull_in_range(self):
        spec = TechnicalSpec("CAT", "797F", "rimpull_maximo", "950", "kN", "", 0.8)
        validated = validate_spec(spec)
        assert validated.confidence == 0.8

    def test_rimpull_out_of_range(self):
        spec = TechnicalSpec("CAT", "797F", "rimpull_maximo", "5000", "kN", "", 0.8)
        validated = validate_spec(spec)
        assert validated.confidence == 0.3

    def test_capacidad_tanque_valid(self):
        spec = TechnicalSpec("CAT", "797F", "capacidad_tanque", "3500", "L", "", 0.9)
        validated = validate_spec(spec)
        assert validated.confidence == 0.9
