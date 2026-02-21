"""Tests para extraccion, QA y cross-validation de curvas rimpull."""

import pytest

from src.parsers.rimpull_extractor import (
    RimpullCurveExtractor,
    RimpullCurve,
    RimpullPoint,
    normalize_gear,
    is_rimpull_table,
    sort_points_by_gear,
    _convert_to_kn,
    GEAR_ORDER,
)
from src.parsers.qa_pipeline import qa_rimpull_curve
from src.parsers.cross_validator import cross_validate_rimpull_curves
from src.models.database import DatabaseManager, RimpullCurvePoint, Equipment, Brand


# ============================================================
# Gear normalization
# ============================================================

class TestGearNormalization:
    def test_standard_labels(self):
        assert normalize_gear("1st") == "1st"
        assert normalize_gear("2nd") == "2nd"
        assert normalize_gear("3rd") == "3rd"

    def test_verbose_labels(self):
        assert normalize_gear("1st gear") == "1st"
        assert normalize_gear("gear 1") == "1st"
        assert normalize_gear("first") == "1st"

    def test_numeric_only(self):
        assert normalize_gear("1") == "1st"
        assert normalize_gear("2") == "2nd"
        assert normalize_gear("7") == "7th"

    def test_special_gears(self):
        assert normalize_gear("D") == "Direct"
        assert normalize_gear("direct drive") == "Direct"
        assert normalize_gear("R") == "Reverse"
        assert normalize_gear("reverse") == "Reverse"

    def test_case_insensitive(self):
        assert normalize_gear("LOW") == "1st"
        assert normalize_gear("REVERSE") == "Reverse"

    def test_trailing_dot(self):
        assert normalize_gear("1st.") == "1st"

    def test_unknown_label_passthrough(self):
        assert normalize_gear("unknown") == "unknown"


# ============================================================
# Unit conversion
# ============================================================

class TestUnitConversion:
    def test_kn_passthrough(self):
        assert _convert_to_kn(950.0, "kN") == 950.0

    def test_lbf_to_kn(self):
        result = _convert_to_kn(224809, "lbf")
        assert abs(result - 1000.0) < 1.0  # ~1000 kN

    def test_kgf_to_kn(self):
        result = _convert_to_kn(101972, "kgf")
        assert abs(result - 1000.0) < 1.0  # ~1000 kN

    def test_lb_alias(self):
        result = _convert_to_kn(224809, "lb")
        assert abs(result - 1000.0) < 1.0


# ============================================================
# Rimpull table detection
# ============================================================

class TestIsRimpullTable:
    def test_positive_with_gear_and_rimpull_header(self):
        table = [
            ["Gear", "Speed (km/h)", "Rimpull (kN)"],
            ["1st", "11.3", "950"],
            ["2nd", "18.5", "580"],
        ]
        assert is_rimpull_table(table) is True

    def test_positive_with_force_header(self):
        table = [
            ["Gear", "Tractive Force (kN)"],
            ["1st", "850"],
            ["2nd", "650"],
        ]
        assert is_rimpull_table(table) is True

    def test_negative_weight_table(self):
        table = [
            ["Parameter", "Value", "Unit"],
            ["Operating Weight", "700", "ton"],
            ["Engine Power", "2500", "hp"],
        ]
        assert is_rimpull_table(table) is False

    def test_negative_empty_table(self):
        assert is_rimpull_table([]) is False
        assert is_rimpull_table([["header"]]) is False

    def test_positive_gear_values_without_keyword_header(self):
        """Detect by gear-like values in first column + numeric in others."""
        table = [
            ["", "kN"],
            ["1st", "950"],
            ["2nd", "580"],
            ["3rd", "420"],
        ]
        assert is_rimpull_table(table) is True


# ============================================================
# Table extraction
# ============================================================

class TestTableExtraction:
    def setup_method(self):
        self.extractor = RimpullCurveExtractor()

    def test_extract_3col_table(self):
        table = [
            ["Gear", "Speed (km/h)", "Rimpull (kN)"],
            ["1st", "11.3", "950"],
            ["2nd", "18.5", "580"],
            ["3rd", "28.0", "420"],
        ]
        curve = self.extractor.extract_from_table(table, "CAT", "797F", "https://cat.com")
        assert curve is not None
        assert len(curve.points) == 3
        assert curve.points[0].gear == "1st"
        assert curve.points[0].force_kn == 950.0
        assert curve.points[0].speed_kmh == 11.3

    def test_extract_2col_table(self):
        table = [
            ["Gear", "Rimpull (kN)"],
            ["1st", "950"],
            ["2nd", "580"],
        ]
        curve = self.extractor.extract_from_table(table, "CAT", "797F", "https://cat.com")
        assert curve is not None
        assert len(curve.points) == 2
        assert curve.points[0].speed_kmh is None

    def test_extract_lbf_table_converts_to_kn(self):
        table = [
            ["Gear", "Rimpull (lbf)"],
            ["1st", "224809"],
            ["2nd", "112405"],
        ]
        curve = self.extractor.extract_from_table(table, "CAT", "793F", "https://cat.com")
        assert curve is not None
        assert abs(curve.points[0].force_kn - 1000.0) < 1.0
        assert curve.points[0].original_unit == "lbf"

    def test_returns_none_for_non_rimpull_table(self):
        table = [
            ["Parameter", "Value"],
            ["Weight", "700"],
            ["Power", "2500"],
        ]
        result = self.extractor.extract_from_table(table, "CAT", "797F", "")
        assert result is None

    def test_returns_none_for_single_point(self):
        table = [
            ["Gear", "Rimpull (kN)"],
            ["1st", "950"],
        ]
        result = self.extractor.extract_from_table(table, "CAT", "797F", "")
        assert result is None

    def test_sorted_by_gear_order(self):
        table = [
            ["Gear", "Rimpull (kN)"],
            ["3rd", "420"],
            ["1st", "950"],
            ["2nd", "580"],
        ]
        curve = self.extractor.extract_from_table(table, "CAT", "797F", "")
        assert [p.gear for p in curve.points] == ["1st", "2nd", "3rd"]

    def test_gear_label_normalization_in_table(self):
        table = [
            ["Gear", "Rimpull (kN)"],
            ["first gear", "950"],
            ["second gear", "580"],
        ]
        curve = self.extractor.extract_from_table(table, "CAT", "797F", "")
        assert curve is not None
        assert curve.points[0].gear == "1st"
        assert curve.points[1].gear == "2nd"


# ============================================================
# Text extraction
# ============================================================

class TestTextExtraction:
    def setup_method(self):
        self.extractor = RimpullCurveExtractor()

    def test_gear_rimpull_pattern(self):
        text = "1st gear rimpull: 950 kN, 2nd gear rimpull: 580 kN, 3rd gear rimpull: 420 kN"
        curves = self.extractor.extract_from_text(text, "CAT", "797F", "https://cat.com")
        assert len(curves) == 1
        assert len(curves[0].points) == 3
        assert curves[0].points[0].gear == "1st"
        assert curves[0].points[0].force_kn == 950.0

    def test_rimpull_parenthetical_pattern(self):
        text = "Rimpull (1st): 950 kN. Rimpull (2nd): 580 kN"
        curves = self.extractor.extract_from_text(text, "CAT", "797F", "")
        assert len(curves) == 1
        assert len(curves[0].points) == 2

    def test_returns_empty_for_single_point(self):
        text = "1st gear rimpull: 950 kN"
        curves = self.extractor.extract_from_text(text, "CAT", "797F", "")
        assert curves == []

    def test_returns_empty_for_no_match(self):
        text = "The truck has great performance and powerful engine."
        curves = self.extractor.extract_from_text(text, "CAT", "797F", "")
        assert curves == []

    def test_inline_list_pattern(self):
        text = "Rimpull values: 1st: 950 kN, 2nd: 580 kN, 3rd: 420 kN"
        curves = self.extractor.extract_from_text(text, "CAT", "797F", "")
        assert len(curves) == 1
        assert len(curves[0].points) == 3


# ============================================================
# QA for rimpull curves
# ============================================================

class TestQARimpullCurve:
    def test_valid_curve_passes(self):
        curve = RimpullCurve(
            brand="CAT", model="797F",
            points=[
                RimpullPoint("1st", 11.3, 950.0, "kN", 0.9, ""),
                RimpullPoint("2nd", 18.5, 580.0, "kN", 0.9, ""),
                RimpullPoint("3rd", 28.0, 420.0, "kN", 0.9, ""),
            ],
        )
        result, report = qa_rimpull_curve(curve)
        assert result is not None
        assert report["passed"] is True
        assert len(result.points) == 3

    def test_rejects_too_few_points(self):
        curve = RimpullCurve(
            brand="CAT", model="797F",
            points=[
                RimpullPoint("1st", 11.3, 950.0, "kN", 0.9, ""),
            ],
        )
        result, report = qa_rimpull_curve(curve)
        assert result is None
        assert report["passed"] is False

    def test_rejects_zero_force(self):
        curve = RimpullCurve(
            brand="CAT", model="797F",
            points=[
                RimpullPoint("1st", None, 0, "kN", 0.9, ""),
                RimpullPoint("2nd", None, 580.0, "kN", 0.9, ""),
                RimpullPoint("3rd", None, 420.0, "kN", 0.9, ""),
            ],
        )
        result, report = qa_rimpull_curve(curve)
        assert result is not None  # Still passes with 2 valid points
        assert len(result.points) == 2

    def test_rejects_out_of_range_force(self):
        curve = RimpullCurve(
            brand="CAT", model="797F",
            points=[
                RimpullPoint("1st", None, 30.0, "kN", 0.9, ""),   # Too low
                RimpullPoint("2nd", None, 580.0, "kN", 0.9, ""),
                RimpullPoint("3rd", None, 420.0, "kN", 0.9, ""),
            ],
        )
        result, report = qa_rimpull_curve(curve)
        assert result is not None
        assert len(result.points) == 2  # Only 2nd and 3rd pass

    def test_rejects_out_of_range_speed(self):
        curve = RimpullCurve(
            brand="CAT", model="797F",
            points=[
                RimpullPoint("1st", 90.0, 950.0, "kN", 0.9, ""),   # Speed too high
                RimpullPoint("2nd", 18.5, 580.0, "kN", 0.9, ""),
                RimpullPoint("3rd", 28.0, 420.0, "kN", 0.9, ""),
            ],
        )
        result, report = qa_rimpull_curve(curve)
        assert result is not None
        assert len(result.points) == 2

    def test_monotonicity_warning(self):
        curve = RimpullCurve(
            brand="CAT", model="797F",
            points=[
                RimpullPoint("1st", None, 500.0, "kN", 0.9, ""),
                RimpullPoint("2nd", None, 700.0, "kN", 0.9, ""),  # Increasing = wrong
                RimpullPoint("3rd", None, 420.0, "kN", 0.9, ""),
            ],
        )
        result, report = qa_rimpull_curve(curve)
        assert result is not None  # Still passes (monotonicity is warning)
        assert any("monotonicity" in issue for issue in report["issues"])

    def test_all_points_invalid_means_rejection(self):
        curve = RimpullCurve(
            brand="CAT", model="797F",
            points=[
                RimpullPoint("1st", None, 10.0, "kN", 0.9, ""),  # Too low
                RimpullPoint("2nd", None, 5.0, "kN", 0.9, ""),   # Too low
            ],
        )
        result, report = qa_rimpull_curve(curve)
        assert result is None
        assert report["passed"] is False


# ============================================================
# Cross-validation of rimpull curves
# ============================================================

class TestCrossValidateRimpullCurves:
    def test_single_curve_passthrough(self):
        curve = RimpullCurve(
            brand="CAT", model="797F",
            points=[
                RimpullPoint("1st", None, 950.0, "kN", 0.9, "src1"),
                RimpullPoint("2nd", None, 580.0, "kN", 0.9, "src1"),
            ],
        )
        result = cross_validate_rimpull_curves([curve])
        assert result is not None
        assert len(result.points) == 2

    def test_merge_two_agreeing_sources(self):
        curve1 = RimpullCurve(
            brand="CAT", model="797F",
            points=[
                RimpullPoint("1st", None, 950.0, "kN", 0.85, "src1"),
                RimpullPoint("2nd", None, 580.0, "kN", 0.85, "src1"),
            ],
        )
        curve2 = RimpullCurve(
            brand="CAT", model="797F",
            points=[
                RimpullPoint("1st", None, 955.0, "kN", 0.9, "src2"),  # Within 10%
                RimpullPoint("2nd", None, 575.0, "kN", 0.9, "src2"),
            ],
        )
        result = cross_validate_rimpull_curves([curve1, curve2])
        assert result is not None
        assert len(result.points) == 2
        # Best confidence should get consensus bonus
        assert result.points[0].confidence > 0.9

    def test_merge_with_conflicting_outlier(self):
        curve1 = RimpullCurve(
            brand="CAT", model="797F",
            points=[
                RimpullPoint("1st", None, 950.0, "kN", 0.9, "src1"),
                RimpullPoint("2nd", None, 580.0, "kN", 0.9, "src1"),
            ],
        )
        curve2 = RimpullCurve(
            brand="CAT", model="797F",
            points=[
                RimpullPoint("1st", None, 500.0, "kN", 0.7, "src2"),  # Outlier (>10% diff)
                RimpullPoint("2nd", None, 575.0, "kN", 0.85, "src2"),
            ],
        )
        result = cross_validate_rimpull_curves([curve1, curve2])
        assert result is not None
        # 1st gear: conflicting, confidence capped at 0.85
        gear1 = [p for p in result.points if p.gear == "1st"][0]
        assert gear1.confidence <= 0.85

    def test_empty_input(self):
        assert cross_validate_rimpull_curves([]) is None

    def test_speed_averaging(self):
        curve1 = RimpullCurve(
            brand="CAT", model="797F",
            points=[
                RimpullPoint("1st", 10.0, 950.0, "kN", 0.9, "src1"),
                RimpullPoint("2nd", 18.0, 580.0, "kN", 0.9, "src1"),
            ],
        )
        curve2 = RimpullCurve(
            brand="CAT", model="797F",
            points=[
                RimpullPoint("1st", 12.0, 955.0, "kN", 0.9, "src2"),
                RimpullPoint("2nd", 19.0, 575.0, "kN", 0.9, "src2"),
            ],
        )
        result = cross_validate_rimpull_curves([curve1, curve2])
        gear1 = [p for p in result.points if p.gear == "1st"][0]
        assert gear1.speed_kmh == 11.0  # avg of 10 and 12


# ============================================================
# Database integration
# ============================================================

class TestRimpullDatabase:
    @pytest.fixture
    def db(self):
        manager = DatabaseManager(db_path=":memory:")
        manager.create_tables()
        return manager

    def test_insert_and_query_rimpull_point(self, db):
        brand_id = db.insert_brand("cat", "Caterpillar", "USA", "", "tier_1")
        equip_id = db.insert_equipment(brand_id, "797F", "transporte", "Camion Minero")
        db.insert_rimpull_point(equip_id, "1st", 11.3, 950.0, "kN", 0.9, "https://cat.com")
        db.insert_rimpull_point(equip_id, "2nd", 18.5, 580.0, "kN", 0.85, "https://cat.com")

        with db.session_scope() as session:
            points = session.query(RimpullCurvePoint).filter_by(equipment_id=equip_id).all()
            assert len(points) == 2
            assert points[0].gear == "1st"
            assert points[0].force_kn == 950.0

    def test_get_rimpull_curves_dataframe(self, db):
        brand_id = db.insert_brand("cat", "Caterpillar", "USA", "", "tier_1")
        equip_id = db.insert_equipment(brand_id, "797F", "transporte", "Camion Minero")
        db.insert_rimpull_point(equip_id, "1st", 11.3, 950.0, "kN", 0.9, "")
        db.insert_rimpull_point(equip_id, "2nd", 18.5, 580.0, "kN", 0.85, "")

        df = db.get_rimpull_curves_dataframe()
        assert len(df) == 2
        assert "brand" in df.columns
        assert "model" in df.columns
        assert "gear" in df.columns
        assert "force_kn" in df.columns

    def test_clear_brand_data_includes_rimpull(self, db):
        brand_id = db.insert_brand("cat", "Caterpillar", "USA", "", "tier_1")
        equip_id = db.insert_equipment(brand_id, "797F", "transporte", "Camion Minero")
        db.insert_rimpull_point(equip_id, "1st", 11.3, 950.0, "kN", 0.9, "")
        db.insert_spec(equip_id, "peso_operativo", "700", "ton", 0.9, "")

        counts = db.clear_brand_data("cat")
        assert counts["rimpull_points"] == 1
        assert counts["specs"] == 1
        assert counts["equipment"] == 1

        with db.session_scope() as session:
            assert session.query(RimpullCurvePoint).count() == 0

    def test_cascade_delete_rimpull_points(self, db):
        brand_id = db.insert_brand("cat", "Caterpillar", "USA", "", "tier_1")
        equip_id = db.insert_equipment(brand_id, "797F", "transporte", "Camion Minero")
        db.insert_rimpull_point(equip_id, "1st", 11.3, 950.0, "kN", 0.9, "")

        # Delete equipment via ORM cascade
        with db.session_scope() as session:
            equip = session.get(Equipment, equip_id)
            session.delete(equip)

        with db.session_scope() as session:
            assert session.query(RimpullCurvePoint).count() == 0

    def test_empty_rimpull_dataframe(self, db):
        df = db.get_rimpull_curves_dataframe()
        assert len(df) == 0
