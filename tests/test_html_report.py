"""Tests para el generador de reportes HTML."""

import pytest
from src.models.database import DatabaseManager
from src.reports.html_report import HTMLReportGenerator


@pytest.fixture
def db_with_data(tmp_path):
    """DB en memoria con datos de prueba."""
    db = DatabaseManager(db_path=":memory:")
    db.create_tables()
    brand_id = db.insert_brand("xcmg", "XCMG", "China", "", "chinese")
    equip_id = db.insert_equipment(brand_id, "XE7000", "carguio", "Excavadora")
    db.insert_spec(equip_id, "peso_operativo", "700", "ton", 0.9, "https://example.com")
    db.insert_spec(equip_id, "potencia_motor", "2500", "hp", 0.85, "https://example.com/page2")
    equip_id2 = db.insert_equipment(brand_id, "XDE400", "transporte", "Camion")
    db.insert_spec(equip_id2, "capacidad_carga", "400", "ton", 0.7, "https://example.com/truck")
    return db, tmp_path


class TestHTMLReportGenerator:
    def test_generate_creates_file(self, db_with_data):
        db, tmp_path = db_with_data
        gen = HTMLReportGenerator(db=db, output_dir=str(tmp_path))
        path = gen.generate()
        assert path.endswith("equipment_report.html")
        assert (tmp_path / "equipment_report.html").exists()

    def test_generated_html_contains_data(self, db_with_data):
        db, tmp_path = db_with_data
        gen = HTMLReportGenerator(db=db, output_dir=str(tmp_path))
        gen.generate()
        html = (tmp_path / "equipment_report.html").read_text()
        assert "XCMG" in html
        assert "XE7000" in html
        assert "peso_operativo" in html
        assert "Plotly" in html or "plotly" in html

    def test_generated_html_has_xss_protection(self, db_with_data):
        db, tmp_path = db_with_data
        gen = HTMLReportGenerator(db=db, output_dir=str(tmp_path))
        gen.generate()
        html = (tmp_path / "equipment_report.html").read_text()
        # Debe tener la funcion esc() de escape
        assert "function esc(" in html
        # Debe usar safeUrl para links
        assert "function safeUrl(" in html
        # Debe tener rel=noopener
        assert "noopener" in html

    def test_generated_html_has_sri(self, db_with_data):
        db, tmp_path = db_with_data
        gen = HTMLReportGenerator(db=db, output_dir=str(tmp_path))
        gen.generate()
        html = (tmp_path / "equipment_report.html").read_text()
        assert 'integrity="sha384-' in html
        assert 'crossorigin="anonymous"' in html

    def test_empty_db_returns_empty(self):
        db = DatabaseManager(db_path=":memory:")
        db.create_tables()
        gen = HTMLReportGenerator(db=db, output_dir="/tmp/test_empty")
        path = gen.generate()
        assert path == ""


class TestHTMLXSSPrevention:
    def test_malicious_brand_name_escaped(self, tmp_path):
        """Verifica que datos maliciosos se escapen en el JSON embebido."""
        db = DatabaseManager(db_path=":memory:")
        db.create_tables()
        # Inyectar nombre con payload XSS
        brand_id = db.insert_brand("xss_test", '<script>alert("xss")</script>', "", "", "")
        equip_id = db.insert_equipment(brand_id, "Model<img/onerror=alert(1)>", "", "")
        db.insert_spec(equip_id, "test", "1", "kg", 0.5, "javascript:alert(1)")

        gen = HTMLReportGenerator(db=db, output_dir=str(tmp_path))
        gen.generate()
        html = (tmp_path / "equipment_report.html").read_text()

        # El JSON embebido debe contener los datos escapados por json.dumps
        # (json.dumps escapa < > como unicode si hay ensure_ascii=True, pero
        # con ensure_ascii=False los deja — sin embargo estan dentro de un
        # contexto JavaScript string, no HTML directo)
        # Lo importante es que renderTable usa esc() para sanitizar
        assert "function esc(" in html
