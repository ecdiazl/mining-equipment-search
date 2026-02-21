"""Tests para DatabaseManager — session management, CRUD, y metodos nuevos."""

import pytest
from src.models.database import DatabaseManager, Brand, Equipment, TechnicalSpecRecord, DataSource


@pytest.fixture
def db():
    """DB en memoria para tests."""
    manager = DatabaseManager(db_path=":memory:")
    manager.create_tables()
    return manager


class TestSessionManagement:
    def test_session_scope_commits(self, db):
        with db.session_scope() as session:
            session.add(Brand(key="test", nombre_completo="Test Brand"))
        # Verificar que se persitio
        with db.session_scope() as session:
            assert session.query(Brand).count() == 1

    def test_session_scope_rollback_on_error(self, db):
        try:
            with db.session_scope() as session:
                session.add(Brand(key="test", nombre_completo="Test"))
                raise ValueError("Error forzado")
        except ValueError:
            pass
        with db.session_scope() as session:
            assert session.query(Brand).count() == 0


class TestInsertMethods:
    def test_insert_brand_returns_id(self, db):
        brand_id = db.insert_brand("komatsu", "Komatsu Ltd.", "Japon", "https://komatsu.com", "tier_1")
        assert isinstance(brand_id, int)
        assert brand_id > 0

    def test_insert_brand_idempotent(self, db):
        id1 = db.insert_brand("komatsu", "Komatsu Ltd.", "Japon", "", "tier_1")
        id2 = db.insert_brand("komatsu", "Komatsu Ltd.", "Japon", "", "tier_1")
        assert id1 == id2

    def test_insert_equipment_returns_id(self, db):
        brand_id = db.insert_brand("xcmg", "XCMG", "China", "", "chinese")
        equip_id = db.insert_equipment(brand_id, "XE7000", "carguio", "Excavadora")
        assert isinstance(equip_id, int)
        assert equip_id > 0

    def test_insert_equipment_idempotent(self, db):
        brand_id = db.insert_brand("xcmg", "XCMG", "China", "", "chinese")
        id1 = db.insert_equipment(brand_id, "XE7000", "carguio", "Excavadora")
        id2 = db.insert_equipment(brand_id, "XE7000", "carguio", "Excavadora")
        assert id1 == id2

    def test_insert_spec(self, db):
        brand_id = db.insert_brand("xcmg", "XCMG", "China", "", "chinese")
        equip_id = db.insert_equipment(brand_id, "XE7000", "carguio", "Excavadora")
        db.insert_spec(equip_id, "peso_operativo", "700", "ton", 0.9, "https://example.com")
        with db.session_scope() as session:
            assert session.query(TechnicalSpecRecord).count() == 1


class TestGetVisitedUrls:
    def test_empty_brand(self, db):
        db.insert_brand("xcmg", "XCMG", "China", "", "chinese")
        urls = db.get_visited_urls_for_brand("xcmg")
        assert urls == set()

    def test_with_sources(self, db):
        brand_id = db.insert_brand("xcmg", "XCMG", "China", "", "chinese")
        equip_id = db.insert_equipment(brand_id, "XE7000", "carguio", "Excavadora")
        with db.session_scope() as session:
            session.add(DataSource(equipment_id=equip_id, url="https://a.com"))
            session.add(DataSource(equipment_id=equip_id, url="https://b.com"))
        urls = db.get_visited_urls_for_brand("xcmg")
        assert urls == {"https://a.com", "https://b.com"}


class TestClearBrandData:
    def test_clear_removes_specs_sources_equipment(self, db):
        brand_id = db.insert_brand("xcmg", "XCMG", "China", "", "chinese")
        equip_id = db.insert_equipment(brand_id, "XE7000", "carguio", "Excavadora")
        db.insert_spec(equip_id, "peso_operativo", "700", "ton", 0.9, "https://example.com")
        with db.session_scope() as session:
            session.add(DataSource(equipment_id=equip_id, url="https://a.com"))

        counts = db.clear_brand_data("xcmg")
        assert counts["specs"] == 1
        assert counts["sources"] == 1
        assert counts["equipment"] == 1

        # Brand debe seguir existiendo
        with db.session_scope() as session:
            assert session.query(Brand).filter_by(key="xcmg").count() == 1
            assert session.query(Equipment).count() == 0

    def test_clear_nonexistent_brand(self, db):
        counts = db.clear_brand_data("nonexistent")
        assert counts == {"specs": 0, "sources": 0, "rimpull_points": 0, "equipment": 0}


class TestGetBrandStatus:
    def test_status_single_brand(self, db):
        brand_id = db.insert_brand("xcmg", "XCMG", "China", "", "chinese")
        equip_id = db.insert_equipment(brand_id, "XE7000", "carguio", "Excavadora")
        db.insert_spec(equip_id, "peso_operativo", "700", "ton", 0.9, "https://example.com")
        db.insert_spec(equip_id, "potencia_motor", "2500", "hp", 0.8, "https://example.com")

        status = db.get_brand_status("xcmg")
        assert status["brand_key"] == "xcmg"
        assert status["total_models"] == 1
        assert status["total_specs"] == 2

    def test_status_all_brands(self, db):
        db.insert_brand("xcmg", "XCMG", "China", "", "chinese")
        db.insert_brand("sany", "SANY", "China", "", "chinese")
        status = db.get_brand_status()
        assert status["total_brands"] == 2
        assert len(status["brands"]) == 2

    def test_status_nonexistent(self, db):
        status = db.get_brand_status("nonexistent")
        assert "error" in status


class TestCascadeDelete:
    def test_fk_cascade_on_equipment_delete(self, db):
        """Verificar que borrar equipment borra specs y sources en cascada."""
        brand_id = db.insert_brand("xcmg", "XCMG", "China", "", "chinese")
        equip_id = db.insert_equipment(brand_id, "XE7000", "carguio", "Excavadora")
        db.insert_spec(equip_id, "peso_operativo", "700", "ton", 0.9, "https://a.com")
        with db.session_scope() as session:
            session.add(DataSource(equipment_id=equip_id, url="https://b.com"))

        # Borrar via ORM (cascade deberia propagarse)
        with db.session_scope() as session:
            equip = session.get(Equipment, equip_id)
            session.delete(equip)

        with db.session_scope() as session:
            assert session.query(TechnicalSpecRecord).count() == 0
            assert session.query(DataSource).count() == 0
