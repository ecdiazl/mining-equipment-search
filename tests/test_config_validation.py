"""Tests para validacion de configuracion con Pydantic."""

import pytest
from src.utils.config_schemas import validate_settings, Settings


class TestSettingsValidation:
    def test_valid_minimal_settings(self):
        settings = validate_settings({})
        assert settings.storage.database == "data/mining_equipment.db"
        assert settings.scraping.request_delay_seconds == 2.0

    def test_valid_full_settings(self):
        raw = {
            "project": {"name": "Test", "version": "2.0"},
            "scraping": {"request_delay_seconds": 3, "max_retries": 5},
            "storage": {"database": "data/test.db"},
            "logging": {"level": "DEBUG"},
        }
        settings = validate_settings(raw)
        assert settings.scraping.request_delay_seconds == 3
        assert settings.logging.level == "DEBUG"

    def test_invalid_log_level(self):
        with pytest.raises(Exception):
            validate_settings({"logging": {"level": "INVALID"}})

    def test_model_dump_returns_dict(self):
        settings = validate_settings({})
        d = settings.model_dump()
        assert isinstance(d, dict)
        assert "storage" in d
        assert d["storage"]["database"] == "data/mining_equipment.db"
