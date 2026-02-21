"""
Utilidad para cargar configuracion YAML y variables de entorno.
"""

import os
import logging
import stat
from pathlib import Path

import yaml
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).parent.parent.parent
CONFIG_DIR = PROJECT_ROOT / "config"


def load_env():
    """Carga variables de entorno desde .env"""
    env_path = PROJECT_ROOT / ".env"
    if env_path.exists():
        # Warn if .env is world-readable
        mode = env_path.stat().st_mode
        if mode & stat.S_IROTH:
            logger.warning(
                f".env es world-readable (permisos {oct(mode)}). "
                "Considere: chmod 600 .env"
            )
        load_dotenv(env_path)
        logger.info("Variables de entorno cargadas desde .env")
    else:
        logger.warning(f"Archivo .env no encontrado en {env_path}")


def load_yaml(filename: str) -> dict:
    """Carga un archivo YAML de configuracion."""
    filepath = (CONFIG_DIR / filename).resolve()
    if not filepath.is_relative_to(CONFIG_DIR.resolve()):
        raise ValueError(f"Path traversal detectado: {filename}")
    if not filepath.exists():
        raise FileNotFoundError(f"Archivo de configuracion no encontrado: {filepath}")

    with open(filepath, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    if not isinstance(data, dict):
        raise ValueError(f"YAML debe ser un dict, no {type(data).__name__}: {filename}")

    logger.info(f"Configuracion cargada: {filename}")
    return data


def load_brands_config() -> dict:
    """Carga la configuracion de marcas y equipos."""
    return load_yaml("brands.yaml")


def load_settings() -> dict:
    """Carga la configuracion general del proyecto (con validacion Pydantic)."""
    raw = load_yaml("settings.yaml")
    try:
        from src.utils.config_schemas import validate_settings
        validated = validate_settings(raw)
        logger.info("Configuracion de settings validada correctamente")
        # Retornar dict para compatibilidad con codigo existente
        return validated.model_dump()
    except ImportError as e:
        logger.warning(f"Validacion de settings fallo, usando raw: {e}")
        return raw


def get_all_brands_flat(brands_config: dict) -> list[dict]:
    """
    Retorna lista plana de todas las marcas con sus equipos.

    Returns:
        Lista de dicts con keys: key, nombre, pais, tier, equipos
    """
    brands = []
    tier_keys = ["tier_1", "tier_2", "chinese_brands"]

    for tier_key in tier_keys:
        tier_data = brands_config.get(tier_key, {})
        for brand_key, brand_info in tier_data.items():
            brands.append({
                "key": brand_key,
                "nombre": brand_info.get("nombre_completo", brand_key),
                "pais": brand_info.get("pais", ""),
                "sitio_web": brand_info.get("sitio_web", ""),
                "tier": tier_key,
                "equipos": brand_info.get("equipos", {}),
            })

    return brands


def get_all_models_for_brand(brand_info: dict) -> list[dict]:
    """
    Retorna todos los modelos/series para una marca.

    Returns:
        Lista de dicts con keys: category, equipment_type, model
    """
    models = []
    for category in ["carguio", "transporte"]:
        equipos = brand_info.get("equipos", {}).get(category, [])
        for equipo in equipos:
            for model in equipo.get("series", []):
                models.append({
                    "category": category,
                    "equipment_type": equipo.get("tipo", ""),
                    "model": model,
                })
    return models
