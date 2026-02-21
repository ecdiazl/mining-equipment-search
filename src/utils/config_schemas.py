"""
Esquemas Pydantic para validacion de archivos de configuracion.
"""

import re

from pydantic import BaseModel, field_validator, model_validator


class ScrapingConfig(BaseModel):
    max_concurrent_requests: int = 5
    request_delay_seconds: float = 2.0
    timeout_seconds: int = 30
    max_retries: int = 3
    user_agent: str = "MiningEquipResearch/1.0"
    respect_robots_txt: bool = True

    @field_validator("max_concurrent_requests")
    @classmethod
    def validate_max_concurrent(cls, v: int) -> int:
        if not 1 <= v <= 50:
            raise ValueError(f"max_concurrent_requests debe estar entre 1 y 50, got {v}")
        return v

    @field_validator("request_delay_seconds")
    @classmethod
    def validate_delay(cls, v: float) -> float:
        if v < 0:
            raise ValueError(f"request_delay_seconds debe ser >= 0, got {v}")
        return v

    @field_validator("timeout_seconds")
    @classmethod
    def validate_timeout(cls, v: int) -> int:
        if not 1 <= v <= 300:
            raise ValueError(f"timeout_seconds debe estar entre 1 y 300, got {v}")
        return v

    @field_validator("max_retries")
    @classmethod
    def validate_retries(cls, v: int) -> int:
        if not 0 <= v <= 10:
            raise ValueError(f"max_retries debe estar entre 0 y 10, got {v}")
        return v


class SearchEngineConfig(BaseModel):
    name: str
    enabled: bool = True
    max_results_per_query: int = 20


class SearchConfig(BaseModel):
    engines: list[SearchEngineConfig] = []
    languages: list[str] = ["en", "es"]
    query_templates: list[str] = []


class NLPConfig(BaseModel):
    embedding_model: str = "sentence-transformers/all-MiniLM-L6-v2"
    chunk_size: int = 512
    chunk_overlap: int = 50
    similarity_threshold: float = 0.75

    @field_validator("chunk_size")
    @classmethod
    def validate_chunk_size(cls, v: int) -> int:
        if v < 1:
            raise ValueError(f"chunk_size debe ser >= 1, got {v}")
        return v

    @field_validator("chunk_overlap")
    @classmethod
    def validate_chunk_overlap(cls, v: int) -> int:
        if v < 0:
            raise ValueError(f"chunk_overlap debe ser >= 0, got {v}")
        return v

    @field_validator("similarity_threshold")
    @classmethod
    def validate_similarity(cls, v: float) -> float:
        if not 0 <= v <= 1:
            raise ValueError(f"similarity_threshold debe estar entre 0 y 1, got {v}")
        return v

    @model_validator(mode="after")
    def validate_overlap_less_than_size(self) -> "NLPConfig":
        if self.chunk_overlap >= self.chunk_size:
            raise ValueError(
                f"chunk_overlap ({self.chunk_overlap}) debe ser menor que chunk_size ({self.chunk_size})"
            )
        return self


class StorageConfig(BaseModel):
    raw_data_dir: str = "data/raw"
    processed_data_dir: str = "data/processed"
    embeddings_dir: str = "data/embeddings"
    reports_dir: str = "data/reports"
    database: str = "data/mining_equipment.db"

    @field_validator("raw_data_dir", "processed_data_dir", "embeddings_dir", "reports_dir", "database")
    @classmethod
    def validate_no_traversal(cls, v: str) -> str:
        if ".." in v or v.startswith("/"):
            raise ValueError(f"Path no puede contener '..' ni ser absoluto: {v}")
        return v


class LoggingConfig(BaseModel):
    level: str = "INFO"
    file: str = "logs/pipeline.log"
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    @field_validator("level")
    @classmethod
    def validate_level(cls, v: str) -> str:
        valid = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        if v.upper() not in valid:
            raise ValueError(f"Nivel de log invalido: {v}. Opciones: {valid}")
        return v.upper()


class ProjectConfig(BaseModel):
    name: str = "Mining Equipment Technical Search"
    version: str = "1.0.0"
    description: str = ""


class Settings(BaseModel):
    """Esquema completo de settings.yaml."""
    project: ProjectConfig = ProjectConfig()
    scraping: ScrapingConfig = ScrapingConfig()
    search: SearchConfig = SearchConfig()
    nlp: NLPConfig = NLPConfig()
    storage: StorageConfig = StorageConfig()
    logging: LoggingConfig = LoggingConfig()


class EquipoConfig(BaseModel):
    tipo: str
    series: list[str] = []
    rango_peso_ton: list[float] = []
    rango_capacidad_ton: list[float] = []
    nota: str = ""


class BrandConfig(BaseModel):
    nombre_completo: str
    pais: str = ""
    sitio_web: str = ""
    ranking_global: str = ""
    equipos: dict[str, list[EquipoConfig]] = {}

    @field_validator("sitio_web")
    @classmethod
    def validate_sitio_web(cls, v: str) -> str:
        if v and not re.match(r"^https?://", v):
            raise ValueError(f"sitio_web debe ser una URL HTTP(S) o vacio, got: {v}")
        return v


def validate_settings(raw: dict) -> Settings:
    """Valida y retorna configuracion tipada. Lanza ValidationError si es invalida."""
    return Settings(**raw)


def validate_brand(key: str, raw: dict) -> BrandConfig:
    """Valida configuracion de una marca."""
    return BrandConfig(**raw)
