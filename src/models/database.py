"""
Modelo de base de datos SQLite para almacenar equipos y especificaciones tecnicas.
"""

import logging
import warnings
from contextlib import contextmanager
from pathlib import Path
from datetime import datetime, timezone

from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Text, ForeignKey, Index, func, select
from sqlalchemy.orm import declarative_base, sessionmaker, relationship

logger = logging.getLogger(__name__)

Base = declarative_base()


def _utcnow():
    return datetime.now(timezone.utc)


class Brand(Base):
    __tablename__ = "brands"

    id = Column(Integer, primary_key=True, autoincrement=True)
    key = Column(String(50), unique=True, nullable=False)
    nombre_completo = Column(String(200), nullable=False)
    pais = Column(String(100))
    sitio_web = Column(String(500))
    tier = Column(String(50))
    created_at = Column(DateTime, default=_utcnow)

    equipos = relationship(
        "Equipment", back_populates="brand", cascade="all, delete-orphan"
    )


class Equipment(Base):
    __tablename__ = "equipment"
    __table_args__ = (Index("ix_equipment_brand_id", "brand_id"),)

    id = Column(Integer, primary_key=True, autoincrement=True)
    brand_id = Column(Integer, ForeignKey("brands.id", ondelete="CASCADE"), nullable=False)
    model = Column(String(100), nullable=False)
    category = Column(String(50))  # carguio / transporte
    equipment_type = Column(String(100))  # Pala Hidraulica, Camion Minero, etc.
    series = Column(String(100))
    created_at = Column(DateTime, default=_utcnow)
    updated_at = Column(DateTime, default=_utcnow, onupdate=_utcnow)

    brand = relationship("Brand", back_populates="equipos")
    specs = relationship(
        "TechnicalSpecRecord", back_populates="equipment", cascade="all, delete-orphan"
    )
    sources = relationship(
        "DataSource", back_populates="equipment", cascade="all, delete-orphan"
    )
    rimpull_points = relationship(
        "RimpullCurvePoint", back_populates="equipment", cascade="all, delete-orphan"
    )


class TechnicalSpecRecord(Base):
    __tablename__ = "technical_specs"
    __table_args__ = (Index("ix_technical_specs_equipment_id", "equipment_id"),)

    id = Column(Integer, primary_key=True, autoincrement=True)
    equipment_id = Column(Integer, ForeignKey("equipment.id", ondelete="CASCADE"), nullable=False)
    parameter = Column(String(100), nullable=False)
    value = Column(String(200))
    unit = Column(String(50))
    confidence = Column(Float, default=0.0)
    source_url = Column(Text)
    extracted_at = Column(DateTime, default=_utcnow)

    equipment = relationship("Equipment", back_populates="specs")


class DataSource(Base):
    __tablename__ = "data_sources"
    __table_args__ = (Index("ix_data_sources_equipment_id", "equipment_id"),)

    id = Column(Integer, primary_key=True, autoincrement=True)
    equipment_id = Column(Integer, ForeignKey("equipment.id", ondelete="CASCADE"), nullable=False)
    url = Column(Text, nullable=False)
    title = Column(Text)
    source_type = Column(String(50))  # web, pdf, brochure, manual
    content_hash = Column(String(64))
    scraped_at = Column(DateTime, default=_utcnow)
    content_length = Column(Integer)

    equipment = relationship("Equipment", back_populates="sources")


class RimpullCurvePoint(Base):
    __tablename__ = "rimpull_curve_points"
    __table_args__ = (Index("ix_rimpull_curve_points_equipment_id", "equipment_id"),)

    id = Column(Integer, primary_key=True, autoincrement=True)
    equipment_id = Column(Integer, ForeignKey("equipment.id", ondelete="CASCADE"), nullable=False)
    gear = Column(String(50), nullable=False)
    speed_kmh = Column(Float)
    force_kn = Column(Float, nullable=False)
    original_unit = Column(String(20))
    confidence = Column(Float, default=0.0)
    source_url = Column(Text)
    extracted_at = Column(DateTime, default=_utcnow)

    equipment = relationship("Equipment", back_populates="rimpull_points")


class SearchLog(Base):
    __tablename__ = "search_log"

    id = Column(Integer, primary_key=True, autoincrement=True)
    query = Column(Text, nullable=False)
    engine = Column(String(50))
    results_count = Column(Integer)
    executed_at = Column(DateTime, default=_utcnow)


class DatabaseManager:
    """Gestiona la conexion y operaciones con la base de datos."""

    def __init__(self, db_path: str = "data/mining_equipment.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.engine = create_engine(
            f"sqlite:///{self.db_path}",
            echo=False,
            connect_args={"timeout": 30},
            pool_pre_ping=True,
        )
        self.Session = sessionmaker(bind=self.engine)

    def create_tables(self):
        """Crea todas las tablas si no existen."""
        Base.metadata.create_all(self.engine)
        logger.info(f"Base de datos inicializada en {self.db_path}")

    @contextmanager
    def session_scope(self):
        """Context manager para sesiones con commit/rollback automatico."""
        session = self.Session()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def get_session(self):
        """Retorna una sesion. Preferir session_scope() para manejo seguro."""
        warnings.warn(
            "get_session() is deprecated, use session_scope() context manager instead",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.Session()

    def insert_brand(self, key: str, nombre: str, pais: str, sitio_web: str, tier: str) -> int:
        """Inserta o retorna brand existente. Retorna el ID."""
        with self.session_scope() as session:
            existing = session.query(Brand).filter_by(key=key).first()
            if existing:
                return existing.id

            brand = Brand(
                key=key, nombre_completo=nombre, pais=pais, sitio_web=sitio_web, tier=tier
            )
            session.add(brand)
            session.flush()
            return brand.id

    def insert_equipment(
        self, brand_id: int, model: str, category: str, equipment_type: str
    ) -> int:
        """Inserta o retorna equipment existente. Retorna el ID."""
        with self.session_scope() as session:
            existing = session.query(Equipment).filter_by(
                brand_id=brand_id, model=model
            ).first()
            if existing:
                return existing.id

            equip = Equipment(
                brand_id=brand_id, model=model, category=category, equipment_type=equipment_type
            )
            session.add(equip)
            session.flush()
            return equip.id

    def insert_spec(
        self,
        equipment_id: int,
        parameter: str,
        value: str,
        unit: str,
        confidence: float,
        source_url: str,
    ):
        with self.session_scope() as session:
            spec = TechnicalSpecRecord(
                equipment_id=equipment_id,
                parameter=parameter,
                value=value,
                unit=unit,
                confidence=confidence,
                source_url=source_url,
            )
            session.add(spec)

    def insert_rimpull_point(
        self,
        equipment_id: int,
        gear: str,
        speed_kmh: float | None,
        force_kn: float,
        original_unit: str,
        confidence: float,
        source_url: str,
    ):
        with self.session_scope() as session:
            point = RimpullCurvePoint(
                equipment_id=equipment_id,
                gear=gear,
                speed_kmh=speed_kmh,
                force_kn=force_kn,
                original_unit=original_unit,
                confidence=confidence,
                source_url=source_url,
            )
            session.add(point)

    def insert_specs_batch(self, equipment_id: int, specs: list[dict]):
        """Batch insert multiple specs in a single transaction.

        Each dict should have: parameter, value, unit, confidence, source_url
        """
        with self.session_scope() as session:
            session.bulk_insert_mappings(
                TechnicalSpecRecord,
                [{"equipment_id": equipment_id, **s} for s in specs],
            )

    def insert_rimpull_points_batch(self, equipment_id: int, points: list[dict]):
        """Batch insert multiple rimpull curve points in a single transaction.

        Each dict should have: gear, speed_kmh, force_kn, original_unit, confidence, source_url
        """
        with self.session_scope() as session:
            session.bulk_insert_mappings(
                RimpullCurvePoint,
                [{"equipment_id": equipment_id, **p} for p in points],
            )

    def get_rimpull_curves_dataframe(self):
        """Retorna curvas rimpull como DataFrame de pandas."""
        import pandas as pd

        stmt = (
            select(
                Brand.nombre_completo.label("brand"),
                Equipment.model,
                RimpullCurvePoint.gear,
                RimpullCurvePoint.speed_kmh,
                RimpullCurvePoint.force_kn,
                RimpullCurvePoint.confidence,
            )
            .join(Equipment, Brand.id == Equipment.brand_id)
            .join(RimpullCurvePoint, Equipment.id == RimpullCurvePoint.equipment_id)
        )

        with self.engine.connect() as conn:
            df = pd.read_sql(stmt, conn)
        return df

    def get_visited_urls_for_brand(self, brand_key: str) -> set[str]:
        """Retorna set de URLs ya visitadas para una marca (desde data_sources)."""
        with self.session_scope() as session:
            rows = (
                session.query(DataSource.url)
                .join(Equipment, DataSource.equipment_id == Equipment.id)
                .join(Brand, Equipment.brand_id == Brand.id)
                .filter(Brand.key == brand_key)
                .all()
            )
            return {row[0] for row in rows}

    def clear_brand_data(self, brand_key: str) -> dict:
        """Borra technical_specs, data_sources y equipment de una marca. Preserva brands."""
        with self.session_scope() as session:
            brand = session.query(Brand).filter_by(key=brand_key).first()
            if not brand:
                return {"specs": 0, "sources": 0, "rimpull_points": 0, "equipment": 0}

            equipment_ids = [
                e.id for e in session.query(Equipment).filter_by(brand_id=brand.id).all()
            ]
            if not equipment_ids:
                return {"specs": 0, "sources": 0, "rimpull_points": 0, "equipment": 0}

            specs_deleted = session.query(TechnicalSpecRecord).filter(
                TechnicalSpecRecord.equipment_id.in_(equipment_ids)
            ).delete(synchronize_session="fetch")

            sources_deleted = session.query(DataSource).filter(
                DataSource.equipment_id.in_(equipment_ids)
            ).delete(synchronize_session="fetch")

            rimpull_deleted = session.query(RimpullCurvePoint).filter(
                RimpullCurvePoint.equipment_id.in_(equipment_ids)
            ).delete(synchronize_session="fetch")

            equip_deleted = session.query(Equipment).filter(
                Equipment.id.in_(equipment_ids)
            ).delete(synchronize_session="fetch")

            counts = {
                "specs": specs_deleted,
                "sources": sources_deleted,
                "rimpull_points": rimpull_deleted,
                "equipment": equip_deleted,
            }
            logger.info(f"Datos borrados para '{brand_key}': {counts}")
            return counts

    def get_brand_status(self, brand_key: str | None = None) -> dict:
        """Retorna estado de recopilacion para una marca o todas."""
        with self.session_scope() as session:
            if brand_key:
                brand = session.query(Brand).filter_by(key=brand_key).first()
                if not brand:
                    return {"error": f"Marca '{brand_key}' no encontrada"}
                return self._brand_detail(session, brand)
            else:
                brands = session.query(Brand).all()
                return {
                    "brands": [self._brand_detail(session, b) for b in brands],
                    "total_brands": len(brands),
                }

    def _brand_detail(self, session, brand: "Brand") -> dict:
        """Detalle de estado para una marca."""
        equipments = session.query(Equipment).filter_by(brand_id=brand.id).all()
        equip_ids = [e.id for e in equipments]

        total_specs = 0
        total_sources = 0
        last_spec_date = None
        models_detail = []

        if equip_ids:
            total_specs = session.query(TechnicalSpecRecord).filter(
                TechnicalSpecRecord.equipment_id.in_(equip_ids)
            ).count()
            total_sources = session.query(DataSource).filter(
                DataSource.equipment_id.in_(equip_ids)
            ).count()
            last_row = (
                session.query(func.max(TechnicalSpecRecord.extracted_at))
                .filter(TechnicalSpecRecord.equipment_id.in_(equip_ids))
                .scalar()
            )
            last_spec_date = str(last_row) if last_row else None

            # Single grouped COUNT query instead of N+1 per-equipment queries
            spec_counts = dict(
                session.query(
                    TechnicalSpecRecord.equipment_id,
                    func.count(TechnicalSpecRecord.id),
                )
                .filter(TechnicalSpecRecord.equipment_id.in_(equip_ids))
                .group_by(TechnicalSpecRecord.equipment_id)
                .all()
            )
            for e in equipments:
                models_detail.append({"model": e.model, "specs": spec_counts.get(e.id, 0)})

        return {
            "brand_key": brand.key,
            "nombre": brand.nombre_completo,
            "total_models": len(equipments),
            "total_specs": total_specs,
            "total_sources": total_sources,
            "last_run": last_spec_date,
            "models": models_detail,
        }

    def get_all_specs_dataframe(self):
        """Retorna todas las specs como DataFrame de pandas."""
        import pandas as pd

        stmt = (
            select(
                Brand.nombre_completo.label("brand"),
                Brand.pais.label("country"),
                Equipment.model,
                Equipment.category,
                Equipment.equipment_type,
                TechnicalSpecRecord.parameter,
                TechnicalSpecRecord.value,
                TechnicalSpecRecord.unit,
                TechnicalSpecRecord.confidence,
                TechnicalSpecRecord.source_url,
            )
            .join(Equipment, Brand.id == Equipment.brand_id)
            .join(TechnicalSpecRecord, Equipment.id == TechnicalSpecRecord.equipment_id)
        )

        with self.engine.connect() as conn:
            df = pd.read_sql(stmt, conn)
        return df
