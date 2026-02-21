"""
Pipeline principal que orquesta todo el proceso de busqueda de informacion tecnica:
1. Carga configuracion de marcas
2. Ejecuta deep search web por marca/modelo
3. Scrapea paginas encontradas
4. Extrae especificaciones tecnicas
5. Genera embeddings y almacena en vector store
6. Persiste resultados en base de datos
7. Genera reportes
"""

import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import pandas as pd
from tqdm import tqdm

from src.utils.config_loader import (
    load_env,
    load_brands_config,
    load_settings,
    get_all_brands_flat,
    get_all_models_for_brand,
)
from src.scrapers.web_search import DeepSearchOrchestrator, SearchResult
from src.scrapers.page_scraper import StaticPageScraper, PDFScraper
from src.parsers.spec_extractor import SpecExtractor, build_equipment_profile
from src.parsers.qa_pipeline import qa_equipment_specs, qa_rimpull_curve
from src.parsers.cross_validator import SpecCandidate, cross_validate_equipment_specs, cross_validate_rimpull_curves
from src.parsers.rimpull_extractor import RimpullCurveExtractor
from src.models.database import DatabaseManager
from src.models.embeddings import EmbeddingEngine, TextChunker, VectorStore

logger = logging.getLogger(__name__)


class MiningEquipmentPipeline:
    """Pipeline completo de busqueda y extraccion de informacion tecnica."""

    def __init__(self, settings: dict | None = None):
        load_env()
        self.settings = settings or load_settings()
        self.brands_config = load_brands_config()

        # Componentes
        self.search_engine = DeepSearchOrchestrator(
            delay_between_queries=self.settings.get("scraping", {}).get("request_delay_seconds", 2)
        )
        self.page_scraper = StaticPageScraper()
        self.pdf_scraper = PDFScraper()
        self.spec_extractor = SpecExtractor()
        self.db = DatabaseManager(
            db_path=os.environ.get("MINING_DB_PATH")
            or self.settings.get("storage", {}).get("database", "data/mining_equipment.db")
        )
        self.embedding_engine = EmbeddingEngine(
            model_name=self.settings.get("nlp", {}).get(
                "embedding_model", "sentence-transformers/all-MiniLM-L6-v2"
            )
        )
        self.chunker = TextChunker(
            chunk_size=self.settings.get("nlp", {}).get("chunk_size", 512),
            overlap=self.settings.get("nlp", {}).get("chunk_overlap", 50),
        )
        self.vector_store = VectorStore(
            persist_dir=self.settings.get("storage", {}).get("embeddings_dir", "data/embeddings")
        )

    def initialize(self):
        """Inicializa base de datos y carga marcas."""
        self.db.create_tables()
        logger.info("Pipeline inicializado")

        # Insertar marcas en DB
        brands = get_all_brands_flat(self.brands_config)
        for brand in brands:
            self.db.insert_brand(
                key=brand["key"],
                nombre=brand["nombre"],
                pais=brand["pais"],
                sitio_web=brand["sitio_web"],
                tier=brand["tier"],
            )
        logger.info(f"{len(brands)} marcas registradas en DB")

    def run_search_phase(self, brand_filter: str | None = None, fresh: bool = False) -> list[SearchResult]:
        """Fase 1: Deep search web para todas las marcas/modelos."""
        logger.info("=" * 60)
        logger.info("FASE 1: DEEP SEARCH WEB")
        logger.info("=" * 60)

        brands = get_all_brands_flat(self.brands_config)
        all_results = []

        target_brands = [b for b in brands if not brand_filter or b["key"] == brand_filter]
        for brand in target_brands:
            brand_key = brand["key"]

            # Fresh: borrar datos previos de esta marca
            if fresh:
                counts = self.db.clear_brand_data(brand_key)
                logger.info(f"Fresh mode: borrados {counts} para '{brand_key}'")

            # Resume: cargar URLs ya visitadas
            previously_visited: set[str] | None = None
            if not fresh:
                previously_visited = self.db.get_visited_urls_for_brand(brand_key)
                if previously_visited:
                    logger.info(f"Resume mode: {len(previously_visited)} URLs ya visitadas para '{brand_key}'")

            models = get_all_models_for_brand(brand)
            brand_name = brand["nombre"]
            logger.info(f"Marca: {brand_name} — {len(models)} modelos a buscar")

            for model_info in tqdm(models, desc=f"Buscando {brand_key}", unit="modelo"):
                model_name = model_info["model"]
                results = self.search_engine.search_brand(
                    brand=brand_name,
                    models=[model_name],
                    equipment_type=model_info["equipment_type"],
                    previously_visited_urls=previously_visited,
                )
                for r in results:
                    r.model = model_name
                all_results.extend(results)
                logger.info(
                    f"  {model_name}: +{len(results)} resultados "
                    f"(total acumulado: {len(all_results)})"
                )

            logger.info(f"  {brand_name}: {len(all_results)} resultados totales")

        # Guardar resultados raw
        self._save_search_results(all_results)
        return all_results

    def run_scraping_phase(self, search_results: list[SearchResult]) -> list[dict]:
        """Fase 2: Scrapeo de paginas encontradas."""
        logger.info("=" * 60)
        logger.info("FASE 2: SCRAPING DE PAGINAS")
        logger.info("=" * 60)

        scraped_data = []
        unique_urls = list({r.url for r in search_results})
        logger.info(f"{len(unique_urls)} URLs unicas a scrapear")

        # O(1) lookup for URL → search result
        url_to_result: dict[str, SearchResult] = {}
        for r in search_results:
            if r.url not in url_to_result:
                url_to_result[r.url] = r

        scraped_ok = 0
        scraped_fail = 0

        max_workers = self.settings.get("scraping", {}).get("max_concurrent_requests", 5)

        def _scrape_url(url: str):
            if url.lower().endswith(".pdf"):
                return self.pdf_scraper.extract_from_url(url)
            return self.page_scraper.scrape(url)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_url = {executor.submit(_scrape_url, url): url for url in unique_urls}
            for future in tqdm(as_completed(future_to_url), total=len(unique_urls),
                               desc="Scrapeando paginas", unit="pag"):
                url = future_to_url[future]
                try:
                    page = future.result()
                except Exception as e:
                    logger.error(f"Error scrapeando {url}: {e}")
                    scraped_fail += 1
                    continue

                if page and page.content_length > 100:
                    matching = url_to_result.get(url)
                    brand = matching.brand if matching else ""
                    model = matching.model if matching else ""

                    scraped_data.append({
                        "page": page,
                        "brand": brand,
                        "model": model,
                    })
                    scraped_ok += 1
                else:
                    scraped_fail += 1

        logger.info(
            f"Scraping completado: {scraped_ok} exitosas, "
            f"{scraped_fail} fallidas/vacias de {len(unique_urls)} URLs"
        )
        return scraped_data

    def run_extraction_phase(self, scraped_data: list[dict]):
        """Fase 3: Extraccion de especificaciones tecnicas con QA y validacion cruzada."""
        logger.info("=" * 60)
        logger.info("FASE 3: EXTRACCION DE SPECS")
        logger.info("=" * 60)

        total_specs = 0
        total_rejected = 0
        total_rimpull_points = 0
        rimpull_extractor = RimpullCurveExtractor()

        # Agrupar datos scrapeados por (brand, model)
        by_model: dict[tuple[str, str], list[dict]] = {}
        for item in scraped_data:
            key = (item["brand"], item["model"])
            by_model.setdefault(key, []).append(item)

        logger.info(f"{len(by_model)} modelos a procesar desde {len(scraped_data)} paginas")

        for (brand, model), items in tqdm(by_model.items(), desc="Extrayendo specs", unit="modelo"):
            # Recopilar specs de todas las fuentes para este equipo
            all_candidates: list[SpecCandidate] = []
            all_rimpull_curves = []

            for item in items:
                page = item["page"]

                # Extraer de texto
                text_specs = self.spec_extractor.extract_from_text(
                    page.text_content, brand, model, page.url
                )
                # Extraer de tablas
                table_specs = []
                for table in page.tables:
                    ts = self.spec_extractor.extract_from_table(table, brand, model, page.url)
                    table_specs.extend(ts)

                    # Extraer curvas rimpull de tablas
                    curve = rimpull_extractor.extract_from_table(table, brand, model, page.url)
                    if curve:
                        all_rimpull_curves.append(curve)

                # Extraer curvas rimpull de texto
                text_curves = rimpull_extractor.extract_from_text(
                    page.text_content, brand, model, page.url
                )
                all_rimpull_curves.extend(text_curves)

                # Construir perfil (normaliza, valida rangos, dedup por parametro)
                profile = build_equipment_profile(brand, model, "", text_specs, table_specs)

                # QA post-extraccion
                valid_specs, qa_report = qa_equipment_specs(profile.specs)
                total_rejected += qa_report["total_rejected"]

                # Agregar candidatos para validacion cruzada
                for spec in valid_specs:
                    all_candidates.append(SpecCandidate(
                        parameter=spec.parameter,
                        value=spec.value,
                        unit=spec.unit,
                        confidence=spec.confidence,
                        source_url=spec.source_url,
                    ))

            # Validacion cruzada multi-fuente
            validated = cross_validate_equipment_specs(all_candidates)

            # QA + cross-validate rimpull curves
            qa_passed_curves = []
            for curve in all_rimpull_curves:
                validated_curve, _ = qa_rimpull_curve(curve)
                if validated_curve:
                    qa_passed_curves.append(validated_curve)

            consolidated_rimpull = cross_validate_rimpull_curves(qa_passed_curves)

            # Insertar en DB
            from src.models.database import Brand as BrandModel
            with self.db.session_scope() as session:
                brand_record = session.query(BrandModel).filter(
                    BrandModel.nombre_completo == brand
                ).first()
                brand_id = brand_record.id if brand_record else None

            if brand_id and (validated or consolidated_rimpull):
                equip_id = self.db.insert_equipment(
                    brand_id=brand_id,
                    model=model,
                    category="",
                    equipment_type="",
                )

                # Batch insert specs
                if validated:
                    spec_dicts = [
                        {
                            "parameter": result.parameter,
                            "value": result.best_value,
                            "unit": result.best_unit,
                            "confidence": result.final_confidence,
                            "source_url": result.sources[0] if result.sources else "",
                        }
                        for result in validated
                    ]
                    self.db.insert_specs_batch(equip_id, spec_dicts)
                    total_specs += len(spec_dicts)

                # Batch insert rimpull curve points
                if consolidated_rimpull:
                    point_dicts = [
                        {
                            "gear": point.gear,
                            "speed_kmh": point.speed_kmh,
                            "force_kn": point.force_kn,
                            "original_unit": point.original_unit,
                            "confidence": point.confidence,
                            "source_url": point.source_url,
                        }
                        for point in consolidated_rimpull.points
                    ]
                    self.db.insert_rimpull_points_batch(equip_id, point_dicts)
                    total_rimpull_points += len(point_dicts)

        logger.info(
            f"Total specs validadas y almacenadas: {total_specs} "
            f"(rechazadas por QA: {total_rejected})"
        )
        logger.info(f"Total puntos rimpull almacenados: {total_rimpull_points}")

    def run_embedding_phase(self, scraped_data: list[dict]):
        """Fase 4: Generacion de embeddings y almacenamiento en vector store."""
        logger.info("=" * 60)
        logger.info("FASE 4: EMBEDDINGS")
        logger.info("=" * 60)

        all_chunks = []
        for item in tqdm(scraped_data, desc="Generando chunks", unit="pag"):
            page = item["page"]
            chunks = self.chunker.chunk_text(
                text=page.text_content,
                brand=item["brand"],
                model=item["model"],
                equipment_type="",
                source_url=page.url,
            )
            all_chunks.extend(chunks)

        if all_chunks:
            texts = [c.text for c in all_chunks]
            embeddings = self.embedding_engine.encode(texts)
            self.vector_store.add_documents(all_chunks, embeddings)
            logger.info(f"Almacenados {len(all_chunks)} chunks con embeddings")
        else:
            logger.warning("No hay chunks para generar embeddings")

    def run_report_phase(self):
        """Fase 5: Generacion de reportes."""
        logger.info("=" * 60)
        logger.info("FASE 5: REPORTES")
        logger.info("=" * 60)

        df = self.db.get_all_specs_dataframe()

        if df.empty:
            logger.warning("No hay datos para generar reportes")
            return

        reports_dir = Path(self.settings.get("storage", {}).get("reports_dir", "data/reports"))
        reports_dir.mkdir(parents=True, exist_ok=True)

        # Reporte completo CSV
        csv_path = reports_dir / "all_specs.csv"
        df.to_csv(csv_path, index=False, encoding="utf-8-sig")
        logger.info(f"Reporte CSV: {csv_path}")

        # Reporte por marca
        for brand in df["brand"].unique():
            brand_df = df[df["brand"] == brand]
            safe_name = re.sub(r"[^a-zA-Z0-9_-]", "", brand.replace(" ", "_"))[:30]
            brand_path = reports_dir / f"specs_{safe_name}.csv"
            brand_df.to_csv(brand_path, index=False, encoding="utf-8-sig")

        # Resumen estadistico
        summary = {
            "total_brands": df["brand"].nunique(),
            "total_models": df["model"].nunique(),
            "total_specs": len(df),
            "specs_per_brand": df.groupby("brand")["parameter"].count().to_dict(),
            "parameters_found": df["parameter"].value_counts().to_dict(),
        }

        import json
        summary_path = reports_dir / "summary.json"
        with open(summary_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)
        logger.info(f"Resumen: {summary_path}")

        # Excel con pivote por marca/modelo
        try:
            pivot = df.pivot_table(
                index=["brand", "model", "equipment_type"],
                columns="parameter",
                values="value",
                aggfunc="first",
            ).reset_index()
            excel_path = reports_dir / "equipment_comparison.xlsx"
            pivot.to_excel(excel_path, index=False)
            logger.info(f"Reporte Excel comparativo: {excel_path}")
        except Exception as e:
            logger.error(f"Error generando Excel: {e}")

        # Reporte HTML interactivo
        try:
            from src.reports.html_report import HTMLReportGenerator
            html_gen = HTMLReportGenerator(db=self.db, output_dir=str(reports_dir))
            html_path = html_gen.generate()
            logger.info(f"Reporte HTML interactivo: {html_path}")
        except Exception as e:
            logger.error(f"Error generando reporte HTML: {e}")

    def run_full_pipeline(self, brand_filter: str | None = None, fresh: bool = False):
        """Ejecuta el pipeline completo end-to-end."""
        logger.info("*" * 60)
        logger.info("INICIO PIPELINE - Mining Equipment Technical Search")
        logger.info("*" * 60)

        self.initialize()

        # Fase 1: Busqueda
        search_results = self.run_search_phase(brand_filter=brand_filter, fresh=fresh)

        if not search_results:
            logger.warning("No se encontraron resultados. Verifique las API keys.")
            return

        # Fase 2: Scraping
        scraped_data = self.run_scraping_phase(search_results)

        # Fase 3: Extraccion
        self.run_extraction_phase(scraped_data)

        # Fase 4: Embeddings
        self.run_embedding_phase(scraped_data)

        # Fase 5: Reportes
        self.run_report_phase()

        # Copiar DB a data/ como backup (SQLite no funciona directo en NFS)
        self._backup_database()

        # Cleanup search engine HTTP client
        self.search_engine.close()

        logger.info("*" * 60)
        logger.info("PIPELINE COMPLETADO")
        logger.info(f"  Resultados busqueda: {len(search_results)}")
        logger.info(f"  Paginas scrapeadas:  {len(scraped_data)}")
        logger.info("*" * 60)

    def print_brand_status(self, brand_key: str | None = None):
        """Muestra el estado de recopilacion de datos."""
        status = self.db.get_brand_status(brand_key)

        if "error" in status:
            print(f"\n  Error: {status['error']}")
            return

        if brand_key:
            # Detalle de una marca
            print(f"\n{'='*50}")
            print(f"  Marca: {status['nombre']} ({status['brand_key']})")
            print(f"{'='*50}")
            print(f"  Modelos:   {status['total_models']}")
            print(f"  Specs:     {status['total_specs']}")
            print(f"  Fuentes:   {status['total_sources']}")
            print(f"  Ultima corrida: {status['last_run'] or 'N/A'}")
            if status["models"]:
                print(f"\n  {'Modelo':<30} {'Specs':>6}")
                print(f"  {'-'*36}")
                for m in sorted(status["models"], key=lambda x: x["specs"], reverse=True):
                    print(f"  {m['model']:<30} {m['specs']:>6}")
        else:
            # Resumen de todas las marcas
            print(f"\n{'='*60}")
            print(f"  Estado de recopilacion - Todas las marcas")
            print(f"{'='*60}")
            print(f"  Total marcas registradas: {status['total_brands']}")
            print(f"\n  {'Marca':<20} {'Modelos':>8} {'Specs':>8} {'Fuentes':>8}")
            print(f"  {'-'*48}")
            for b in status["brands"]:
                print(f"  {b['brand_key']:<20} {b['total_models']:>8} {b['total_specs']:>8} {b['total_sources']:>8}")

    def semantic_search(self, query: str, n_results: int = 10, brand: str | None = None) -> dict:
        """Busqueda semantica sobre los documentos recopilados."""
        return self.vector_store.search_by_text(
            query=query,
            embedding_engine=self.embedding_engine,
            n_results=n_results,
            brand_filter=brand,
        )

    def _backup_database(self):
        """Crea copia de respaldo de la base de datos con timestamp."""
        import shutil
        db_path = self.db.db_path
        if db_path.exists():
            backup_dir = Path(self.settings.get("storage", {}).get("processed_data_dir", "data/processed"))
            backup_dir.mkdir(parents=True, exist_ok=True)
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_path = backup_dir / f"mining_equipment_{ts}.db.bak"
            try:
                shutil.copy2(db_path, backup_path)
                logger.info(f"Backup de DB copiado a: {backup_path}")
            except Exception as e:
                logger.warning(f"No se pudo copiar backup de DB: {e}")

    def _save_search_results(self, results: list[SearchResult]):
        """Guarda resultados de busqueda en CSV."""
        raw_dir = Path(self.settings.get("storage", {}).get("raw_data_dir", "data/raw"))
        raw_dir.mkdir(parents=True, exist_ok=True)

        rows = [
            {
                "brand": r.brand,
                "model": r.model,
                "title": r.title,
                "url": r.url,
                "snippet": r.snippet,
                "source_engine": r.source_engine,
                "query": r.query,
            }
            for r in results
        ]

        df = pd.DataFrame(rows)
        path = raw_dir / "search_results.csv"
        df.to_csv(path, index=False, encoding="utf-8-sig")
        logger.info(f"Resultados de busqueda guardados: {path} ({len(df)} filas)")
