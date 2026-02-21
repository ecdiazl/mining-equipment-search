"""
Entry point para el pipeline de busqueda de informacion tecnica de equipos mineros.

Uso:
    python main.py                          # Pipeline completo (resume por defecto)
    python main.py --brand xcmg             # Resume: expande busqueda de una marca
    python main.py --brand xcmg --fresh     # Fresh: borra datos y re-busca desde cero
    python main.py --status --brand xcmg    # Ver estado de recopilacion
    python main.py --status                 # Ver todas las marcas
    python main.py --search-only            # Solo fase de busqueda
    python main.py --report-only            # Regenerar reportes (incl. HTML)
    python main.py --view                   # Generar HTML y abrir en browser
    python main.py --query "Komatsu 930E payload capacity"  # Busqueda semantica
"""

import sys
import logging
import argparse
from pathlib import Path

# Agregar raiz del proyecto al path
sys.path.insert(0, str(Path(__file__).parent))

from src.pipelines.main_pipeline import MiningEquipmentPipeline
from src.utils.config_loader import load_settings


def setup_logging(settings: dict):
    """Configura logging segun settings."""
    log_config = settings.get("logging", {})
    log_file = log_config.get("file", "logs/pipeline.log")

    Path(log_file).parent.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=getattr(logging, log_config.get("level", "INFO")),
        format=log_config.get("format", "%(asctime)s - %(name)s - %(levelname)s - %(message)s"),
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )

    # Silenciar loggers ruidosos de terceros
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("chromadb").setLevel(logging.WARNING)
    logging.getLogger("sentence_transformers").setLevel(logging.WARNING)


def main():
    parser = argparse.ArgumentParser(
        description="Mining Equipment Technical Search - Deep Search Pipeline"
    )
    parser.add_argument(
        "--brand",
        type=str,
        default=None,
        help="Filtrar por marca (ej: komatsu, hitachi, xcmg, sany)",
    )
    parser.add_argument(
        "--search-only",
        action="store_true",
        help="Solo ejecutar fase de busqueda web (sin scraping ni extraccion)",
    )
    parser.add_argument(
        "--query",
        type=str,
        default=None,
        help="Realizar busqueda semantica sobre datos recopilados",
    )
    parser.add_argument(
        "--report-only",
        action="store_true",
        help="Solo regenerar reportes desde DB existente",
    )
    parser.add_argument(
        "--fresh",
        action="store_true",
        help="Borrar datos previos de la marca antes de buscar (empezar desde cero)",
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Mostrar estado de recopilacion de datos",
    )
    parser.add_argument(
        "--view",
        action="store_true",
        help="Generar y abrir reporte HTML interactivo",
    )
    args = parser.parse_args()

    settings = load_settings()
    setup_logging(settings)

    logger = logging.getLogger(__name__)
    pipeline = MiningEquipmentPipeline(settings=settings)

    # --status: mostrar estado y salir
    if args.status:
        pipeline.initialize()
        pipeline.print_brand_status(args.brand)
        return

    # --view: generar HTML y abrir en browser
    if args.view:
        pipeline.initialize()
        pipeline.run_report_phase()
        reports_dir = Path(settings.get("storage", {}).get("reports_dir", "data/reports"))
        html_path = reports_dir / "equipment_report.html"
        if html_path.exists():
            print(f"\nReporte HTML generado: {html_path}")
            import webbrowser
            webbrowser.open(str(html_path.resolve()))
        else:
            print("No se pudo generar el reporte HTML.")
        return

    if args.query:
        # Modo busqueda semantica
        logger.info(f"Busqueda semantica: '{args.query}'")
        results = pipeline.semantic_search(args.query, n_results=10, brand=args.brand)
        print("\n--- Resultados de busqueda semantica ---")
        if results.get("documents"):
            for i, (doc, meta, dist) in enumerate(zip(
                results["documents"][0],
                results["metadatas"][0],
                results["distances"][0],
            )):
                print(f"\n[{i+1}] Score: {1-dist:.3f}")
                print(f"    Marca: {meta.get('brand', 'N/A')}")
                print(f"    Modelo: {meta.get('model', 'N/A')}")
                print(f"    Fuente: {meta.get('source_url', 'N/A')}")
                print(f"    Texto: {doc[:200]}...")
        else:
            print("No se encontraron resultados. Ejecute primero el pipeline completo.")
        return

    if args.report_only:
        pipeline.initialize()
        pipeline.run_report_phase()
        return

    # --fresh sin --brand: pedir confirmacion antes de borrar todo
    if args.fresh and not args.brand:
        resp = input("ADVERTENCIA: --fresh sin --brand borrara datos de TODAS las marcas. Continuar? [y/N] ")
        if resp.lower() != "y":
            print("Operacion cancelada.")
            return

    if args.search_only:
        pipeline.initialize()
        results = pipeline.run_search_phase(brand_filter=args.brand, fresh=args.fresh)
        logger.info(f"Busqueda completada: {len(results)} resultados")
        return

    # Pipeline completo (resume por defecto, fresh si se pide)
    pipeline.run_full_pipeline(brand_filter=args.brand, fresh=args.fresh)


if __name__ == "__main__":
    main()
