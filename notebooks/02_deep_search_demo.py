"""
Demo de Deep Search: Ejecuta busqueda para una marca especifica
y muestra resultados antes de correr el pipeline completo.

Uso: python notebooks/02_deep_search_demo.py
"""

# %% Imports
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(message)s")

from src.utils.config_loader import load_env, load_brands_config, get_all_brands_flat
from src.scrapers.web_search import DeepSearchOrchestrator
from src.scrapers.page_scraper import StaticPageScraper
from src.parsers.spec_extractor import SpecExtractor

load_env()

# %% Configurar busqueda para una marca de prueba
brands = get_all_brands_flat(load_brands_config())

# Seleccionar marca para demo (cambiar indice segun necesidad)
demo_brand = brands[0]  # Primera marca (Komatsu)
print(f"Demo de busqueda para: {demo_brand['nombre']}")
print(f"Pais: {demo_brand['pais']}")

# Tomar solo 2 modelos para demo rapido
equipos = demo_brand.get("equipos", {})
demo_models = []
for cat in ["carguio", "transporte"]:
    for equipo in equipos.get(cat, []):
        if equipo.get("series"):
            demo_models.append(equipo["series"][0])  # Solo primer modelo de cada tipo
            break

print(f"Modelos de prueba: {demo_models}")

# %% Ejecutar deep search
orchestrator = DeepSearchOrchestrator(delay_between_queries=1.5)
results = orchestrator.search_brand(
    brand=demo_brand["nombre"],
    models=demo_models,
    max_results_per_query=5,
)

print(f"\nResultados encontrados: {len(results)}")
for i, r in enumerate(results[:10]):
    print(f"\n[{i+1}] {r.title}")
    print(f"    URL: {r.url}")
    print(f"    Snippet: {r.snippet[:150]}...")

# %% Scrapear primera pagina de resultados como prueba
if results:
    print("\n\n--- Scrapeando primera pagina ---")
    scraper = StaticPageScraper()
    page = scraper.scrape(results[0].url)

    if page:
        print(f"Titulo: {page.title}")
        print(f"Longitud texto: {page.content_length} chars")
        print(f"Tablas encontradas: {len(page.tables)}")
        print(f"PDFs encontrados: {len(page.pdf_links)}")

        # Extraer specs
        extractor = SpecExtractor()
        specs = extractor.extract_from_text(
            page.text_content,
            demo_brand["nombre"],
            demo_models[0],
            page.url,
        )
        print(f"\nSpecs extraidas: {len(specs)}")
        for spec in specs:
            print(f"  {spec.parameter}: {spec.value} {spec.unit} (confianza: {spec.confidence})")
