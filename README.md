# Mining Equipment Technical Search

Pipeline de busqueda profunda y extraccion automatizada de especificaciones tecnicas para equipos mineros de gran escala (palas hidraulicas, cargadores, camiones mineros) de los principales fabricantes mundiales.

## Arquitectura

```
config/brands.yaml          Inventario de marcas y modelos
        |
   [1. Deep Search]         Google Custom Search + Serper API
        |
   [2. Scraping]            HTML estatico + PDF (pdfplumber)
        |
   [3. Extraction]          30+ parametros via regex + tablas
        |                   QA pipeline + validacion cruzada
        |
   [4. Embeddings]          sentence-transformers → ChromaDB
        |
   [5. Reports]             CSV + Excel + HTML interactivo (Plotly)
        |
   data/mining_equipment.db SQLite (SQLAlchemy ORM)
```

## Marcas Soportadas

| Tier | Marcas |
|------|--------|
| Tier 1 | Komatsu, Hitachi, Liebherr, Volvo CE |
| Tier 2 | BELAZ, Doosan Infracore |
| Chinese | XCMG, SANY, Zoomlion, LiuGong, Shantui |

## Parametros Extraidos

Peso operativo, potencia motor, modelo motor, capacidad balde/cucharon, capacidad de carga, torque, cilindrada, velocidad maxima, consumo combustible, capacidad tanque, dimensiones (ancho/largo/alto), fuerza excavacion, presion hidraulica, caudal hidraulico, altura descarga, profundidad excavacion, alcance maximo, presion suelo, velocidad giro, pendiente maxima, radio giro, norma emisiones, tipo transmision, tamano neumaticos, voltaje sistema, **curvas rimpull** (fuerza vs marcha/velocidad), y mas.

## Requisitos

- Python 3.11+
- Al menos una API key de busqueda (Google Custom Search o Serper)

## Instalacion

```bash
# Clonar
git clone https://github.com/ecdiazl/mining-equipment-search.git
cd mining-equipment-search

# Entorno virtual
python -m venv .venv
source .venv/bin/activate

# Dependencias
pip install -r requirements.txt

# Configurar API keys
cp .env.example .env
# Editar .env con tus keys
```

### Variables de Entorno

| Variable | Descripcion | Requerida |
|----------|-------------|-----------|
| `GOOGLE_API_KEY` | Google Custom Search API key | Si* |
| `GOOGLE_CX` | Custom Search Engine ID | Si* |
| `SERPER_API_KEY` | Serper.dev API key | Si* |
| `BING_API_KEY` | Bing Search API key | No |

*Al menos Google o Serper debe estar configurado.

## Uso

### CLI (main.py)

```bash
# Pipeline completo (todas las marcas)
python main.py

# Pipeline para una marca especifica
python main.py --brand komatsu

# Fresh run (borra datos previos de la marca)
python main.py --brand komatsu --fresh

# Solo fase de busqueda web
python main.py --search-only --brand komatsu

# Regenerar reportes desde DB existente
python main.py --report-only

# Ver estado de recopilacion
python main.py --status
python main.py --status --brand komatsu

# Busqueda semantica sobre documentos recopilados
python main.py --query "rimpull curve Komatsu 930E"

# Generar y abrir reporte HTML
python main.py --view
```

### Shell wrapper (run.sh)

```bash
./run.sh pipeline --brand komatsu    # Pipeline completo
./run.sh search --brand komatsu      # Solo busqueda
./run.sh report                      # Regenerar reportes
./run.sh status                      # Estado de todas las marcas
./run.sh test                        # Correr tests
./run.sh test-cov                    # Tests con cobertura
./run.sh validate                    # Validar entorno y API keys
./run.sh db-fix                      # Reparar DB bloqueada (NFS)
./run.sh backup                      # Backup manual de DB
./run.sh clean --brand komatsu       # Limpiar datos de una marca
```

## Estructura del Proyecto

```
mining-equipment-search/
├── config/
│   ├── brands.yaml              # Inventario de marcas y modelos
│   └── settings.yaml            # Configuracion general
├── src/
│   ├── scrapers/
│   │   ├── web_search.py        # Deep search (Google, Serper)
│   │   └── page_scraper.py      # HTML + PDF scraping
│   ├── parsers/
│   │   ├── spec_extractor.py    # Extraccion de specs (regex)
│   │   ├── rimpull_extractor.py # Curvas rimpull
│   │   ├── qa_pipeline.py       # Validacion post-extraccion
│   │   ├── cross_validator.py   # Validacion cruzada multi-fuente
│   │   └── confidence_scorer.py # Scoring por tipo de fuente
│   ├── models/
│   │   ├── database.py          # SQLAlchemy ORM + SQLite
│   │   └── embeddings.py        # Sentence-transformers + ChromaDB
│   ├── pipelines/
│   │   └── main_pipeline.py     # Orquestador del pipeline
│   ├── reports/
│   │   └── html_report.py       # Reporte HTML interactivo
│   └── utils/
│       ├── config_loader.py     # Carga YAML + .env
│       ├── config_schemas.py    # Validacion Pydantic
│       └── url_validator.py     # Proteccion SSRF
├── notebooks/
│   ├── 01_exploracion_marcas.py # Explorar inventario de marcas
│   └── 02_deep_search_demo.py   # Demo rapido de busqueda
├── tests/                       # 156 tests (pytest)
├── main.py                      # CLI entry point
├── run.sh                       # Shell wrapper
└── requirements.txt
```

## Seguridad

- **SSRF protection**: bloqueo de IPs privadas, cloud metadata (AWS/GCP/Azure), IPv4-mapped IPv6
- **Path traversal**: validacion en config loader y reportes
- **XSS prevention**: escape de JSON en reportes HTML
- **Input validation**: schemas Pydantic con bounds en todos los campos
- **ReDoS mitigation**: regex acotados con limites de longitud
- **Fail-closed**: DNS no resuelto y URLs no parseables se bloquean
- **robots.txt**: respeto por defecto con cache TTL

## Tests

```bash
python -m pytest tests/ -v
```

156 tests cubriendo: extraccion de specs, rimpull curves, QA pipeline, validacion cruzada, confidence scoring, SSRF protection, config validation, database operations, HTML report generation.

## Dependencias Principales

| Libreria | Uso |
|----------|-----|
| httpx | HTTP client (scraping + APIs) |
| beautifulsoup4 | Parsing HTML |
| pdfplumber | Extraccion de texto/tablas de PDFs |
| sqlalchemy | ORM para SQLite |
| pydantic | Validacion de configuracion |
| sentence-transformers | Embeddings para busqueda semantica |
| chromadb | Vector store |
| plotly | Graficos en reporte HTML |
| pandas | Procesamiento de datos y reportes |
| tenacity | Reintentos con backoff exponencial |

## Outputs

- `data/mining_equipment.db` — Base de datos SQLite con todas las specs
- `data/reports/equipment_report.html` — Reporte HTML interactivo con tabla filtrable y graficos
- `data/reports/all_specs.csv` — Todas las specs en CSV
- `data/reports/equipment_comparison.xlsx` — Tabla pivote por marca/modelo
- `data/reports/summary.json` — Resumen estadistico
- `data/embeddings/` — Vector store para busqueda semantica
