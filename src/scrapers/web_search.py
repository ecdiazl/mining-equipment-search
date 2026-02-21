"""
Modulo de busqueda web profunda (deep search) para informacion tecnica de equipos mineros.
Soporta multiples motores de busqueda: Google Custom Search, Serper API, Bing.
"""

import collections
import os
import time
import logging
from typing import Optional
from urllib.parse import urlparse
from dataclasses import dataclass, field

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from src.utils.url_validator import is_safe_url

logger = logging.getLogger(__name__)


@dataclass
class SearchResult:
    """Resultado individual de busqueda."""
    title: str
    url: str
    snippet: str
    source_engine: str
    query: str
    brand: str = ""
    model: str = ""
    relevance_score: float = 0.0


@dataclass
class SearchSession:
    """Sesion de busqueda con resultados acumulados."""
    results: list[SearchResult] = field(default_factory=list)
    queries_executed: int = 0
    total_results_found: int = 0


class GoogleCustomSearchEngine:
    """Busqueda via Google Custom Search JSON API."""

    BASE_URL = "https://www.googleapis.com/customsearch/v1"

    def __init__(self):
        self.api_key = os.getenv("GOOGLE_API_KEY")
        self.cx = os.getenv("GOOGLE_CX")  # Custom Search Engine ID
        self.available = bool(self.api_key and self.cx)
        if not self.available:
            logger.warning("Google Custom Search API no configurada (GOOGLE_API_KEY / GOOGLE_CX)")

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, max=10))
    def search(self, query: str, num_results: int = 10, language: str = "en") -> list[SearchResult]:
        if not self.api_key:
            return []

        results = []
        params = {
            "key": self.api_key,
            "cx": self.cx,
            "q": query,
            "num": min(num_results, 10),
            "lr": f"lang_{language}",
        }

        with httpx.Client(timeout=30) as client:
            response = client.get(self.BASE_URL, params=params)
            response.raise_for_status()
            data = response.json()

        for item in data.get("items", []):
            results.append(SearchResult(
                title=item.get("title", ""),
                url=item.get("link", ""),
                snippet=item.get("snippet", ""),
                source_engine="google",
                query=query,
            ))

        logger.info(f"Google: {len(results)} resultados para '{query}'")
        return results


class SerperSearchEngine:
    """Busqueda via Serper.dev API (wrapper de Google)."""

    BASE_URL = "https://google.serper.dev/search"

    def __init__(self):
        self.api_key = os.getenv("SERPER_API_KEY")
        self.available = bool(self.api_key)
        if not self.available:
            logger.warning("Serper API no configurada (SERPER_API_KEY)")

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, max=10))
    def search(self, query: str, num_results: int = 20, language: str = "en") -> list[SearchResult]:
        if not self.api_key:
            return []

        results = []
        headers = {"X-API-KEY": self.api_key, "Content-Type": "application/json"}
        payload = {"q": query, "num": num_results, "hl": language}

        with httpx.Client(timeout=30) as client:
            response = client.post(self.BASE_URL, json=payload, headers=headers)
            response.raise_for_status()
            data = response.json()

        for item in data.get("organic", []):
            results.append(SearchResult(
                title=item.get("title", ""),
                url=item.get("link", ""),
                snippet=item.get("snippet", ""),
                source_engine="serper",
                query=query,
            ))

        logger.info(f"Serper: {len(results)} resultados para '{query}'")
        return results


class DeepSearchOrchestrator:
    """
    Orquestador de busqueda profunda.
    Genera multiples queries por marca/modelo y busca en multiples motores.
    """

    BLOCKED_DOMAINS = [
        "youtube.com", "youtu.be", "tiktok.com", "facebook.com", "instagram.com",
        "twitter.com", "x.com", "reddit.com", "pinterest.com", "linkedin.com",
        "amazon.com", "ebay.com", "aliexpress.com", "alibaba.com",
        "quora.com", "stackoverflow.com", "medium.com",
    ]

    # Templates base — siempre se ejecutan
    QUERY_TEMPLATES = [
        "{brand} {model} technical specifications mining",
        "{brand} {model} specs datasheet PDF",
        "{brand} {model} ficha tecnica mineria",
        "{brand} {model} brochure mining equipment",
        "{brand} {model} performance data horsepower weight capacity",
        "{brand} {model} maintenance manual",
        "{brand} mining {equipment_type} specifications {year}",
    ]

    # Templates expandidos — busquedas mas especificas
    EXPANDED_TEMPLATES = [
        # Specs de rendimiento y traccion
        "{brand} {model} rimpull curve speed chart",
        "{brand} {model} payload capacity operating weight",
        "{brand} {model} engine power fuel consumption specifications",
        # Dimensiones y geometria
        "{brand} {model} dimensions height width length dump height",
        "{brand} {model} digging depth reach radius specifications",
        # PDFs y brochures especificos
        "{brand} {model} specification sheet filetype:pdf",
        "{brand} {model} product brochure download PDF {year}",
        # Fuentes especializadas
        "site:lectura-specs.com {brand} {model}",
        "site:ritchiespecs.com {brand} {model}",
        # Busquedas en español / otros idiomas
        "{brand} {model} especificaciones tecnicas peso potencia capacidad",
        "{brand} {model} ficha tecnica PDF descarga",
        # Comparativas y reviews tecnicos
        "{brand} {model} vs specifications comparison mining",
        "{brand} {model} technical review mining {equipment_type}",
    ]

    # Fuentes directas por fabricante — URL patterns conocidos
    MANUFACTURER_SPEC_URLS = {
        "komatsu": [
            "https://www.komatsu.com/en/products/{equipment_type}/",
            "https://www.komatsu-mining.com/products/",
        ],
        "caterpillar": [
            "https://www.cat.com/en_US/products/new/equipment/",
        ],
        "xcmg": [
            "https://www.xcmg.com/en/product/",
        ],
        "sany": [
            "https://www.sanygroup.com/en/product/mining/",
        ],
        "liebherr": [
            "https://www.liebherr.com/en/int/products/mining-equipment/",
        ],
        "hitachi": [
            "https://www.hitachicm.com/global/products/",
        ],
        "belaz": [
            "https://belaz.by/en/products/dump-trucks/",
        ],
    }

    def __init__(self, delay_between_queries: float = 2.0, max_queries_per_minute: int = 20):
        self.engines = [
            GoogleCustomSearchEngine(),
            SerperSearchEngine(),
        ]
        self.delay = delay_between_queries
        self.max_qpm = max_queries_per_minute
        self._query_timestamps: collections.deque[float] = collections.deque()
        self.session = SearchSession()
        # Persistent HTTP client for connection pooling
        self._http_client = httpx.Client(timeout=30, follow_redirects=True)

    def close(self):
        """Close the persistent HTTP client."""
        self._http_client.close()

    def _enforce_rate_limit(self):
        """Espera si se excede el rate limit global (queries por minuto)."""
        now = time.time()
        # Remove timestamps older than 60 seconds — O(1) popleft
        while self._query_timestamps and now - self._query_timestamps[0] >= 60:
            self._query_timestamps.popleft()
        if len(self._query_timestamps) >= self.max_qpm:
            wait_time = 60 - (now - self._query_timestamps[0])
            if wait_time > 0:
                logger.info(f"Rate limit alcanzado ({self.max_qpm}/min). Esperando {wait_time:.1f}s")
                time.sleep(wait_time)
        self._query_timestamps.append(time.time())

    def _is_blocked_url(self, url: str) -> bool:
        """Retorna True si la URL pertenece a un dominio bloqueado."""
        try:
            domain = urlparse(url).netloc.lower()
            return any(domain == blocked or domain.endswith("." + blocked)
                       for blocked in self.BLOCKED_DOMAINS)
        except Exception:
            # Fail-closed: if we can't parse the URL, block it
            logger.warning(f"No se pudo parsear URL para bloqueo, bloqueando: {url}")
            return True

    def generate_queries(
        self,
        brand: str,
        models: list[str],
        equipment_type: str = "",
        year: str = "2024 2025",
        expanded: bool = True,
    ) -> list[str]:
        """Genera lista de queries a partir de templates base y expandidos."""
        templates = list(self.QUERY_TEMPLATES)
        if expanded:
            templates.extend(self.EXPANDED_TEMPLATES)

        queries = []
        for model in models:
            for template in templates:
                q = template.format(
                    brand=brand,
                    model=model,
                    equipment_type=equipment_type,
                    year=year,
                )
                queries.append(q)
        return queries

    def search_brand(
        self,
        brand: str,
        models: list[str],
        equipment_type: str = "",
        max_results_per_query: int = 10,
        previously_visited_urls: set[str] | None = None,
    ) -> list[SearchResult]:
        """Ejecuta deep search completo para una marca y sus modelos."""
        queries = self.generate_queries(brand, models, equipment_type)
        all_results: list[SearchResult] = []

        active_engines = [e for e in self.engines if e.available]
        logger.info(
            f"Deep search {brand} [{', '.join(m for m in models)}]: "
            f"{len(queries)} queries x {len(active_engines)} engines"
        )

        for i, query in enumerate(queries, 1):
            logger.info(f"  Query {i}/{len(queries)}: {query[:80]}")
            for engine in active_engines:
                self._enforce_rate_limit()
                try:
                    results = engine.search(query, num_results=max_results_per_query)
                    for r in results:
                        r.brand = brand
                    all_results.extend(results)
                    if results:
                        logger.info(f"    {engine.__class__.__name__}: +{len(results)} resultados")
                except Exception as e:
                    logger.error(f"Error en busqueda '{query}': {e}")

                time.sleep(self.delay)

            self.session.queries_executed += 1

        # Deduplicar por URL y filtrar dominios bloqueados
        seen_urls: set[str] = set(previously_visited_urls) if previously_visited_urls else set()
        if previously_visited_urls:
            logger.info(f"Pre-cargadas {len(previously_visited_urls)} URLs ya visitadas para {brand}")
        unique_results: list[SearchResult] = []
        blocked_count = 0
        ssrf_count = 0
        for r in all_results:
            if r.url not in seen_urls:
                if self._is_blocked_url(r.url):
                    blocked_count += 1
                    logger.debug(f"URL bloqueada (dominio no idoneo): {r.url}")
                    continue
                if not is_safe_url(r.url, resolve_dns=False):
                    ssrf_count += 1
                    logger.warning(f"URL rechazada (SSRF): {r.url}")
                    continue
                seen_urls.add(r.url)
                unique_results.append(r)

        if blocked_count:
            logger.info(f"Filtradas {blocked_count} URLs de dominios no idoneos")
        if ssrf_count:
            logger.info(f"Filtradas {ssrf_count} URLs por validacion SSRF")

        self.session.results.extend(unique_results)
        self.session.total_results_found += len(unique_results)

        logger.info(
            f"Deep search {brand}: {len(unique_results)} resultados unicos "
            f"de {len(all_results)} totales"
        )
        return unique_results

    def search_all_brands(self, brands_config: dict) -> SearchSession:
        """Ejecuta deep search para todas las marcas configuradas."""
        for tier_name, tier_brands in brands_config.items():
            if tier_name.startswith("_"):
                continue
            for brand_key, brand_info in tier_brands.items():
                brand_name = brand_info.get("nombre_completo", brand_key)
                for category in ["carguio", "transporte"]:
                    equipos = brand_info.get("equipos", {}).get(category, [])
                    for equipo in equipos:
                        models = equipo.get("series", [])
                        eq_type = equipo.get("tipo", "")
                        if models:
                            self.search_brand(brand_name, models, eq_type)

        logger.info(
            f"Deep search completo: {self.session.queries_executed} queries, "
            f"{self.session.total_results_found} resultados"
        )
        return self.session
