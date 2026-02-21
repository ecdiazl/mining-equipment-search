"""
Modulo de scraping de paginas web para extraer contenido tecnico de equipos mineros.
Soporta HTML estatico y paginas con JavaScript (via Playwright).
"""

import logging
import os
import re
import time
from dataclasses import dataclass
from urllib.parse import urlparse, urljoin

import httpx
from bs4 import BeautifulSoup

from urllib.robotparser import RobotFileParser

from src.utils.url_validator import is_safe_url

logger = logging.getLogger(__name__)


# Limites de seguridad
MAX_HTML_BYTES = 10 * 1024 * 1024   # 10 MB para paginas HTML
MAX_PDF_BYTES = 50 * 1024 * 1024    # 50 MB para PDFs
PDF_CONTENT_TYPES = {"application/pdf", "application/x-pdf"}


_ROBOTS_TTL_SECONDS = 3600  # 1 hour


class RobotsChecker:
    """Cache de robots.txt por dominio con verificacion de permisos y TTL."""

    def __init__(self, user_agent: str = "MiningEquipResearch/1.0"):
        self.user_agent = user_agent
        self._cache: dict[str, tuple[RobotFileParser | None, float]] = {}

    def is_allowed(self, url: str) -> bool:
        """Verifica si el user-agent tiene permiso para acceder a la URL."""
        try:
            parsed = urlparse(url)
            base = f"{parsed.scheme}://{parsed.netloc}"

            now = time.time()
            cached = self._cache.get(base)
            if cached is not None:
                rp, ts = cached
                if now - ts > _ROBOTS_TTL_SECONDS:
                    # TTL expired, re-fetch
                    del self._cache[base]
                    cached = None

            if cached is None:
                rp = RobotFileParser()
                robots_url = f"{base}/robots.txt"
                rp.set_url(robots_url)
                try:
                    import urllib.request
                    with urllib.request.urlopen(robots_url, timeout=10) as resp:
                        raw = resp.read(64 * 1024).decode("utf-8", errors="replace")
                    rp.parse(raw.splitlines())
                except Exception:
                    # Si no se puede leer robots.txt, permitir acceso
                    self._cache[base] = (None, now)
                    return True
                self._cache[base] = (rp, now)
            else:
                rp = cached[0]

            if rp is None:
                return True
            return rp.can_fetch(self.user_agent, url)
        except Exception:
            return True


@dataclass
class ScrapedPage:
    """Pagina scrapeada con contenido extraido."""
    url: str
    title: str
    text_content: str
    tables: list[list[list[str]]]
    pdf_links: list[str]
    images: list[str]
    language: str = ""
    content_length: int = 0


class StaticPageScraper:
    """Scraper para paginas HTML estaticas."""

    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9,es;q=0.8",
    }

    def __init__(self, timeout: int = 30, respect_robots: bool = True):
        self.timeout = timeout
        self._robots = RobotsChecker() if respect_robots else None

    def scrape(self, url: str) -> ScrapedPage | None:
        """Scrapea una pagina web y extrae contenido relevante."""
        if not is_safe_url(url):
            logger.warning(f"URL rechazada por validacion SSRF: {url}")
            return None

        if self._robots and not self._robots.is_allowed(url):
            logger.info(f"URL bloqueada por robots.txt: {url}")
            return None

        try:
            with httpx.Client(
                timeout=httpx.Timeout(self.timeout, connect=10),
                follow_redirects=True,
                max_redirects=5,
            ) as client:
                response = client.get(url, headers=self.HEADERS)
                response.raise_for_status()

                if len(response.content) > MAX_HTML_BYTES:
                    logger.warning(f"Respuesta demasiado grande ({len(response.content)} bytes): {url}")
                    return None

                soup = BeautifulSoup(response.text, "html.parser")

                # Remover scripts y estilos
                for tag in soup(["script", "style", "nav", "footer", "header"]):
                    tag.decompose()

                title = soup.title.string.strip() if soup.title and soup.title.string else ""
                text = self._extract_text(soup)
                tables = self._extract_tables(soup)
                pdf_links = self._extract_pdf_links(soup, url)
                images = self._extract_images(soup, url)

                page = ScrapedPage(
                    url=url,
                    title=title,
                    text_content=text,
                    tables=tables,
                    pdf_links=pdf_links,
                    images=images,
                    content_length=len(text),
                )
                logger.info(f"Scrapeado: {url} ({len(text)} chars, {len(tables)} tablas)")
                return page

        except httpx.TimeoutException:
            logger.error(f"Timeout scrapeando {url} (>{self.timeout}s)")
            return None
        except Exception as e:
            logger.error(f"Error scrapeando {url}: {e}")
            return None

    def _extract_text(self, soup: BeautifulSoup) -> str:
        """Extrae texto limpio del HTML."""
        text = soup.get_text(separator="\n", strip=True)
        # Limpiar lineas vacias multiples
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text

    def _extract_tables(self, soup: BeautifulSoup) -> list[list[list[str]]]:
        """Extrae tablas del HTML con soporte para colspan/rowspan y deteccion de headers."""
        tables = []
        for table_tag in soup.find_all("table"):
            table_data = self._parse_table_tag(table_tag)
            if table_data and len(table_data) >= 2:  # Al menos header + 1 fila
                tables.append(table_data)
        return tables

    def _parse_table_tag(self, table_tag) -> list[list[str]]:
        """Parsea un tag <table> con soporte para colspan."""
        rows = table_tag.find_all("tr")
        if not rows:
            return []

        table_data = []
        for row in rows:
            cells = []
            for cell in row.find_all(["td", "th"]):
                text = cell.get_text(strip=True)
                # Expandir colspan: repetir el texto para cada columna abarcada
                colspan = int(cell.get("colspan", 1))
                cells.append(text)
                for _ in range(colspan - 1):
                    cells.append("")
            if cells:
                table_data.append(cells)

        return table_data

    def _extract_pdf_links(self, soup: BeautifulSoup, base_url: str) -> list[str]:
        """Extrae links a PDFs (brochures, datasheets, manuales)."""
        pdf_links = []
        for a_tag in soup.find_all("a", href=True):
            href = a_tag["href"]
            if href.lower().endswith(".pdf"):
                full_url = urljoin(base_url, href)
                if is_safe_url(full_url, resolve_dns=False):
                    pdf_links.append(full_url)
        return pdf_links

    def _extract_images(self, soup: BeautifulSoup, base_url: str) -> list[str]:
        """Extrae URLs de imagenes relevantes."""
        images = []
        for img in soup.find_all("img", src=True):
            src = img["src"]
            full_url = urljoin(base_url, src)
            if is_safe_url(full_url, resolve_dns=False):
                images.append(full_url)
        return images


class DynamicPageScraper:
    """Scraper para paginas con JavaScript usando Playwright."""

    async def scrape(self, url: str) -> ScrapedPage | None:
        """Scrapea pagina renderizada con JavaScript."""
        if not is_safe_url(url):
            logger.warning(f"URL rechazada por validacion SSRF: {url}")
            return None

        try:
            from playwright.async_api import async_playwright

            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                try:
                    page = await browser.new_page()
                    await page.goto(url, wait_until="networkidle", timeout=30000)
                    content = await page.content()
                finally:
                    await browser.close()

            if len(content.encode("utf-8", errors="replace")) > MAX_HTML_BYTES:
                logger.warning(f"Dynamic content too large ({len(content)} chars): {url}")
                return None

            soup = BeautifulSoup(content, "html.parser")
            for tag in soup(["script", "style", "nav", "footer"]):
                tag.decompose()

            title = soup.title.string.strip() if soup.title and soup.title.string else ""
            static = StaticPageScraper()

            return ScrapedPage(
                url=url,
                title=title,
                text_content=static._extract_text(soup),
                tables=static._extract_tables(soup),
                pdf_links=static._extract_pdf_links(soup, url),
                images=static._extract_images(soup, url),
                content_length=len(soup.get_text()),
            )
        except Exception as e:
            logger.error(f"Error en scraping dinamico {url}: {e}")
            return None


class PDFScraper:
    """Extrae texto y tablas de documentos PDF (brochures, datasheets)."""

    def extract_from_url(self, url: str) -> ScrapedPage | None:
        """Descarga y extrae contenido de un PDF con validaciones de seguridad."""
        if not is_safe_url(url):
            logger.warning(f"URL de PDF rechazada por validacion SSRF: {url}")
            return None

        tmp_path = None
        try:
            import tempfile

            with httpx.Client(
                timeout=httpx.Timeout(60, connect=10),
                follow_redirects=True,
                max_redirects=5,
            ) as client:
                # Streaming download con limite de tamano
                with client.stream("GET", url) as response:
                    response.raise_for_status()

                    # Validar content-type: require valid CT OR .pdf extension
                    ct = response.headers.get("content-type", "").lower().split(";")[0].strip()
                    is_pdf_ct = ct in PDF_CONTENT_TYPES
                    is_pdf_ext = url.lower().endswith(".pdf")
                    if not is_pdf_ct and not is_pdf_ext:
                        logger.warning(f"Content-type no es PDF ({ct}) y URL no termina en .pdf: {url}")
                        return None

                    # Stream directly to temp file to avoid memory accumulation
                    total = 0
                    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as f:
                        tmp_path = f.name
                        for chunk in response.iter_bytes(chunk_size=8192):
                            total += len(chunk)
                            if total > MAX_PDF_BYTES:
                                logger.warning(f"PDF excede limite de {MAX_PDF_BYTES} bytes: {url}")
                                return None
                            f.write(chunk)

            return self.extract_from_file(tmp_path, source_url=url)

        except Exception as e:
            logger.error(f"Error extrayendo PDF {url}: {e}")
            return None
        finally:
            # Limpiar archivo temporal
            if tmp_path:
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass

    def extract_from_file(self, filepath: str, source_url: str = "") -> ScrapedPage | None:
        """Extrae contenido de un archivo PDF local."""
        try:
            import pdfplumber

            all_text = []
            all_tables = []

            with pdfplumber.open(filepath) as pdf:
                for page in pdf.pages:
                    text = page.extract_text()
                    if text:
                        all_text.append(text)

                    tables = page.extract_tables()
                    for table in tables:
                        clean_table = [
                            [cell or "" for cell in row]
                            for row in table
                            if row
                        ]
                        if clean_table:
                            all_tables.append(clean_table)

            full_text = "\n\n".join(all_text)

            return ScrapedPage(
                url=source_url,
                title=f"PDF: {source_url.split('/')[-1] if source_url else filepath}",
                text_content=full_text,
                tables=all_tables,
                pdf_links=[],
                images=[],
                content_length=len(full_text),
            )

        except Exception as e:
            logger.error(f"Error procesando PDF {filepath}: {e}")
            return None
