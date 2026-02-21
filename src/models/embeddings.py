"""
Modulo de embeddings y busqueda semantica para documentos tecnicos de equipos mineros.
Usa sentence-transformers para generar embeddings y ChromaDB/FAISS como vector store.
"""

import hashlib
import logging
from pathlib import Path
from dataclasses import dataclass

import numpy as np

logger = logging.getLogger(__name__)


@dataclass
class DocumentChunk:
    """Fragmento de documento con metadata."""
    chunk_id: str
    text: str
    brand: str
    model: str
    equipment_type: str
    source_url: str
    embedding: np.ndarray | None = None


class EmbeddingEngine:
    """Genera embeddings usando sentence-transformers."""

    def __init__(self, model_name: str = "sentence-transformers/all-MiniLM-L6-v2"):
        self.model_name = model_name
        self._model = None

    @property
    def model(self):
        if self._model is None:
            from sentence_transformers import SentenceTransformer
            self._model = SentenceTransformer(self.model_name)
            logger.info(f"Modelo de embeddings cargado: {self.model_name}")
        return self._model

    def encode(self, texts: list[str], batch_size: int = 32) -> np.ndarray:
        """Genera embeddings para una lista de textos."""
        embeddings = self.model.encode(texts, batch_size=batch_size, show_progress_bar=True)
        return np.array(embeddings)

    def encode_single(self, text: str) -> np.ndarray:
        """Genera embedding para un texto individual."""
        return self.model.encode([text])[0]


class TextChunker:
    """Divide textos largos en chunks con overlap para embeddings."""

    def __init__(self, chunk_size: int = 512, overlap: int = 50):
        if chunk_size < 1:
            raise ValueError(f"chunk_size must be >= 1, got {chunk_size}")
        if overlap < 0:
            raise ValueError(f"overlap must be >= 0, got {overlap}")
        if overlap >= chunk_size:
            raise ValueError(f"overlap ({overlap}) must be < chunk_size ({chunk_size})")
        self.chunk_size = chunk_size
        self.overlap = overlap

    def chunk_text(
        self,
        text: str,
        brand: str,
        model: str,
        equipment_type: str,
        source_url: str,
    ) -> list[DocumentChunk]:
        """Divide texto en chunks con metadata."""
        words = text.split()
        chunks = []

        start = 0
        chunk_idx = 0
        while start < len(words):
            end = start + self.chunk_size
            chunk_words = words[start:end]
            chunk_text = " ".join(chunk_words)

            # Include source_url hash to avoid chunk_id collisions across sources
            url_hash = hashlib.sha256(source_url.encode()).hexdigest()[:8]
            chunks.append(DocumentChunk(
                chunk_id=f"{brand}_{model}_{url_hash}_{chunk_idx}",
                text=chunk_text,
                brand=brand,
                model=model,
                equipment_type=equipment_type,
                source_url=source_url,
            ))

            start += self.chunk_size - self.overlap
            chunk_idx += 1

        return chunks


class VectorStore:
    """Almacena y busca embeddings usando ChromaDB."""

    def __init__(self, persist_dir: str = "data/embeddings"):
        self.persist_dir = Path(persist_dir)
        self.persist_dir.mkdir(parents=True, exist_ok=True)
        self._collection = None

    @property
    def collection(self):
        if self._collection is None:
            import chromadb
            client = chromadb.PersistentClient(path=str(self.persist_dir))
            self._collection = client.get_or_create_collection(
                name="mining_equipment_docs",
                metadata={"hnsw:space": "cosine"},
            )
            logger.info("ChromaDB collection inicializada")
        return self._collection

    def add_documents(self, chunks: list[DocumentChunk], embeddings: np.ndarray):
        """Agrega documentos con sus embeddings al vector store."""
        self.collection.add(
            ids=[c.chunk_id for c in chunks],
            embeddings=embeddings.tolist(),
            documents=[c.text for c in chunks],
            metadatas=[
                {
                    "brand": c.brand,
                    "model": c.model,
                    "equipment_type": c.equipment_type,
                    "source_url": c.source_url,
                }
                for c in chunks
            ],
        )
        logger.info(f"Agregados {len(chunks)} chunks al vector store")

    def search(
        self,
        query_embedding: np.ndarray,
        n_results: int = 10,
        brand_filter: str | None = None,
    ) -> dict:
        """Busca documentos similares por embedding."""
        where_filter = {"brand": brand_filter} if brand_filter else None

        results = self.collection.query(
            query_embeddings=[query_embedding.tolist()],
            n_results=n_results,
            where=where_filter,
            include=["documents", "metadatas", "distances"],
        )
        return results

    def search_by_text(
        self,
        query: str,
        embedding_engine: EmbeddingEngine,
        n_results: int = 10,
        brand_filter: str | None = None,
    ) -> dict:
        """Busca por texto (genera embedding automaticamente)."""
        query_emb = embedding_engine.encode_single(query)
        return self.search(query_emb, n_results, brand_filter)
