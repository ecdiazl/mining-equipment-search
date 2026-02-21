"""Tests para confidence_scorer — scoring basado en fuente."""

from src.parsers.confidence_scorer import classify_source, compute_source_confidence, TRUST_LEVELS


class TestClassifySource:
    def test_manufacturer_cat(self):
        assert classify_source("https://www.cat.com/en_US/products/new/equipment") == "manufacturer"

    def test_manufacturer_komatsu(self):
        assert classify_source("https://www.komatsu.com/en/products/trucks") == "manufacturer"

    def test_manufacturer_xcmg(self):
        assert classify_source("https://www.xcmg.com/en/product/mining") == "manufacturer"

    def test_spec_database_lectura(self):
        assert classify_source("https://www.lectura-specs.com/en/model/komatsu/pc8000") == "spec_database"

    def test_spec_database_ritchie(self):
        assert classify_source("https://www.ritchiespecs.com/model/caterpillar-797f") == "spec_database"

    def test_industry_publication(self):
        assert classify_source("https://www.mining.com/article/caterpillar-797f") == "industry_publication"

    def test_pdf_brochure(self):
        assert classify_source("https://example.com/docs/brochure.pdf") == "pdf_brochure"

    def test_pdf_from_manufacturer(self):
        assert classify_source("https://www.cat.com/content/dam/cat/brochure.pdf") == "manufacturer"

    def test_dealer_url(self):
        assert classify_source("https://www.mining-dealer.com/used/cat-797f") == "dealer"

    def test_generic_unknown(self):
        assert classify_source("https://www.random-blog.com/article") == "generic"

    def test_empty_url(self):
        assert classify_source("") == "generic"


class TestComputeSourceConfidence:
    def test_manufacturer_table_gets_high_score(self):
        score = compute_source_confidence(0.9, "https://cat.com/specs", is_table_source=True)
        # 0.9*0.6 + 1.0*0.4 + 0.05 = 0.54 + 0.40 + 0.05 = 0.99
        assert score >= 0.95

    def test_generic_text_gets_lower_score(self):
        score = compute_source_confidence(0.8, "https://random-blog.com/article", is_table_source=False)
        # 0.8*0.6 + 0.6*0.4 = 0.48 + 0.24 = 0.72
        assert score < 0.8

    def test_table_bonus_applied(self):
        score_text = compute_source_confidence(0.8, "https://cat.com/specs", is_table_source=False)
        score_table = compute_source_confidence(0.8, "https://cat.com/specs", is_table_source=True)
        assert score_table > score_text

    def test_clamped_to_one(self):
        score = compute_source_confidence(1.0, "https://cat.com/specs", is_table_source=True)
        assert score <= 1.0

    def test_trust_levels_complete(self):
        """Verificar que todos los tipos de fuente tienen un trust level."""
        all_types = {"manufacturer", "spec_database", "industry_publication", "dealer", "pdf_brochure", "generic"}
        assert all_types.issubset(set(TRUST_LEVELS.keys()))
