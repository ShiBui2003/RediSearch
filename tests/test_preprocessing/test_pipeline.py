"""Tests for preprocessing pipeline profiles."""

from redisearch.preprocessing.pipeline import PreprocessingProfile, TextPreprocessor


def test_document_profile_applies_html_strip_stopword_and_stemming():
    preprocessor = TextPreprocessor()

    text = "<p>This is running quickly on https://example.com and tested.</p>"
    tokens = preprocessor.preprocess(text, profile=PreprocessingProfile.DOCUMENT)

    assert "this" not in tokens
    assert "is" not in tokens
    assert "run" in tokens
    assert "quickli" in tokens
    assert "test" in tokens
    assert "http" not in tokens


def test_autocomplete_profile_skips_stopword_removal_and_stemming():
    preprocessor = TextPreprocessor()

    text = "Running and tested"
    tokens = preprocessor.preprocess(text, profile=PreprocessingProfile.AUTOCOMPLETE)

    assert tokens == ["running", "and", "tested"]
