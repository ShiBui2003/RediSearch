"""Deterministic text preprocessing pipeline with profile-specific behavior."""

from __future__ import annotations

import html
import re
import unicodedata
from enum import Enum

from bs4 import BeautifulSoup
from nltk.stem import PorterStemmer

from redisearch.config.settings import PreprocessingSettings, get_settings


_BASIC_STOPWORDS = {
    "a", "an", "and", "are", "as", "at", "be", "by", "for", "from",
    "in", "is", "it", "of", "on", "or", "that", "the", "this", "to",
    "was", "were", "with",
}

_URL_RE = re.compile(r"(?:https?://\S+|www\.\S+)", re.IGNORECASE)
_NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")


class PreprocessingProfile(str, Enum):
    """Supported preprocessing profiles."""

    DOCUMENT = "document"
    QUERY = "query"
    AUTOCOMPLETE = "autocomplete"


class TextPreprocessor:
    """
    9-step text preprocessing pipeline.

    Steps:
      1) HTML strip (DOCUMENT only)
      2) HTML entity decode
      3) Unicode normalization (NFKC)
      4) Lowercasing
      5) URL removal
      6) Punctuation/whitespace normalization
      7) Tokenization
      8) Token length filtering
      9) Profile transforms (stopword removal / stemming)
    """

    def __init__(self, settings: PreprocessingSettings | None = None) -> None:
        self._settings = settings or get_settings().preprocessing
        self._stemmer = PorterStemmer()

    def preprocess(self, text: str | None, profile: PreprocessingProfile) -> list[str]:
        """Run the pipeline and return processed tokens."""
        if not text:
            return []

        value = text

        if profile == PreprocessingProfile.DOCUMENT:
            value = BeautifulSoup(value, "lxml").get_text(" ")

        value = html.unescape(value)
        value = unicodedata.normalize("NFKC", value)
        value = value.lower()
        value = _URL_RE.sub(" ", value)
        value = _NON_ALNUM_RE.sub(" ", value)

        tokens = [t for t in value.split() if t]
        tokens = [
            t
            for t in tokens
            if self._settings.min_token_length <= len(t) <= self._settings.max_token_length
        ]

        if profile == PreprocessingProfile.AUTOCOMPLETE:
            return tokens

        tokens = [t for t in tokens if t not in _BASIC_STOPWORDS]
        tokens = [self._stemmer.stem(t) for t in tokens]
        return tokens
