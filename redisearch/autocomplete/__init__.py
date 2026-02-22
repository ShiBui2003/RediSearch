"""Autocomplete package — trie-based prefix suggestions."""

from redisearch.autocomplete.builder import AutocompleteBuilder
from redisearch.autocomplete.suggester import PrefixSuggester
from redisearch.autocomplete.trie import Suggestion, Trie

__all__ = [
    "AutocompleteBuilder",
    "PrefixSuggester",
    "Suggestion",
    "Trie",
]