"""Base lookup functionality for procurement data translation."""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
import logging

class BaseLookup(ABC):
    """Abstract base class for all lookup implementations."""
    
    def __init__(self):
        """Initialize the lookup with empty cache."""
        self._cache = {}
        self._logger = logging.getLogger(__name__)

    @abstractmethod
    def translate(self, value: str) -> str:
        """Translate a value using the lookup.
        
        Args:
            value: The value to translate
            
        Returns:
            The translated value
        """
        pass

    @abstractmethod
    def validate(self, value: str) -> bool:
        """Validate if a value is valid for this lookup.
        
        Args:
            value: The value to validate
            
        Returns:
            True if valid, False otherwise
        """
        pass

    def clear_cache(self):
        """Clear the lookup cache."""
        self._cache.clear()

    def _get_cached(self, key: str) -> Optional[str]:
        """Get a value from cache."""
        return self._cache.get(key)

    def _set_cached(self, key: str, value: str):
        """Set a value in cache."""
        self._cache[key] = value
