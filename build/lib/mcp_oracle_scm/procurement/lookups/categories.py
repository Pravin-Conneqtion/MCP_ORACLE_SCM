"""Category lookup functionality."""

from typing import Dict
from .base import BaseLookup

class CategoryLookup(BaseLookup):
    """Lookup implementation for categories."""

    # Standard category mappings
    STANDARD_CATEGORIES = {
        # R12 Categories
        'R12': 'R12_STANDARD',
        'R12 - Accessory': 'R12_ACCESSORY',
        # X2 Categories
        'X2': 'X2_STANDARD',
        'X2 - Accessory': 'X2_ACCESSORY',
        'X2 - Bran': 'X2_BRAN'
    }

    def __init__(self):
        """Initialize the category lookup."""
        super().__init__()
        self._reverse_mapping = {v: k for k, v in self.STANDARD_CATEGORIES.items()}

    def translate(self, value: str) -> str:
        """Translate a category name to its standard form.
        
        Args:
            value: The category name to translate
            
        Returns:
            The standardized category name
        """
        # Check cache first
        cached = self._get_cached(value)
        if cached:
            return cached

        # Look up the standard form
        standard = self.STANDARD_CATEGORIES.get(value, value)
        
        # Cache and return
        self._set_cached(value, standard)
        return standard

    def validate(self, value: str) -> bool:
        """Validate if a category name is known.
        
        Args:
            value: The category name to validate
            
        Returns:
            True if valid, False otherwise
        """
        return value in self.STANDARD_CATEGORIES or value in self._reverse_mapping

    def get_display_name(self, code: str) -> str:
        """Get the display name for a category code.
        
        Args:
            code: The category code
            
        Returns:
            The display name
        """
        return self._reverse_mapping.get(code, code)
