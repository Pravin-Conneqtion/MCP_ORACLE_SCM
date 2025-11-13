"""Business Unit lookup functionality."""

from typing import Dict, Set, Optional
from .base import BaseLookup

class BusinessUnitLookup(BaseLookup):
    """Lookup implementation for business units."""

    # What users might query -> What Oracle recognizes
    BU_ALIASES = {
        # US variations -> Oracle recognized name
        'BLOCK': 'US',
        'BLOCK US': 'US',
        'BLOCK USA': 'US',
        'BLOCK INC': 'US',
        'BLOCK 101': 'US',
        'US 101': 'US',
        'BU 101': 'US',
        '101': 'US',
        
        # Canada variations -> Oracle recognized name
        'SQUARE CA': 'Canada',
        'SQUARE CANADA': 'Canada',
        'SQUARE TECH CA': 'Canada',
        'CA 152': 'Canada',
        'BU 152': 'Canada',
        '152': 'Canada',
        
        # Australia variations -> Oracle recognized name
        'SQUARE AU': 'Australia',
        'SQUARE AUS': 'Australia',
        'SQUARE AUSTRALIA': 'Australia',
        'AU 250': 'Australia',
        'BU 250': 'Australia',
        '250': 'Australia',
        
        # Ireland variations -> Oracle recognized name
        'SQUARE IE': 'Ireland',
        'SQUARE IRELAND': 'Ireland',
        'SQUAREUP IE': 'Ireland',
        'IE 312': 'Ireland',
        'BU 312': 'Ireland',
        '312': 'Ireland',
        
        # UK variations -> Oracle recognized name
        'SQUARE UK': 'UK',
        'SQUAREUP UK': 'UK',
        'UK 302': 'UK',
        'BU 302': 'UK',
        '302': 'UK',
        
        # Japan variations -> Oracle recognized name
        'SQUARE JP': 'Japan',
        'SQUARE JAPAN': 'Japan',
        'SQUARE KK': 'Japan',
        'JP 210': 'Japan',
        'BU 210': 'Japan',
        '210': 'Japan'
    }

    # Valid Oracle Business Units for validation
    ORACLE_BUS = {
        'US',
        'Canada',
        'Australia',
        'Ireland',
        'UK',
        'Japan'
    }

    # Business Unit Categories
    BU_CATEGORIES = {
        'AMERICAS': ['US', 'Canada'],
        'APAC': ['Australia', 'Japan'],
        'EMEA': ['Ireland', 'UK']
    }

    def __init__(self):
        """Initialize the business unit lookup."""
        super().__init__()
        
        # Create normalized lookup maps for case-insensitive matching
        self._normalized_bus = {
            self._normalize_text(k): v for k, v in self.BU_ALIASES.items()
        }

    def _normalize_text(self, text: str) -> str:
        """Normalize text for consistent lookup.
        
        Args:
            text: Text to normalize
            
        Returns:
            Normalized text
        """
        return text.upper().replace(' ', '').replace('-', '').replace('.', '').replace(',', '')

    def translate(self, value: str) -> str:
        """Translate a user's business unit input to Oracle's recognized name.
        
        Args:
            value: The business unit name to translate (user input)
            
        Returns:
            The Oracle recognized business unit name
            
        Example:
            >>> lookup = BusinessUnitLookup()
            >>> lookup.translate("BLOCK US")
            'US'
            >>> lookup.translate("US 101")
            'US'
        """
        if not value:
            return value

        # If it's already an Oracle recognized name, return it
        if value in self.ORACLE_BUS:
            return value

        # Check cache first
        cached = self._get_cached(value)
        if cached:
            return cached

        # Normalize input
        normalized = self._normalize_text(value)
        
        # Look up the Oracle recognized name
        oracle_name = self._normalized_bus.get(normalized, value)
        
        # Cache and return
        self._set_cached(value, oracle_name)
        return oracle_name

    def validate(self, value: str) -> bool:
        """Validate if a business unit name is valid in Oracle.
        
        Args:
            value: The business unit name to validate
            
        Returns:
            True if valid in Oracle, False otherwise
            
        Example:
            >>> lookup = BusinessUnitLookup()
            >>> lookup.validate("BLOCK US")  # Translates to valid Oracle BU
            True
            >>> lookup.validate("US")  # Direct Oracle BU
            True
            >>> lookup.validate("Unknown BU")
            False
        """
        # First translate to Oracle name
        oracle_name = self.translate(value)
        # Check if it's a valid Oracle business unit
        return oracle_name in self.ORACLE_BUS

    def get_category(self, value: str) -> Optional[str]:
        """Get the category for a business unit.
        
        Args:
            value: The business unit name
            
        Returns:
            The category name (AMERICAS, APAC, EMEA) or None if not found
            
        Example:
            >>> lookup = BusinessUnitLookup()
            >>> lookup.get_category("BLOCK US")
            'AMERICAS'
        """
        oracle_name = self.translate(value)
        
        for category, bus in self.BU_CATEGORIES.items():
            if oracle_name in bus:
                return category
        return None

    def get_all_variations(self, oracle_name: str) -> Set[str]:
        """Get all known variations of a business unit name.
        
        Args:
            oracle_name: The Oracle recognized business unit name
            
        Returns:
            Set of all known variations
            
        Example:
            >>> lookup = BusinessUnitLookup()
            >>> variations = lookup.get_all_variations("US")
            >>> 'BLOCK US' in variations
            True
            >>> 'US 101' in variations
            True
        """
        if oracle_name not in self.ORACLE_BUS:
            return set()

        variations = {oracle_name}
        
        # Add all aliases that map to this Oracle name
        variations.update(
            alias for alias, oracle in self.BU_ALIASES.items()
            if oracle == oracle_name
        )
        
        return variations

    def get_bu_number(self, value: str) -> Optional[str]:
        """Extract the business unit number from a name.
        
        Args:
            value: The business unit name
            
        Returns:
            The business unit number or None if not found
            
        Example:
            >>> lookup = BusinessUnitLookup()
            >>> lookup.get_bu_number("BLOCK US")
            '101'
            >>> lookup.get_bu_number("US 101")
            '101'
        """
        oracle_name = self.translate(value)
        if not oracle_name:
            return None
            
        # Extract the number from the end of the Oracle name (e.g., "- 101")
        try:
            return oracle_name.split('-')[-1].strip()
        except:
            return None
