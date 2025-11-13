"""Location lookup functionality."""

from typing import Dict
from .base import BaseLookup

class LocationLookup(BaseLookup):
    """Lookup implementation for locations."""

    # Standard location mappings
    STANDARD_LOCATIONS = {
        # US Locations
        'CVU(Ceva US) - INDIANA': 'US_CEVA_IN',
        'IMU(Ingram Micro US) - INDIANA': 'US_INGRAM_IN',
        'JDU Fulfillment Center': 'US_JDU_FC',
        
        # Canada Locations
        'IMC(Ingram MIcro Canada) - Mississauga': 'CA_INGRAM_MISSISSAUGA',
        
        # Australia Locations
        'ARV (Arvato AU) - Sydney': 'AU_ARVATO_SYDNEY',
        'SYD Square AU C/O DB Schenker': 'AU_SCHENKER_SYDNEY',
        
        # Europe Locations
        'NLD (Mainfreight) Born': 'NL_MAINFREIGHT_BORN',
        'GBR(Mainfreight) - UK': 'UK_MAINFREIGHT',
        
        # Japan Locations
        'SCH Schenker Fulfillment, c/o Square K.K.': 'JP_SCHENKER_KK'
    }

    def __init__(self):
        """Initialize the location lookup."""
        super().__init__()
        self._reverse_mapping = {v: k for k, v in self.STANDARD_LOCATIONS.items()}

    def translate(self, value: str) -> str:
        """Translate a location name to its standard form.
        
        Args:
            value: The location name to translate
            
        Returns:
            The standardized location name
        """
        # Check cache first
        cached = self._get_cached(value)
        if cached:
            return cached

        # Look up the standard form
        standard = self.STANDARD_LOCATIONS.get(value, value)
        
        # Cache and return
        self._set_cached(value, standard)
        return standard

    def validate(self, value: str) -> bool:
        """Validate if a location name is known.
        
        Args:
            value: The location name to validate
            
        Returns:
            True if valid, False otherwise
        """
        return value in self.STANDARD_LOCATIONS or value in self._reverse_mapping

    def get_display_name(self, code: str) -> str:
        """Get the display name for a location code.
        
        Args:
            code: The location code
            
        Returns:
            The display name
        """
        return self._reverse_mapping.get(code, code)
