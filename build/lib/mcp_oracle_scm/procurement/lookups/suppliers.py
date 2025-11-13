"""Supplier lookup functionality."""

from typing import Dict, Set
from .base import BaseLookup

class SupplierLookup(BaseLookup):
    """Lookup implementation for suppliers."""

    # Standard supplier mappings (Oracle recognized names as keys)
    STANDARD_SUPPLIERS = {
        'Hon Hai Precision Industry Co., Ltd': 'HON_HAI',
        'Cheng Uei Precision Industry Co Ltd': 'CHENG_UEI',
        'Luxshare Precision Limited': 'LUXSHARE'
    }

    # Supplier aliases/variations mapping to their Oracle standard names
    SUPPLIER_ALIASES = {
        # Hon Hai / Foxconn variations
        'FOXCONN': 'Hon Hai Precision Industry Co., Ltd',
        'FXN': 'Hon Hai Precision Industry Co., Ltd',
        'HON HAI': 'Hon Hai Precision Industry Co., Ltd',
        'HONHAI': 'Hon Hai Precision Industry Co., Ltd',
        'FOXCONN TECHNOLOGY': 'Hon Hai Precision Industry Co., Ltd',
        
        # Cheng Uei / Foxlink variations
        'FOXLINK': 'Cheng Uei Precision Industry Co Ltd',
        'CHENG UEI': 'Cheng Uei Precision Industry Co Ltd',
        'FXL': 'Cheng Uei Precision Industry Co Ltd',
        
        # Luxshare variations
        'LUXSHARE-ICT': 'Luxshare Precision Limited',
        'LUXSHARE PRECISION': 'Luxshare Precision Limited',
        'LUX': 'Luxshare Precision Limited'
    }

    # Standard supplier site mappings
    STANDARD_SITES = {
        'New Taipei City': 'TAIPEI_CITY',
        'New Taipei-CAD': 'TAIPEI_CAD',
        'Taipei Hsien': 'TAIPEI_HSIEN',
        'Main-Hong Kong': 'HK_MAIN'
    }

    # Site aliases mapping to standard sites
    SITE_ALIASES = {
        'TAIPEI': 'New Taipei City',
        'NEW TAIPEI': 'New Taipei City',
        'TAIPEI CAD': 'New Taipei-CAD',
        'HK': 'Main-Hong Kong',
        'HONG KONG': 'Main-Hong Kong',
        'HONGKONG': 'Main-Hong Kong',
        'TAIPEI HSIEN': 'Taipei Hsien',
        'HSIEN': 'Taipei Hsien'
    }

    def __init__(self):
        """Initialize the supplier lookup."""
        super().__init__()
        self._supplier_reverse = {v: k for k, v in self.STANDARD_SUPPLIERS.items()}
        self._site_reverse = {v: k for k, v in self.STANDARD_SITES.items()}
        
        # Create normalized lookup maps for case-insensitive matching
        self._normalized_suppliers = {
            self._normalize_text(k): v 
            for k, v in {**self.STANDARD_SUPPLIERS, **self.SUPPLIER_ALIASES}.items()
        }
        
        self._normalized_sites = {
            self._normalize_text(k): v 
            for k, v in {**self.STANDARD_SITES, **self.SITE_ALIASES}.items()
        }

    def _normalize_text(self, text: str) -> str:
        """Normalize text for consistent lookup.
        
        Args:
            text: Text to normalize
            
        Returns:
            Normalized text
        """
        return text.upper().replace(' ', '').replace('-', '').replace('.', '').replace(',', '')

    def translate(self, value: str, is_site: bool = False) -> str:
        """Translate a supplier name or site to its standard form.
        
        Args:
            value: The supplier name or site to translate
            is_site: Whether this is a supplier site lookup
            
        Returns:
            The standardized supplier name or site
            
        Example:
            >>> lookup = SupplierLookup()
            >>> lookup.translate("FXN")
            'Hon Hai Precision Industry Co. Ltd'
            >>> lookup.translate("Foxconn")
            'Hon Hai Precision Industry Co. Ltd'
            >>> lookup.translate("HK", is_site=True)
            'Main-Hong Kong'
        """
        if not value:
            return value

        # Check cache first
        cache_key = f"{'site' if is_site else 'supplier'}:{value}"
        cached = self._get_cached(cache_key)
        if cached:
            return cached

        # Normalize input
        normalized = self._normalize_text(value)
        
        # Look up the standard form
        if is_site:
            # First check if it's already a standard site
            if value in self.STANDARD_SITES:
                standard = value
            else:
                # Look up in normalized sites
                standard = self._normalized_sites.get(normalized, value)
        else:
            # First check if it's already a standard supplier
            if value in self.STANDARD_SUPPLIERS:
                standard = value
            else:
                # Look up in normalized suppliers
                oracle_name = self._normalized_suppliers.get(normalized, value)
                standard = oracle_name if oracle_name in self.STANDARD_SUPPLIERS else value
        
        # Cache and return
        self._set_cached(cache_key, standard)
        return standard

    def get_code(self, value: str, is_site: bool = False) -> str:
        """Get the internal code for a supplier or site.
        
        Args:
            value: The supplier name or site to get code for
            is_site: Whether this is a supplier site lookup
            
        Returns:
            The internal code
            
        Example:
            >>> lookup = SupplierLookup()
            >>> lookup.get_code("FXN")
            'HON_HAI'
            >>> lookup.get_code("HK", is_site=True)
            'HK_MAIN'
        """
        # First translate to standard name
        standard = self.translate(value, is_site)
        
        # Then get code
        if is_site:
            return self.STANDARD_SITES.get(standard, standard)
        return self.STANDARD_SUPPLIERS.get(standard, standard)

    def validate(self, value: str, is_site: bool = False) -> bool:
        """Validate if a supplier name or site is known.
        
        Args:
            value: The supplier name or site to validate
            is_site: Whether this is a supplier site validation
            
        Returns:
            True if valid, False otherwise
            
        Example:
            >>> lookup = SupplierLookup()
            >>> lookup.validate("FXN")
            True
            >>> lookup.validate("Unknown Supplier")
            False
        """
        normalized = self._normalize_text(value)
        
        if is_site:
            return (value in self.STANDARD_SITES or 
                   value in self._site_reverse or 
                   normalized in self._normalized_sites)
        return (value in self.STANDARD_SUPPLIERS or 
                value in self._supplier_reverse or 
                normalized in self._normalized_suppliers)

    def get_display_name(self, code: str, is_site: bool = False) -> str:
        """Get the display name for a supplier or site code.
        
        Args:
            code: The supplier or site code
            is_site: Whether this is a supplier site lookup
            
        Returns:
            The display name
            
        Example:
            >>> lookup = SupplierLookup()
            >>> lookup.get_display_name("HON_HAI")
            'Hon Hai Precision Industry Co. Ltd'
        """
        if is_site:
            return self._site_reverse.get(code, code)
        return self._supplier_reverse.get(code, code)

    def get_all_variations(self, oracle_name: str) -> Set[str]:
        """Get all known variations of a supplier name.
        
        Args:
            oracle_name: The Oracle standard supplier name
            
        Returns:
            Set of all known variations
            
        Example:
            >>> lookup = SupplierLookup()
            >>> variations = lookup.get_all_variations("Hon Hai Precision Industry Co. Ltd")
            >>> 'FOXCONN' in variations
            True
            >>> 'FXN' in variations
            True
        """
        variations = {oracle_name}
        
        # Add the standard code
        if oracle_name in self.STANDARD_SUPPLIERS:
            variations.add(self.STANDARD_SUPPLIERS[oracle_name])
            
        # Add all aliases that map to this oracle name
        variations.update(
            alias for alias, std in self.SUPPLIER_ALIASES.items()
            if std == oracle_name
        )
        
        return variations
