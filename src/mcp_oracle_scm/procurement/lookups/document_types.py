"""Document type lookup functionality for procurement documents."""

from typing import Dict, Set, Optional
from .base import BaseLookup

class DocumentTypeLookup(BaseLookup):
    """Lookup implementation for procurement document types."""

    # Standard document type mappings
    STANDARD_TYPES = {
        'Purchase Order': 'PO',
        'Purchase Requisition': 'REQ',
        'Request for Quote': 'RFQ',
        'Blanket Purchase Agreement': 'BPA',
        'Contract Purchase Agreement': 'CPA',
        'Standard Purchase Order': 'STANDARD_PO',
        'Planned Purchase Order': 'PLANNED_PO',
        'Contract': 'CONTRACT',
        'Quote': 'QUOTE',
        'Bid': 'BID',
        'Agreement': 'AGREEMENT'
    }

    # Document type aliases and variations
    TYPE_ALIASES = {
        # Purchase Order variations
        'PO': 'Purchase Order',
        'P.O.': 'Purchase Order',
        'P/O': 'Purchase Order',
        'PURCH ORDER': 'Purchase Order',
        'PURCHASE ODR': 'Purchase Order',
        
        # Purchase Requisition variations
        'PR': 'Purchase Requisition',
        'PREQ': 'Purchase Requisition',
        'P.R.': 'Purchase Requisition',
        'P/R': 'Purchase Requisition',
        'REQ': 'Purchase Requisition',
        'REQUISITION': 'Purchase Requisition',
        
        # RFQ variations
        'REQUEST FOR QUOTATION': 'Request for Quote',
        'REQUEST FOR QUOTES': 'Request for Quote',
        'REQUEST FOR QUOTATIONS': 'Request for Quote',
        'RFQ': 'Request for Quote',
        'R.F.Q.': 'Request for Quote',
        
        # BPA variations
        'BLANKET AGREEMENT': 'Blanket Purchase Agreement',
        'BLANKET PO': 'Blanket Purchase Agreement',
        'BPA': 'Blanket Purchase Agreement',
        'B.P.A.': 'Blanket Purchase Agreement',
        
        # CPA variations
        'CONTRACT AGREEMENT': 'Contract Purchase Agreement',
        'CONTRACT PO': 'Contract Purchase Agreement',
        'CPA': 'Contract Purchase Agreement',
        'C.P.A.': 'Contract Purchase Agreement',
        
        # Standard PO variations
        'STD PO': 'Standard Purchase Order',
        'STANDARD PO': 'Standard Purchase Order',
        'STANDARD ORDER': 'Standard Purchase Order',
        
        # Planned PO variations
        'PLANNED ORDER': 'Planned Purchase Order',
        'PLANNED PO': 'Planned Purchase Order',
        'PLAN PO': 'Planned Purchase Order'
    }

    # Document type categories
    TYPE_CATEGORIES = {
        'PO': ['Purchase Order', 'Standard Purchase Order', 'Planned Purchase Order'],
        'AGREEMENT': ['Blanket Purchase Agreement', 'Contract Purchase Agreement', 'Contract'],
        'REQUISITION': ['Purchase Requisition'],
        'QUOTE': ['Request for Quote', 'Quote', 'Bid']
    }

    def __init__(self):
        """Initialize the document type lookup."""
        super().__init__()
        self._type_reverse = {v: k for k, v in self.STANDARD_TYPES.items()}
        
        # Create normalized lookup maps for case-insensitive matching
        self._normalized_types = {
            self._normalize_text(k): v 
            for k, v in {**self.STANDARD_TYPES, **self.TYPE_ALIASES}.items()
        }

    def _normalize_text(self, text: str) -> str:
        """Normalize text for consistent lookup.
        
        Args:
            text: Text to normalize
            
        Returns:
            Normalized text
        """
        return text.upper().replace(' ', '').replace('-', '').replace('.', '').replace('/', '')

    def translate(self, value: str) -> str:
        """Translate a document type to its standard form.
        
        Args:
            value: The document type to translate
            
        Returns:
            The standardized document type
            
        Example:
            >>> lookup = DocumentTypeLookup()
            >>> lookup.translate("PO")
            'Purchase Order'
            >>> lookup.translate("REQ")
            'Purchase Requisition'
        """
        if not value:
            return value

        # Check cache first
        cached = self._get_cached(value)
        if cached:
            return cached

        # Normalize input
        normalized = self._normalize_text(value)
        
        # Look up the standard form
        if value in self.STANDARD_TYPES:
            standard = value
        else:
            standard = self._normalized_types.get(normalized, value)
        
        # Cache and return
        self._set_cached(value, standard)
        return standard

    def get_code(self, value: str) -> str:
        """Get the internal code for a document type.
        
        Args:
            value: The document type to get code for
            
        Returns:
            The internal code
            
        Example:
            >>> lookup = DocumentTypeLookup()
            >>> lookup.get_code("Purchase Order")
            'PO'
        """
        # First translate to standard name
        standard = self.translate(value)
        return self.STANDARD_TYPES.get(standard, standard)

    def validate(self, value: str) -> bool:
        """Validate if a document type is known.
        
        Args:
            value: The document type to validate
            
        Returns:
            True if valid, False otherwise
            
        Example:
            >>> lookup = DocumentTypeLookup()
            >>> lookup.validate("PO")
            True
            >>> lookup.validate("Unknown Type")
            False
        """
        normalized = self._normalize_text(value)
        return (value in self.STANDARD_TYPES or 
                value in self._type_reverse or 
                normalized in self._normalized_types)

    def get_display_name(self, code: str) -> str:
        """Get the display name for a document type code.
        
        Args:
            code: The document type code
            
        Returns:
            The display name
            
        Example:
            >>> lookup = DocumentTypeLookup()
            >>> lookup.get_display_name("PO")
            'Purchase Order'
        """
        return self._type_reverse.get(code, code)

    def get_category(self, value: str) -> Optional[str]:
        """Get the category for a document type.
        
        Args:
            value: The document type to get category for
            
        Returns:
            The category name or None if not found
            
        Example:
            >>> lookup = DocumentTypeLookup()
            >>> lookup.get_category("Purchase Order")
            'PO'
            >>> lookup.get_category("Request for Quote")
            'QUOTE'
        """
        standard = self.translate(value)
        
        for category, types in self.TYPE_CATEGORIES.items():
            if standard in types:
                return category
        return None

    def get_all_variations(self, standard_type: str) -> Set[str]:
        """Get all known variations of a document type.
        
        Args:
            standard_type: The standard document type name
            
        Returns:
            Set of all known variations
            
        Example:
            >>> lookup = DocumentTypeLookup()
            >>> variations = lookup.get_all_variations("Purchase Order")
            >>> 'PO' in variations
            True
            >>> 'P.O.' in variations
            True
        """
        variations = {standard_type}
        
        # Add the standard code
        if standard_type in self.STANDARD_TYPES:
            variations.add(self.STANDARD_TYPES[standard_type])
            
        # Add all aliases that map to this standard type
        variations.update(
            alias for alias, std in self.TYPE_ALIASES.items()
            if std == standard_type
        )
        
        return variations

    def is_agreement(self, value: str) -> bool:
        """Check if a document type is an agreement type.
        
        Args:
            value: The document type to check
            
        Returns:
            True if it's an agreement type, False otherwise
            
        Example:
            >>> lookup = DocumentTypeLookup()
            >>> lookup.is_agreement("BPA")
            True
            >>> lookup.is_agreement("PO")
            False
        """
        category = self.get_category(value)
        return category == 'AGREEMENT' if category else False

    def is_purchase_order(self, value: str) -> bool:
        """Check if a document type is a purchase order type.
        
        Args:
            value: The document type to check
            
        Returns:
            True if it's a purchase order type, False otherwise
            
        Example:
            >>> lookup = DocumentTypeLookup()
            >>> lookup.is_purchase_order("PO")
            True
            >>> lookup.is_purchase_order("REQ")
            False
        """
        category = self.get_category(value)
        return category == 'PO' if category else False
