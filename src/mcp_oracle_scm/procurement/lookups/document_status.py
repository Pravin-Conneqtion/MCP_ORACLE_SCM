"""Document status lookup functionality."""

from typing import Dict
from .base import BaseLookup

class DocumentStatusLookup(BaseLookup):
    """Lookup implementation for document statuses."""

    # Standard status mappings
    STANDARD_STATUSES = {
        'INCOMPLETE': 'incomplete',
        'IN PROCESS': 'in_process',
        'APPROVED': 'approved',
        'REJECTED': 'rejected',
        'CLOSED': 'closed',
        'CANCELLED': 'cancelled',
        'ON HOLD': 'on_hold',
        'REQUIRES REAPPROVAL': 'requires_reapproval',
        'PENDING APPROVAL': 'pending_approval'
    }

    def __init__(self):
        """Initialize the document status lookup."""
        super().__init__()
        self._reverse_mapping = {v: k for k, v in self.STANDARD_STATUSES.items()}

    def translate(self, value: str) -> str:
        """Translate a document status to its standard form.
        
        Args:
            value: The status to translate
            
        Returns:
            The standardized status
        """
        # Check cache first
        cached = self._get_cached(value)
        if cached:
            return cached

        # Normalize input
        normalized = value.upper()
        
        # Look up the standard form
        standard = self.STANDARD_STATUSES.get(normalized, value)
        
        # Cache and return
        self._set_cached(value, standard)
        return standard

    def validate(self, value: str) -> bool:
        """Validate if a status is known.
        
        Args:
            value: The status to validate
            
        Returns:
            True if valid, False otherwise
        """
        normalized = value.upper()
        return normalized in self.STANDARD_STATUSES or value in self._reverse_mapping

    def get_display_name(self, code: str) -> str:
        """Get the display name for a status code.
        
        Args:
            code: The status code
            
        Returns:
            The display name
        """
        return self._reverse_mapping.get(code, code)
