"""Constants for field mappings in procurement data."""

# Business Unit field mappings
BUSINESS_UNIT_FIELDS = {
    'PROCUREMENT_BU': 'procurement_bu',
    'REQUISITIONING_BU': 'requisitioning_bu',
}

# Category field mappings
CATEGORY_FIELDS = {
    'CATEGORY': 'category',
    'CATEGORY_NAME': 'category_name',
}

# Document status mappings
DOCUMENT_STATUS = {
    'INCOMPLETE': 'incomplete',
    'IN_PROCESS': 'in_process',
    'APPROVED': 'approved',
    'REJECTED': 'rejected',
    'CLOSED': 'closed',
}

# Location field mappings
LOCATION_FIELDS = {
    'SHIP_TO_LOCATION': 'ship_to_location',
    'BILL_TO_LOCATION': 'bill_to_location',
}

# Supplier field mappings
SUPPLIER_FIELDS = {
    'SUPPLIER': 'supplier',
    'SUPPLIER_SITE': 'supplier_site',
    'SUPPLIER_NUMBER': 'supplier_number',
}

# Data type mappings
FIELD_TYPES = {
    'string': str,
    'integer': int,
    'float': float,
    'date': 'date',
    'datetime': 'datetime',
}
