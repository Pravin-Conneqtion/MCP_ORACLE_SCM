"""Oracle Item Management Module"""

from datetime import datetime
from typing import Optional, Dict, Any, Union, List
import os
import json
from mcp_oracle_scm.common.report_service import OracleReportService
from mcp_oracle_scm.config.environment import get_env_config
from mcp_oracle_scm.config.logger_config import LoggerConfig as Logger

# Field mapping from Oracle BI Report to standardized names
FIELD_MAPPING = {
    'CATEGORY_NAME': 'Item Category',
    'ITEM_NUMBER': 'Item Number/SKU/Product',
    'ITEM_DESCRIPTION': 'Description',
    'ORGANIZATION_CODE': 'Organization/Warehouse',
    'CREATION_DATE': 'Item Creation Date',
    'CREATED_BY': 'Created By',
    'LAST_UPDATE_DATE': 'Item Update Date',
    'LAST_UPDATED_BY': 'Updated by',
    'RING_FENCING_ENABLED_FLAG': 'Ring Fencing Enabled',
    'SKU_SHARING_COUNTRY': 'SKU Sharing Country',
    'SKU_SHARING_WAREHOUSE': 'SKU Sharing Warehouse',
    'ITEM_EFF': 'D2C Enabled',  # Special handling for "SKU Sharing" value
    'SKU_PRICE': 'SKU Price'
}

class ItemService:
    """Service class for retrieving and managing item details from Oracle."""

    REPORT_PATH = "/Custom/Square SCM Reports/Block MCP/ProductManagement/PIM Item details Report.xdo"

    def __init__(self):
        """Initialize the ItemService with Oracle Reports Service."""
        self.report_service = OracleReportService()
        Logger.log("ItemService initialized",
                  level="INFO")

    async def lookup_item_details(
        self,
        p_item_number: Optional[str],
        p_org: Optional[str],
        p_category: Optional[str],
        offset_days: Optional[int],
        p_d2c: Optional[str]
    ) -> Dict[str, Any]:
        """Look up item details using the Oracle BI Report.
        
        Args:
            p_item_number: Optional item number to filter by
            p_org: Optional organization/warehouse code to filter by
            p_category: Optional item category to filter by
            offset_days: Optional number of days to offset the search
            p_d2c: Optional filter for D2C enabled items ('Y' or 'N')
        """
        try:
            start_time = datetime.now()
            Logger.log("Looking up item details",
                      level="INFO",
                      p_item_number=p_item_number,
                      p_org=p_org,
                      p_category=p_category,
                      offset_days=offset_days,
                      p_d2c=p_d2c)
            
            # Initialize report service
            Logger.log("Using report path",
                      level="INFO",
                      path=self.REPORT_PATH)
            
            # Prepare parameters - only include non-None parameters
            parameters = {}
            if p_item_number is not None:
                parameters['p_item_number'] = p_item_number
            if p_org is not None:
                parameters['p_org'] = p_org
            if p_category is not None:
                parameters['p_category'] = p_category
            if offset_days is not None:
                parameters['offset_days'] = offset_days
            if p_d2c is not None:
                if p_d2c not in ['Y', 'N']:
                    Logger.log("Invalid p_d2c value",
                             level="ERROR",
                             value=p_d2c)
                    raise ValueError("p_d2c must be either 'Y' or 'N'")
                parameters['p_d2c'] = p_d2c
                
            Logger.log("Report parameters prepared",
                      level="INFO",
                      parameters=parameters)
                
            # Run report and get file path
            report_file = await self.report_service.get_report_data(self.REPORT_PATH, parameters)
            Logger.log("Processing item details report file",
                      level="INFO",
                      file=report_file)
            
            # Process the report file
            items = []
            raw_data = {"headers": None, "rows": []}
            
            try:
                # First check if file exists and is readable
                if not os.path.exists(report_file):
                    Logger.log("Report file not found",
                             level="ERROR",
                             file=report_file)
                    raise FileNotFoundError(f"Report file not found: {report_file}")
                
                # Check file size
                file_size = os.path.getsize(report_file)
                Logger.log("Report file size",
                          level="DEBUG",
                          size_bytes=file_size)
                
                # Read the entire file content for inspection
                with open(report_file, 'r', encoding='utf-8') as f:
                    content = f.read().strip()
                    
                    if not content:
                        Logger.log("Empty file received from report",
                                 level="ERROR")
                        raise ValueError("Empty file received from report")
                    
                    # Save raw data for debugging
                    debug_file = report_file + ".debug"
                    with open(debug_file, 'w') as df:
                        df.write(f"Original Content:\n{content}\n\n")
                
                # Process the file line by line
                with open(report_file, 'r', encoding='utf-8') as f:
                    lines = f.readlines()
                    
                    if not lines:
                        Logger.log("No data in report file",
                                 level="ERROR")
                        raise ValueError("No data in report file")
                    
                    # Get and process headers
                    headers = [h.strip().strip('"') for h in lines[0].strip().split(',')]
                    raw_data["headers"] = headers
                    Logger.log("CSV headers processed",
                             level="DEBUG",
                             headers=headers)
                    
                    # Process each line
                    for line in lines[1:]:
                        if line.strip():  # Skip empty lines
                            # Split line and clean values
                            values = [v.strip().strip('"') for v in line.strip().split(',')]
                            raw_data["rows"].append(values)
                            
                            # Create row dict
                            row = dict(zip(headers, values))
                            Logger.log("Processing row",
                                     level="DEBUG",
                                     row=row)
                            
                            try:
                                # Process row
                                item = self._process_item_row(row)
                                if item:  # Only add non-None items
                                    items.append(item)
                            except Exception as e:
                                Logger.log("Error processing row",
                                         level="ERROR",
                                         error=str(e),
                                         row=row)
                                continue
                
                # Save raw data to debug file
                with open(debug_file, 'a') as df:
                    df.write("\nProcessed Data:\n")
                    json.dump(raw_data, df, indent=2)
                    
                # Filter items by D2C status if p_d2c parameter is provided
                if p_d2c is not None:
                    original_count = len(items)
                    d2c_value = True if p_d2c == 'Y' else False
                    items = [item for item in items if item.get('D2C Enabled') == d2c_value]
                    Logger.log("Filtered items by D2C status",
                             level="INFO",
                             d2c_value=d2c_value,
                             original_count=original_count,
                             filtered_count=len(items))
                    
            except Exception as e:
                Logger.log("Error reading item details file",
                          level="ERROR",
                          error=str(e))
                raise
            
            # Calculate summary statistics
            total_items = len(items)
            
            # Group items by category and warehouse
            grouped_items = self._group_items(items)
            
            elapsed = (datetime.now() - start_time).total_seconds()
            Logger.log("Item details lookup completed",
                      level="INFO",
                      elapsed_seconds=elapsed,
                      total_items=total_items)
            
            # Create response with summary and details
            return {
                "total_items": total_items,
                "parameters_used": {
                    k: v for k, v in {
                        "p_item_number": p_item_number,
                        "p_org": p_org,
                        "p_category": p_category,
                        "offset_days": offset_days,
                        "p_d2c": p_d2c
                    }.items() if v is not None
                },
                "grouped_items": grouped_items,
                "items": items,
                "debug_file": report_file + ".debug"  # Include debug file path in response
            }
            
        except Exception as e:
            Logger.log("Error looking up item details",
                      level="ERROR",
                      error=str(e),
                      parameters={
                          "p_item_number": p_item_number,
                          "p_org": p_org,
                          "p_category": p_category,
                          "offset_days": offset_days,
                          "p_d2c": p_d2c
                      })
            return {
                "error": str(e),
                "error_type": type(e).__name__,
                "parameters": {
                    k: v for k, v in {
                        "p_item_number": p_item_number,
                        "p_org": p_org,
                        "p_category": p_category,
                        "offset_days": offset_days,
                        "p_d2c": p_d2c
                    }.items() if v is not None
                }
            }
    
    def _process_item_row(self, row: Dict[str, str]) -> Optional[Dict[str, Any]]:
        """Process a single item row from the report."""
        Logger.log("Processing row data",
                  level="DEBUG",
                  row=row)
        
        # Skip empty rows
        if not any(row.values()):
            return None
        
        # Create a new dictionary with mapped field names
        processed_row = {}
        
        # Process each field according to the mapping
        for oracle_field, display_field in FIELD_MAPPING.items():
            # Get the value, handling case where field might not exist
            value = row.get(oracle_field, '')
            
            # Strip any quotes and whitespace
            if isinstance(value, str):
                value = value.strip().strip('"')
            
            # Convert empty strings to None
            if value == "":
                processed_row[display_field] = None
                continue
            
            # Special handling for D2C enabled
            if oracle_field == 'ITEM_EFF':
                processed_row[display_field] = (value == 'SKU Sharing')
                continue
                
            # Convert boolean fields
            if oracle_field == 'RING_FENCING_ENABLED_FLAG':
                processed_row[display_field] = (value == 'Y')
                continue
                
            # Convert numeric fields
            if oracle_field == 'SKU_PRICE':
                try:
                    processed_row[display_field] = float(value) if value else None
                except (ValueError, TypeError):
                    Logger.log("Invalid SKU price value",
                             level="WARNING",
                             value=value)
                    processed_row[display_field] = None
                continue
            
            # Default handling
            processed_row[display_field] = value
        
        Logger.log("Processed row",
                  level="DEBUG",
                  processed=processed_row)
        return processed_row
    
    def _group_items(self, items: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Group items by category and warehouse."""
        by_category = {}
        by_warehouse = {}
        d2c_enabled = []
        ring_fenced = []
        
        try:
            # Group by category
            for item in items:
                category = item.get('Item Category')
                if category:
                    if category not in by_category:
                        by_category[category] = {
                            'total_items': 0,
                            'items': []
                        }
                    
                    by_category[category]['total_items'] += 1
                    by_category[category]['items'].append(item)
            
            # Group by warehouse
            for item in items:
                warehouse = item.get('Organization/Warehouse')
                if warehouse:
                    if warehouse not in by_warehouse:
                        by_warehouse[warehouse] = {
                            'total_items': 0,
                            'items': []
                        }
                    
                    by_warehouse[warehouse]['total_items'] += 1
                    by_warehouse[warehouse]['items'].append(item)
                    
                # Track D2C enabled items
                if item.get('D2C Enabled'):
                    d2c_enabled.append(item)
                    
                # Track ring-fenced items
                if item.get('Ring Fencing Enabled'):
                    ring_fenced.append(item)

            Logger.log("Items grouped successfully",
                      level="INFO",
                      category_count=len(by_category),
                      warehouse_count=len(by_warehouse),
                      d2c_enabled_count=len(d2c_enabled),
                      ring_fenced_count=len(ring_fenced))
            
            return {
                'by_category': by_category,
                'by_warehouse': by_warehouse,
                'special_configurations': {
                    'd2c_enabled': d2c_enabled,
                    'ring_fenced': ring_fenced
                }
            }
            
        except Exception as e:
            Logger.log("Error grouping items",
                      level="ERROR",
                      error=str(e))
            raise

def get_item_service() -> ItemService:
    """Get configured Item Service client."""
    try:
        return ItemService()
    except Exception as e:
        Logger.log("Failed to initialize Item Service",
                  level="ERROR",
                  error=str(e))
        raise Exception(
            "Failed to initialize Item Service. Please check environment configuration."
        ) from e
