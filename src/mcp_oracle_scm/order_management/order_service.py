"""Oracle Order Management Module"""

from collections import defaultdict
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, Union, List, Tuple
import aiohttp
import json
import asyncio
import csv
import io
import shutil
import re
from enum import Enum
import urllib.parse
from pydantic import BaseModel, Field
from mcp_oracle_scm.common.report_service import OracleReportService
from mcp_oracle_scm.common.auth import OracleAuth
from mcp_oracle_scm.config.environment import get_env_config
from mcp_oracle_scm.config.logger_config import LoggerConfig as Logger
from mcp_oracle_scm.order_management.order_utils import (
    process_order_report_row,
    format_order_response,
    format_order_summary,
)

# Get configuration
config = get_env_config()
API_PATH = config['api']['paths']['base_api']
DEFAULT_TIMEOUT = config['api']['timeout']['default']
CANCEL_TIMEOUT = config['api']['timeout']['cancel']

# Timeout configuration
TIMEOUT_CONFIG = {
    'total': DEFAULT_TIMEOUT,
    'connect': config['api']['timeout']['connect'],
    'sock_connect': config['api']['timeout']['sock_connect'],
    'sock_read': DEFAULT_TIMEOUT
}

class OrderSearchType(Enum):
    ORDER_NUMBER = "OrderNumber"
    SOURCE_ORDER_NUMBER = "SourceOrderNumber"
    PURCHASE_ORDER_NUMBER = "PurchaseOrderNumber"

class EnvironmentConfig(BaseModel):
    """Environment configuration for Oracle SCM."""
    base_url: str = Field(
        ...,
        description="Oracle SCM base URL",
        example="https://ehsg-dev1.fa.us6.oraclecloud.com"
    )

class OracleOrderManager:
    def __init__(self, config: EnvironmentConfig):
        self.base_url = config.base_url
        self.auth = OracleAuth()
        Logger.log("Initialized OracleOrderManager",
                  level="INFO",
                  base_url=self.base_url)

    def _format_elapsed_time(self, start_time: datetime) -> str:
        """Format elapsed time with milliseconds for logging."""
        elapsed = datetime.now() - start_time
        total_seconds = elapsed.total_seconds()
        milliseconds = int((total_seconds - int(total_seconds)) * 1000)
        return f"{int(total_seconds)}.{milliseconds:03d}"

    async def _get_auth_header(self) -> Dict[str, str]:
        """Get the authorization header using OAuth token."""
        access_token = self.auth.get_connection()
        if not access_token:
            raise Exception("Failed to get OAuth access token")
            
        return {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }

    async def _make_request(self, endpoint: str, params: Dict[str, Any] = None, timeout_seconds: int = DEFAULT_TIMEOUT) -> Dict[str, Any]:
        """Make an authenticated request to the Oracle SCM API."""
        try:
            base_url = f"{self.base_url}{API_PATH}/{endpoint}"
            headers = await self._get_auth_header()
            
            timeout = aiohttp.ClientTimeout(
                total=timeout_seconds,
                connect=TIMEOUT_CONFIG['connect'],
                sock_connect=TIMEOUT_CONFIG['sock_connect'],
                sock_read=timeout_seconds
            )
            
            Logger.log("Making API request",
                      level="INFO",
                      params=params)
            
            if params:
                processed_params = {}
                
                for k, v in params.items():
                    if v is not None and v != "":
                        processed_params[k] = v
                        
                Logger.log("Processed request parameters",
                          level="INFO",
                          processed_params=processed_params)
                
                query_parts = []
                for k, v in processed_params.items():
                    if k == 'q':
                        query_parts.append(f"{k}={urllib.parse.quote(str(v))}")
                    else:
                        encoded_value = urllib.parse.quote_plus(str(v))
                        query_parts.append(f"{k}={encoded_value}")
                    
                query_string = "&".join(query_parts)
                full_url = f"{base_url}?{query_string}"
                
                Logger.log("Final request URL constructed",
                          level="INFO",
                          url=full_url)
            else:
                full_url = base_url

            Logger.log("Making request",
                      level="INFO",
                      url=full_url)
            
            async with aiohttp.ClientSession() as session:
                async with session.get(full_url, headers=headers, timeout=timeout) as response:
                    if response.status == 200:
                        json_response = await response.json()
                        
                        if isinstance(json_response, dict):
                            json_response["request_url"] = full_url
                            
                        return json_response
                    else:
                        text = await response.text()
                        Logger.log("API request failed",
                                 level="ERROR",
                                 status=response.status,
                                 response=text)
                        raise Exception(f"API request failed with status {response.status}: {text}")
                    
        except Exception as e:
            Logger.log("Request error",
                      level="ERROR",
                      error=str(e))
            raise

    async def check_single_order_details(
        self,
        order_number: str
    ) -> Dict[str, Any]:
        """Get details for a specific order number."""
        try:
            search_value_str = str(order_number)
            start_time = datetime.now()
            Logger.log("Starting order search",
                      level="INFO",
                      search_value=search_value_str)
            
            search_types = ["OrderNumber", "CustomerPONumber", "SourceTransactionNumber"]
            
            for search_type in search_types:
                try:
                    Logger.log("Attempting search",
                             level="INFO",
                             search_type=search_type)
                    
                    query = f"{search_type}={search_value_str}"
                    
                    params = {
                        'q': query,
                        'expand': 'lines'
                    }
                    
                    base_url = f"{self.base_url}{API_PATH}/salesOrdersForOrderHub"
                    query_parts = []
                    for k, v in params.items():
                        if v:
                            encoded_value = urllib.parse.quote_plus(str(v))
                            query_parts.append(f"{k}={encoded_value}")
                    
                    api_url = f"{base_url}?{'&'.join(query_parts)}"
                    Logger.log("API request details",
                             level="INFO",
                             url=api_url,
                             params=params)
                    
                    response = await self._make_request('salesOrdersForOrderHub', params)
                    
                    if 'items' in response:
                        Logger.log("Response received",
                                 level="INFO",
                                 items_count=len(response['items']))
                    else:
                        Logger.log("Response contains no items",
                                 level="INFO",
                                 response=response)
                    
                    if 'items' in response and len(response['items']) > 0:
                        Logger.log("Order matches found",
                                 level="INFO",
                                 search_type=search_type,
                                 matches=len(response['items']))
                        
                        item = response['items'][0]
                        
                        order_lines = []
                        if 'lines' in item and len(item.get('lines', [])) > 0:
                            for line in item['lines']:
                                order_line = {
                                    "ProductNumber": line.get('ProductNumber'),
                                    "RequestFulfillmentOrganizationCode": line.get('RequestedFulfillmentOrganizationCode'),
                                    "StatusCode": line.get('StatusCode'),
                                    "OrderedQuantity": line.get('OrderedQuantity'),
                                    "LineNumber": line.get('LineNumber')
                                }
                                order_lines.append(order_line)
                        
                        simplified_order = {
                            "OrderNumber": item.get('OrderNumber'),
                            "SourceTransactionSystem": item.get('SourceTransactionSystem'),
                            "BusinessUnitName": item.get('BusinessUnitName'),
                            "TransactionOn": item.get('TransactionOn'),
                            "CustomerPONumber": item.get('CustomerPONumber'),
                            "TransactionType": item.get('TransactionType'),
                            "OrderLines": order_lines
                        }
                        
                        elapsed = (datetime.now() - start_time).total_seconds()
                        Logger.log("Search completed successfully",
                                 level="INFO",
                                 elapsed_seconds=elapsed)
                        
                        return {
                            "items": [simplified_order],
                            "search_details": {
                                "original_search_value": search_value_str,
                                "matched_in": search_type,
                                "search_time_seconds": elapsed,
                                "api_url": api_url
                            }
                        }
                    
                except Exception as e:
                    Logger.log("Search attempt failed",
                             level="ERROR",
                             search_type=search_type,
                             error=str(e))
                    continue
            
            elapsed = (datetime.now() - start_time).total_seconds()
            Logger.log("Search completed with no matches",
                      level="INFO",
                      elapsed_seconds=elapsed)
            
            return {
                "message": f"No matches found for '{search_value_str}' in any search type",
                "searched_types": search_types,
                "items": [],
                "search_details": {
                    "original_search_value": search_value_str,
                    "search_time_seconds": elapsed,
                    "tried_api_urls": [
                        f"{self.base_url}{API_PATH}/salesOrdersForOrderHub?q={t}={search_value_str}&expand=lines"
                        for t in search_types
                    ]
                }
            }
            
        except Exception as e:
            Logger.log("Order search failed",
                      level="ERROR",
                      error=str(e),
                      search_value=search_value_str)
            return {
                "error": str(e),
                "error_type": type(e).__name__,
                "search_value": search_value_str
            }

    async def get_order_count(
        self,
        offset_days: Optional[int] = None,
        p_bu: Optional[str] = None,
        p_source: Optional[str] = None,
        p_order_type: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get total order count by running Oracle BI Report."""
        try:
            start_time = datetime.now()
            Logger.log("Getting order count",
                      level="INFO",
                      offset_days=offset_days,
                      p_bu=p_bu,
                      p_source=p_source,
                      p_order_type=p_order_type)
            
            report_service = OracleReportService()
            report_path = "Custom/Square SCM Reports/Block MCP/OrderManagement/OrderCount_Rep.xdo"
            parameters = {}
            if offset_days is not None:
                parameters['offset_days'] = str(offset_days)
            if p_bu is not None and p_bu.strip():
                parameters['p_bu'] = p_bu.strip()
            if p_source is not None and p_source.strip():
                parameters['p_source'] = p_source.strip()
            if p_order_type is not None and p_order_type.strip():
                parameters['p_order_type'] = p_order_type.strip()
            
            report_file = await report_service.get_report_data(report_path, parameters)
            Logger.log("Report file received",
                      level="INFO",
                      file=report_file)
            
            order_counts = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(int))))
            customer_details = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(set))))
            
            try:
                with open(report_file, 'r', newline='', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        try:
                            bu = row.get('BUSINESS_UNIT', 'Unknown').strip('"')
                            source = row.get('SOURCE', 'Unknown').strip('"')
                            order_type = row.get('ORDER_TYPE', 'Unknown').strip('"')
                            count = int(row.get('ORDER_COUNT', '0').strip('"'))
                            customer = row.get('CUSTOMER', 'Unknown').strip('"')
                            
                            order_counts[bu][source][order_type][customer] += count
                            customer_details[bu][source][order_type][customer].add(customer)
                            
                        except Exception as e:
                            Logger.log("Error processing row",
                                     level="ERROR",
                                     error=str(e))
                            continue
                            
            except Exception as e:
                Logger.log("Error reading report file",
                          level="ERROR",
                          error=str(e),
                          file=report_file)
                raise
                
            summary = []
            for bu in sorted(order_counts.keys()):
                bu_summary = [f"For BU - {bu},"]
                for source in sorted(order_counts[bu].keys()):
                    source_summary = [f"   -for source '{source}',"]
                    for order_type in sorted(order_counts[bu][source].keys()):
                        type_summary = [f"           - for order type '{order_type}':"]
                        total_count = 0
                        customer_summary = []
                        for customer in sorted(order_counts[bu][source][order_type].keys()):
                            count = order_counts[bu][source][order_type][customer]
                            total_count += count
                            customer_summary.append(f"                * Customer: {customer} - Order count: {count}")
                        type_summary.append(f"             Total order count: {total_count}")
                        type_summary.extend(customer_summary)
                        source_summary.extend(type_summary)
                    bu_summary.extend(source_summary)
                summary.extend(bu_summary)
                summary.append("")
                
            elapsed = (datetime.now() - start_time).total_seconds()
            result = {
                "summary": "\n".join(summary),
                "order_counts": dict(order_counts),
                "parameters_used": {
                    "offset_days": offset_days,
                    "p_bu": p_bu,
                    "p_source": p_source,
                    "p_order_type": p_order_type
                },
                "execution_time": f"{elapsed:.1f} seconds"
            }
            
            Logger.log("Order count completed",
                      level="INFO",
                      elapsed_seconds=elapsed)
            return result
            
        except Exception as e:
            Logger.log("Error getting order count",
                      level="ERROR",
                      error=str(e),
                      parameters={
                          "offset_days": offset_days,
                          "p_bu": p_bu,
                          "p_source": p_source,
                          "p_order_type": p_order_type
                      })
            return {
                "error": str(e),
                "error_type": type(e).__name__,
                "parameters": {
                    "offset_days": offset_days,
                    "p_bu": p_bu,
                    "p_source": p_source,
                    "p_order_type": p_order_type
                }
            }

    async def get_open_orders(
        self,
        offset_days: Optional[int] = None,
        p_sku: Optional[str] = None,
        p_warehouse: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get open orders by running Oracle BI Report."""
        try:
            start_time = datetime.now()
            Logger.log("Getting open orders",
                      level="INFO",
                      offset_days=offset_days,
                      p_sku=p_sku,
                      p_warehouse=p_warehouse)
            
            # Initialize report service
            report_service = OracleReportService()
            
            # Define report path
            report_path = "Custom/Square SCM Reports/Block MCP/OrderManagement/BLK_OPEN_ORDERS.xdo"
            
            # Prepare parameters
            parameters = {}
            if offset_days is not None:
                parameters['offset_days'] = str(offset_days)
            if p_sku is not None and p_sku.strip():
                parameters['p_sku'] = p_sku.strip()
            if p_warehouse is not None and p_warehouse.strip():
                parameters['p_warehouse'] = p_warehouse.strip()

            # Clean up empty parameters
            parameters = {k: v for k, v in parameters.items() if v is not None and v != ""}
                
            Logger.log("Report parameters prepared",
                      level="INFO",
                      parameters=parameters)
            
            # Run report and get file path
            report_file = await report_service.get_report_data(report_path, parameters)
            Logger.log("Report file received",
                      level="INFO",
                      file=report_file)
            
            # Process the report file
            warehouse_summary = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
            total_orders = 0
            
            try:
                with open(report_file, 'r', newline='', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        try:
                            # Extract values
                            warehouse = row.get('ORGANIZATION_CODE', 'Unknown')
                            sku = row.get('ITEM_NUMBER', 'Unknown')
                            quantity = int(float(row.get('ORDERED_QTY', '0')))
                            order_number = row.get('ORDER_NUMBER', 'Unknown')
                            
                            # Update summary
                            warehouse_summary[warehouse][sku]["order_count"] += 1
                            warehouse_summary[warehouse][sku]["total_quantity"] += quantity
                            total_orders += 1
                            
                        except Exception as e:
                            Logger.log("Error processing row",
                                     level="ERROR",
                                     error=str(e))
                            continue
                            
            except Exception as e:
                Logger.log("Error reading report file",
                          level="ERROR",
                          error=str(e),
                          file=report_file)
                raise
                
            # Format summary text
            summary_text = []
            summary_text.append(f"Open Orders Summary (Last {offset_days if offset_days else 7} days):")
            
            for warehouse in sorted(warehouse_summary.keys()):
                summary_text.append(f"\nWarehouse: {warehouse}")
                for sku, data in sorted(warehouse_summary[warehouse].items()):
                    summary_text.append(
                        f"  SKU: {sku}"
                        f"\n    Order Count: {data['order_count']}"
                        f"\n    Total Quantity: {data['total_quantity']}"
                    )
            
            # Calculate execution time
            elapsed = (datetime.now() - start_time).total_seconds()
            
            # Prepare result
            result = {
                "summary": {
                    "total_orders": total_orders,
                    "warehouses": dict(warehouse_summary),
                    "summary_text": "\n".join(summary_text)
                },
                "parameters_used": {
                    "offset_days": offset_days,
                    "p_sku": p_sku,
                    "p_warehouse": p_warehouse
                },
                "execution_time": f"{elapsed:.1f} seconds"
            }
            
            Logger.log("Open orders summary completed",
                      level="INFO",
                      elapsed_seconds=elapsed,
                      total_orders=total_orders)
            return result
            
        except Exception as e:
            Logger.log("Error getting open orders",
                      level="ERROR",
                      error=str(e),
                      parameters={
                          "offset_days": offset_days,
                          "p_sku": p_sku,
                          "p_warehouse": p_warehouse
                      })
            return {
                "error": str(e),
                "error_type": type(e).__name__,
                "parameters": {
                    "offset_days": offset_days,
                    "p_sku": p_sku,
                    "p_warehouse": p_warehouse
                }
            }

    async def extract_order_line_details(
        self,
        offset_days: Optional[int] = None,
        p_sku: Optional[str] = None,
        p_warehouse: Optional[str] = None
    ) -> Dict[str, Any]:
        """Extract order line details by running Oracle BI Report."""
        try:
            start_time = datetime.now()
            Logger.log("Extracting order line details",
                      level="INFO",
                      offset_days=offset_days,
                      p_sku=p_sku,
                      p_warehouse=p_warehouse)
            
            report_service = OracleReportService()
            report_path = "Custom/Square SCM Reports/Block MCP/OrderManagement/OrderLineReport.xdo"
            
            
            parameters = {}
            if offset_days is not None:
                parameters['offset_days'] = str(offset_days)
            if p_sku is not None and p_sku.strip():
                parameters['p_sku'] = p_sku.strip()
            if p_warehouse is not None and p_warehouse.strip():
                parameters['p_warehouse'] = p_warehouse.strip()

            parameters = {k: v for k, v in parameters.items() if v is not None and v != ""}
            Logger.log("Report parameters prepared",
                      level="INFO",
                      parameters=parameters)
            
            report_file = await report_service.get_report_data(report_path, parameters)
            Logger.log("Report file received",
                      level="INFO",
                      file=report_file)
            
            total_rows = 0
            order_summary = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(int))))
            customer_details = defaultdict(lambda: defaultdict(lambda: defaultdict(set)))
            
            try:
                with open(report_file, 'r', newline='', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        total_rows += 1
                        warehouse = row.get('WAREHOUSE', 'Unknown')
                        status = row.get('LINE_STATUS', 'Unknown')
                        customer = row.get('CUSTOMER', 'Unknown')
                        
                        # Update order summary with customer information
                        order_summary[warehouse][status][customer]["count"] += 1
                        customer_details[warehouse][status][customer].add(customer)
                        
            except Exception as e:
                Logger.log("Error reading report file",
                          level="ERROR",
                          error=str(e),
                          file=report_file)
                raise
            
            # Format summary text with customer information
            summary_text = []
            summary_text.append(f"Order Line Details Summary:")
            
            for warehouse in sorted(order_summary.keys()):
                summary_text.append(f"\nWarehouse: {warehouse}")
                for status in sorted(order_summary[warehouse].keys()):
                    status_summary = []
                    total_status_count = 0
                    
                    for customer in sorted(order_summary[warehouse][status].keys()):
                        count = order_summary[warehouse][status][customer]["count"]
                        total_status_count += count
                        status_summary.append(f"    * Customer: {customer} - Order count: {count}")
                    
                    summary_text.append(f"  Status: {status}")
                    summary_text.append(f"  Total count: {total_status_count}")
                    summary_text.extend(status_summary)
            
            elapsed = (datetime.now() - start_time).total_seconds()
            
            result = {
                "status": "success",
                "message": f"Report extracted successfully with {total_rows} rows",
                "file_path": report_file,
                "summary": {
                    "total_rows": total_rows,
                    "warehouse_summary": dict(order_summary),
                    "summary_text": "\n".join(summary_text)
                },
                "parameters_used": {
                    "offset_days": offset_days,
                    "p_sku": p_sku,
                    "p_warehouse": p_warehouse
                },
                "execution_time": f"{elapsed:.1f} seconds"
            }
            
            Logger.log("Order line details extraction completed",
                      level="INFO",
                      elapsed_seconds=elapsed,
                      total_rows=total_rows)
            return result
            
        except Exception as e:
            Logger.log("Error extracting order line details",
                      level="ERROR",
                      error=str(e),
                      parameters={
                          "offset_days": offset_days,
                          "p_sku": p_sku,
                          "p_warehouse": p_warehouse
                      })
            return {
                "error": str(e),
                "error_type": type(e).__name__,
                "parameters": {
                    "offset_days": offset_days,
                    "p_sku": p_sku,
                    "p_warehouse": p_warehouse
                }
            }

    async def get_order_line_summary(
        self,
        offset_days: Optional[int] = None,
        p_sku: Optional[str] = None,
        p_warehouse: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get order line summary by running Oracle BI Report."""
        try:
            start_time = datetime.now()
            Logger.log("Getting order line summary",
                      level="INFO",
                      offset_days=offset_days,
                      p_sku=p_sku,
                      p_warehouse=p_warehouse)
            
            report_service = OracleReportService()
            report_path = "Custom/Square SCM Reports/Block MCP/OrderManagement/OrderLineSummaryReport.xdo"
            
            parameters = {}
            if offset_days is not None:
                parameters['offset_days'] = str(offset_days)
            if p_sku is not None and p_sku.strip():
                parameters['p_sku'] = p_sku.strip()
            if p_warehouse is not None and p_warehouse.strip():
                parameters['p_warehouse'] = p_warehouse.strip()

            parameters = {k: v for k, v in parameters.items() if v is not None and v != ""}
            Logger.log("Report parameters prepared",
                      level="INFO",
                      parameters=parameters)
            
            report_file = await report_service.get_report_data(report_path, parameters)
            Logger.log("Report file received",
                      level="INFO",
                      file=report_file)
            
            processed_data = []
            
            try:
                with open(report_file, 'r', newline='', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        try:
                            processed_row = self._process_order_line_row(row)
                            processed_data.append(processed_row)
                        except Exception as e:
                            Logger.log("Error processing row",
                                     level="ERROR",
                                     error=str(e))
                            continue
            except Exception as e:
                Logger.log("Error reading report file",
                          level="ERROR",
                          error=str(e),
                          file=report_file)
                raise
            
            summary = self._create_order_line_summary(processed_data, offset_days, p_sku, p_warehouse)
            
            result = {
                "summary": summary,
                "parameters_used": {
                    "offset_days": offset_days,
                    "p_sku": p_sku,
                    "p_warehouse": p_warehouse
                },
                "total_rows": len(processed_data)
            }
            
            elapsed = (datetime.now() - start_time).total_seconds()
            Logger.log("Order line summary completed",
                      level="INFO",
                      elapsed_seconds=elapsed,
                      total_rows=len(processed_data))
            
            return result
            
        except Exception as e:
            Logger.log("Error getting order line summary",
                      level="ERROR",
                      error=str(e),
                      parameters={
                          "offset_days": offset_days,
                          "p_sku": p_sku,
                          "p_warehouse": p_warehouse
                      })
            return {
                "error": str(e),
                "error_type": type(e).__name__,
                "parameters": {
                    "offset_days": offset_days,
                    "p_sku": p_sku,
                    "p_warehouse": p_warehouse
                }
            }
    
    def _process_order_line_row(self, row: Dict[str, str]) -> Dict[str, Any]:
        """Process a single row from the order line report into a standardized format."""
        try:
            processed_row = {}
            for key, value in row.items():
                if key:
                    clean_key = key.strip().strip('"')
                    clean_value = value.strip().strip('"') if value else None
                    processed_row[clean_key] = clean_value
            
            warehouse = processed_row.get('WAREHOUSE')
            sku = processed_row.get('SKU')
            status = processed_row.get('LINE_STATUS', '').upper()
            customer = processed_row.get('CUSTOMER', 'Unknown')  # Added customer field
            
            try:
                order_count = int(float(processed_row.get('ORDER_COUNT', '0')))
            except (ValueError, TypeError):
                Logger.log("Invalid order count value",
                          level="WARNING",
                          value=processed_row.get('ORDER_COUNT'),
                          fallback=0)
                order_count = 0
                
            try:
                quantity = int(float(processed_row.get('TOTAL_ORDERED_QUANTITY', '0')))
            except (ValueError, TypeError):
                Logger.log("Invalid quantity value",
                          level="WARNING",
                          value=processed_row.get('TOTAL_ORDERED_QUANTITY'),
                          fallback=0)
                quantity = 0
            
            standardized_row = {
                "warehouse": warehouse,
                "sku": sku,
                "status": status,
                "customer": customer,  # Added customer to standardized row
                "order_count": order_count,
                "total_quantity": quantity,
                "raw_data": processed_row
            }
            
            return standardized_row
            
        except Exception as e:
            Logger.log("Error processing order line row",
                      level="ERROR",
                      error=str(e),
                      row_data=row)
            raise
    
    def _create_order_line_summary(
        self, 
        data: List[Dict[str, Any]], 
        offset_days: int,
        p_sku: Optional[str] = None,
        p_warehouse: Optional[str] = None
    ) -> Dict[str, Any]:
        """Create a summary of the order line data."""
        try:
            warehouse_data = {}
            
            for row in data:
                warehouse = row.get("warehouse")
                status = row.get("status")
                sku = row.get("sku")
                customer = row.get("customer", "Unknown")  # Get customer from row
                order_count = row.get("order_count", 0)
                total_quantity = row.get("total_quantity", 0)
                
                if not warehouse or not status or not sku:
                    Logger.log("Missing required field in row",
                             level="WARNING",
                             warehouse=warehouse,
                             status=status,
                             sku=sku)
                    continue
                
                if warehouse not in warehouse_data:
                    warehouse_data[warehouse] = {}
                if status not in warehouse_data[warehouse]:
                    warehouse_data[warehouse][status] = {}
                if sku not in warehouse_data[warehouse][status]:
                    warehouse_data[warehouse][status][sku] = {
                        "customers": {},  # Initialize customers dict
                        "total_order_count": 0,
                        "total_quantity": 0
                    }
                
                # Update customer-specific counts
                if customer not in warehouse_data[warehouse][status][sku]["customers"]:
                    warehouse_data[warehouse][status][sku]["customers"][customer] = {
                        "order_count": 0,
                        "total_quantity": 0
                    }
                
                warehouse_data[warehouse][status][sku]["customers"][customer]["order_count"] += order_count
                warehouse_data[warehouse][status][sku]["customers"][customer]["total_quantity"] += total_quantity
                warehouse_data[warehouse][status][sku]["total_order_count"] += order_count
                warehouse_data[warehouse][status][sku]["total_quantity"] += total_quantity
            
            summary_text = []
            
            if p_sku:
                summary_text.append(f"In last {offset_days} days, for SKU = {p_sku}")
            else:
                summary_text.append(f"In last {offset_days} days:")
            
            if p_warehouse:
                warehouses_to_include = [p_warehouse] if p_warehouse in warehouse_data else []
                if not warehouses_to_include:
                    warehouses_to_include = [w for w in warehouse_data.keys() 
                                           if w.upper() == p_warehouse.upper()]
            else:
                warehouses_to_include = sorted(warehouse_data.keys())
            
            for warehouse in warehouses_to_include:
                summary_text.append(f"\nWarehouse {warehouse} =>")
                
                for status in sorted(warehouse_data[warehouse].keys()):
                    for sku, data in sorted(warehouse_data[warehouse][status].items()):
                        summary_text.append(
                            f"            {status} total order count = {data['total_order_count']} "
                            f"for SKU {sku} and total ordered_quantity = {data['total_quantity']}"
                        )
                        # Add customer breakdown
                        for customer, counts in sorted(data["customers"].items()):
                            summary_text.append(
                                f"                * Customer: {customer}"
                                f" - Order count: {counts['order_count']}"
                                f", Quantity: {counts['total_quantity']}"
                            )
            
            structured_summary = {
                "warehouses": warehouse_data,
                "summary_text": "\n".join(summary_text),
                "input_parameters": {
                    "offset_days": offset_days,
                    "p_sku": p_sku,
                    "p_warehouse": p_warehouse
                }
            }
            
            Logger.log("Order line summary created",
                      level="INFO",
                      warehouse_count=len(warehouse_data))
            
            return structured_summary
            
        except Exception as e:
            Logger.log("Error creating order line summary",
                      level="ERROR",
                      error=str(e))
            raise

    async def get_back_orders(
        self,
        p_from_sales_ord_date: Optional[str] = None,
        p_to_sales_ord_date: Optional[str] = None,
        p_warehouse: Optional[str] = None,
        p_item: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get back orders by running Oracle BI Report.
        
        This method runs a BI report to get back order data and summarizes the results.
        Only includes true back orders where:
        - back_ordered flag is 'YES'
        
        Args:
            p_from_sales_ord_date: From date in format 'MM-DD-YYYY'
            p_to_sales_ord_date: To date in format 'MM-DD-YYYY'
            p_warehouse: Optional warehouse code to filter by (e.g., 'CVU')
            p_item: Optional SKU number to filter by (e.g., 'A-SKU-0525')
        """
        try:
            start_time = datetime.now()
            Logger.log("Getting back orders",
                      level="INFO",
                      from_date=p_from_sales_ord_date,
                      to_date=p_to_sales_ord_date,
                      p_warehouse=p_warehouse,
                      p_item=p_item)
            
            # Initialize report service
            report_service = OracleReportService()
            
            # Define report path
            report_path = "Custom/Square SCM Reports/Block MCP/OrderManagement/SquareBackOrder_Rep.xdo"
            
            # Prepare parameters
            parameters = {}
            if p_from_sales_ord_date is not None and p_from_sales_ord_date.strip():
                parameters['p_from_sales_ord_date'] = p_from_sales_ord_date.strip()
            if p_to_sales_ord_date is not None and p_to_sales_ord_date.strip():
                parameters['p_to_sales_ord_date'] = p_to_sales_ord_date.strip()
            if p_warehouse is not None and p_warehouse.strip():
                parameters['p_warehouse'] = p_warehouse.strip()
            if p_item is not None and p_item.strip():
                parameters['p_item'] = p_item.strip()

            parameters = {k: v for k, v in parameters.items() if v is not None and v != ""}
            Logger.log("Report parameters prepared",
                      level="INFO",
                      parameters=parameters)
            
            # Run report
            report_file = await report_service.get_report_data(report_path, parameters)
            Logger.log("Report file received",
                      level="INFO",
                      file=report_file)
            
            # New data structures to track true back orders
            back_order_summary = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
            
            total_back_orders = 0
            
            try:
                with open(report_file, 'r', newline='', encoding='utf-8-sig') as f:  # Changed to utf-8-sig to handle BOM
                    reader = csv.DictReader(f, delimiter='|')
                    fieldnames = reader.fieldnames
                    Logger.log("DictReader Fields",
                             level="INFO",
                             fields=fieldnames)
                    
                    row_count = 0
                    for row in reader:
                        try:
                            row_count += 1
                            # Extract values
                            warehouse = row.get('SHIP_FROM_ORG', 'Unknown')
                            sku = row.get('SKU', 'Unknown')
                            item_desc = row.get('ITEMDESCRIPTION', '')
                            
                            # Debug - print raw row data for first row
                            if row_count == 1:
                                Logger.log("First Row Data",
                                         level="INFO",
                                         raw_row=dict(row))
                            
                            back_ordered_raw = row.get('BACK_ORDERED', 'NO').strip()
                            back_ordered = back_ordered_raw == 'YES'  # Direct comparison
                            
                            # Debug string comparison
                            if back_ordered:
                                Logger.log("Found backorder",
                                         level="INFO",
                                         raw_value=back_ordered_raw,
                                         order_number=order_number,
                                         sku=sku)
                            line_status = row.get('LINE_STATUS', '')
                            available_to_reserve = int(float(row.get('AVAILABLE_TO_RESERVE', '0')))
                            quantity = int(float(row.get('QTY', '0')))
                            order_number = row.get('ORDERNUMBER', 'Unknown')
                            
                            # Debug logging for every 10th row
                            if row_count % 10 == 0:
                                Logger.log("Processing row",
                                         level="INFO",
                                         row_number=row_count,
                                         back_ordered_raw=back_ordered_raw,
                                         back_ordered=back_ordered,
                                         order_number=order_number,
                                         sku=sku)
                            
                            # Additional debug for potential backorders
                            if back_ordered_raw.upper() == 'YES':
                                Logger.log("Found YES in BACK_ORDERED column",
                                         level="INFO",
                                         row_number=row_count,
                                         raw_value=back_ordered_raw,
                                         processed_value=back_ordered,
                                         order_number=order_number,
                                         sku=sku)
                            
                            # Determine if this is a true back order
                            is_true_back_order = (
                                back_ordered
                            )
                            
                            if is_true_back_order:
                                # Update back order summary
                                Logger.log("Processing true backorder",
                                         level="INFO",
                                         row_number=row_count,
                                         order_number=order_number,
                                         sku=sku,
                                         quantity=quantity)
                                back_order_summary[warehouse][sku]["order_count"] += 1
                                back_order_summary[warehouse][sku]["total_quantity"] += quantity
                                back_order_summary[warehouse][sku]["description"] = item_desc
                                total_back_orders += 1
                            
                        except Exception as e:
                            Logger.log("Error processing row",
                                     level="ERROR",
                                     error=str(e))
                            continue
                            
            except Exception as e:
                Logger.log("Error reading report file",
                          level="ERROR",
                          error=str(e),
                          file=report_file)
                raise
                
            # Format summary text
            summary_text = []
            summary_text.append(f"Back Orders Summary:")
            if p_from_sales_ord_date and p_to_sales_ord_date:
                summary_text.append(f"Date Range: {p_from_sales_ord_date} to {p_to_sales_ord_date}")
            
            # Add true back orders section
            summary_text.append("\n=== TRUE BACK ORDERS ===")
            summary_text.append(f"Total True Back Orders: {total_back_orders}")
            for warehouse in sorted(back_order_summary.keys()):
                summary_text.append(f"\nWarehouse: {warehouse}")
                for sku, data in sorted(back_order_summary[warehouse].items()):
                    summary_text.append(
                        f"  SKU: {sku}"
                        f"\n    Description: {data['description']}"
                        f"\n    Back Order Count: {data['order_count']}"
                        f"\n    Total Back Ordered Quantity: {data['total_quantity']}"
                    )
            
            # Calculate execution time
            elapsed = (datetime.now() - start_time).total_seconds()
            
            # Prepare result
            result = {
                "summary": {
                    "total_back_orders": total_back_orders,
                    "back_orders": dict(back_order_summary),
                    "summary_text": "\n".join(summary_text)
                },
                "parameters_used": {
                    "p_from_sales_ord_date": p_from_sales_ord_date,
                    "p_to_sales_ord_date": p_to_sales_ord_date,
                    "p_warehouse": p_warehouse,
                    "p_item": p_item
                },
                "execution_time": f"{elapsed:.1f} seconds"
            }
            
            Logger.log("Back orders summary completed",
                      level="INFO",
                      elapsed_seconds=elapsed,
                      total_back_orders=total_back_orders)
            return result
            
        except Exception as e:
            Logger.log("Error getting back orders",
                      level="ERROR",
                      error=str(e),
                      parameters={
                          "p_from_sales_ord_date": p_from_sales_ord_date,
                          "p_to_sales_ord_date": p_to_sales_ord_date,
                          "p_warehouse": p_warehouse,
                          "p_item": p_item
                      })
            return {
                "error": str(e),
                "error_type": type(e).__name__,
                "parameters": {
                    "p_from_sales_ord_date": p_from_sales_ord_date,
                    "p_to_sales_ord_date": p_to_sales_ord_date,
                    "p_warehouse": p_warehouse,
                    "p_item": p_item
                }
            }

def get_oracle_om() -> OracleOrderManager:
    """Get configured Oracle Order Manager client."""
    try:
        env_config = get_env_config()
        config = EnvironmentConfig(
            base_url=env_config['base_url']
        )
        return OracleOrderManager(config)
    except Exception as e:
        Logger.log("Failed to initialize Oracle Order Manager",
                  level="ERROR",
                  error=str(e))
        raise Exception(
            "Failed to initialize Oracle Order Manager. Please check environment configuration."
        ) from e