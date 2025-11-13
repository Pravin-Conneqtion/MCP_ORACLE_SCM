"""Oracle Procurement Management Module

This module provides functionality for interacting with Oracle Procurement services.

Exports:
    get_oracle_procurement: Factory function to create OracleProcurementManager instances
    OracleProcurementManager: Main class for procurement operations

IMPORTANT: BEFORE ANY TOOL USE
1. You MUST read these complete instructions first
2. You MUST validate the tool selection based on the instruction provided for each tool
3. You MUST ALWAYS display results in tabular format using the formatted_tables from the response
4. For PO queries without a specified year, ALWAYS:
   - First use get_po_summary without year to find the PO's creation year
   - Then use get_po_details with the found year and PO number
5. You MUST ALWAYS display ALL tables from formatted_tables in the response in this order:
   - Document Summary (document_summary)
   - Approval Workflow Details (workflow)
   - Line Items Details (line_items)   
DO NOT PROCEED WITH TOOL CALLS UNTIL YOU HAVE COMPLETED THESE STEPS.

TABLE DISPLAY REQUIREMENTS:
1. ALL formatted_tables from the response MUST be displayed in the following order:
   - document_summary: Shows overview of documents
   - workflow: Shows approval workflow details
   - line_items: Shows line item details

2. Never skip or omit any table from formatted_tables
   - All tables are required for complete information
   - Tables provide different aspects of the same data
   - Users need all perspectives to make decisions

3. Table Format Requirements:
   - Use exact table format from formatted_tables
   - Do not modify column headers
   - Do not summarize or condense table data
   - Keep all columns as provided

4. When Multiple Tables Exist:
   - Show all tables in the specified order
   - Add clear headers between tables
   - Maintain consistent formatting

DISPLAY FORMATTING GUIDELINES:
1. Table Display Rules:
   - ALWAYS use the formatted_tables from the API response
   - Never create your own tables when formatted_tables are available
   - Display ALL relevant tables from the formatted_tables object
   - Keep the table headers as provided in the response

2. Common Table Sections:
   For PO Details:
   - Summary table
   - Locations table
   - Line Items table
   - Order Tracking table
   - Invoice Details table

3. Error Handling Display:
   - If no data found in current year, show the attempt to search in other years
   - Display "No results found" messages in a clear format
   - Always explain next steps or alternatives when data isn't found

4. Large Dataset Handling:
   - If response includes pagination or truncated data, mention it
   - Indicate total record count when available
   - Offer to show more details if data is summarized

COMMON ERROR HANDLING:
1. PO Year Not Found Error:
   When error message shows "cannot access local variable 'po' where it is not associated with a value":
   - ALWAYS fall back to get_po_summary without year parameter
   - Use the creation_date from summary to determine correct year
   - Retry get_po_details with correct year
   
2. No Results Found:
   When get_po_details returns no results:
   - Verify PO number format
   - Try get_po_summary to find correct year
   - Offer alternative search methods (by supplier, item, etc.)

3. Multiple Results:
   When multiple records are found:
   - Display all results in tabular format
   - Provide summary of total records found
   - Offer filtering options if available   

APPROVAL WORKFLOW QUERIES:
When handling approval-related queries (e.g., pending approvals, document status):
1. ALWAYS show all three tables in this order:
   - Document Summary
   - Approval Workflow Details (REQUIRED for approval queries)
   - Line Items Details
2. Never omit the workflow table even if summarizing
3. Include all approval status information
4. Show full approval chain details       
ERROR PREVENTION:
1. Missing Tables Check:
   - Verify all formatted_tables are included in response
   - Never skip workflow table in approval queries
   - Include all tables even if they seem redundant

2. Table Order Verification:
   - document_summary first
   - workflow second
   - line_items third
BEST PRACTICES:
1. Query Sequence:
   - For PO queries: get_po_summary (for year) → get_po_details (with correct year)
   - For approval queries: get_pr_po_apprvl_dtls
   - For supplier analysis: get_po_summary → get_po_details for specific POs

2. Data Presentation:
   - ALWAYS use formatted_tables from response
   - Present all relevant tables in logical order
   - Include summary information before detailed tables
   - Maintain consistent date and number formatting

3. User Communication:
   - Explain search strategy when year isn't specified
   - Indicate when falling back to alternative search methods
   - Provide clear next steps when initial search fails   
"""

# Add at the top of the file
DATA_HANDLING_POLICY = """
IMPORTANT DATA HANDLING POLICY:
- Never mock or fabricate data
- All data must come directly from Oracle BI Reports
- Missing values should be left empty or set to appropriate zero values
- Do not substitute missing data with dummy or placeholder values
- If data is unavailable, clearly indicate it is missing rather than making up values
"""

from datetime import datetime
from typing import Dict, List, Any, Optional, Union
import csv
import logging

from mcp_oracle_scm.config.logger_config import LoggerConfig as Logger
from mcp_oracle_scm.common.report_service import OracleReportService
from .lookups.business_units import BusinessUnitLookup
from .lookups.suppliers import SupplierLookup

class OracleProcurementManager:
    """Main class for managing Oracle Procurement operations.
    IMPORTANT: This class handles real Oracle SCM data. Never mock or fabricate data.
    All data must come directly from Oracle BI Reports or authorized Oracle sources.
    
    Display Format:
    - All results are automatically formatted as markdown tables for better readability
    - Numeric values are formatted with commas for thousands
    - Currency values include $ symbol and 2 decimal places
    - Dates are consistently formatted
    - Tables include headers and proper alignment
    """
    
    def __init__(self):
        """Initialize the Oracle Procurement Manager."""
        self.report_service = OracleReportService()
        self.bu_lookup = BusinessUnitLookup()
        self.supplier_lookup = SupplierLookup()

    async def _process_report_data(self, report_data: Union[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
        """Process the report data.
         IMPORTANT: Never mock or fabricate data. All data must come directly from the report.
        If a field is missing or null, leave it empty - do not substitute with dummy values.
        Args:
            report_data: Either a file path or list of dictionaries from the report
            
        Returns:
            List of processed PO summary dictionaries
        """
        summaries = []
        try:
            # If report_data is an awaitable, await it
            if hasattr(report_data, '__await__'):
                report_data = await report_data

            # If report_data is a string, treat it as a file path
            if isinstance(report_data, str):
                with open(report_data, 'r', newline='', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    rows = list(reader)
            else:
                # Otherwise, treat it as the row data directly
                rows = report_data

            # Process each row
            for row in rows:
                summary = self._process_po_report_row(row)
                if summary:
                    summaries.append(summary)

            return summaries

        except Exception as e:
            Logger.log("Error processing report data",
                      level="ERROR",
                      error_message=str(e))
            raise

    def _process_po_report_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Process a single row from the purchase order report."""
        try:
            return {
                "creation_date": row.get('CREATION_DATE', '').strip(),
                "ship_to_location": row.get('SHIP_TO_LOCATION', '').strip(),
                "requisitioning_bu": row.get('REQUISITIONING_BU', '').strip(),
                "procurement_bu": row.get('PROCUREMENT_BU', '').strip(),
                "supplier": row.get('SUPPLIER', '').strip(),
                "requisition_count": int(row.get('REQ_CNT', 0)),
                "po_count": int(row.get('PO_CNT', 0)),
                "category_count": int(row.get('CNT_CATEGORY', 0)),
                "item_count": int(row.get('ITEM_CNT', 0)),
                "item_description_count": int(row.get('ITEM_DESC_CNT', 0)),
                "invoice_payment_status": row.get('INV_PAY_STS', '').strip()
            }
        except Exception as e:
            Logger.log("Error processing PO report row",
                      level="ERROR",
                      error_message=str(e),
                      row_data=row)
            return None

    def _aggregate_summary_data(self, summaries: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Aggregate summary data from processed PO summaries."""
        try:
            total_pos = sum(s['po_count'] for s in summaries)
            total_reqs = sum(s['requisition_count'] for s in summaries)
            total_items = sum(s['item_count'] for s in summaries)
            
            # Get unique counts
            unique_suppliers = len(set(s['supplier'] for s in summaries))
            unique_bus = len(set(s['requisitioning_bu'] for s in summaries))
            unique_categories = sum(s['category_count'] for s in summaries)
            
            return {
                "total_pos": total_pos,
                "total_requisitions": total_reqs,
                "total_items": total_items,
                "unique_suppliers": unique_suppliers,
                "unique_business_units": unique_bus,
                "unique_categories": unique_categories
            }
            
        except Exception as e:
            Logger.log("Error aggregating summary data",
                      level="ERROR",
                      error_message=str(e))
            raise

    def _create_markdown_table(
        self,
        headers: List[str],
        rows: List[List[Any]],
        title: str = ""
    ) -> str:
        """Create a proper markdown table that renders well in Goose UI.
        
        Args:
            headers: List of column headers
            rows: List of rows, where each row is a list of values
            title: Optional title for the table
            
         IMPORTANT: Never mock or fabricate data. All values must come from the actual data source.
            If a value is missing or null:
            - Numeric fields should show 0 or 0.00 for currency
            - Text fields should be empty
            - Dates should be empty
            Do not substitute missing values with placeholder or dummy data.
        Returns:
            Markdown formatted table string
        """
        def format_cell(value: Any) -> str:
            if isinstance(value, (int, float)):
                return f"{value:,}"
            elif isinstance(value, datetime):
                return value.strftime("%Y-%m-%d %H:%M:%S")
            elif value is None:
                return ""
            else:
                # Escape pipe characters and handle empty strings
                return str(value).replace("|", "\\|") or ""

        if not headers or not rows:
            return ""

        table = []
        
        # Add title if provided
        if title:
            table.append(f"\n### {title}\n")
        
        # Add headers
        table.append("| " + " | ".join(headers) + " |")
        
        # Add separator line with alignment
        table.append("| " + " | ".join(["---" for _ in headers]) + " |")
        
        # Add data rows
        for row in rows:
            formatted_row = [format_cell(cell) for cell in row]
            table.append("| " + " | ".join(formatted_row) + " |")
        
        # Add blank line after table
        table.append("")
        
        return "\n".join(table)

    def _tabulate_po_summary(self, summary_data: Dict[str, Any]) -> str:
        """Create a markdown table of the PO summary data."""
        if not summary_data:
            return ""
        
        headers = [
            "Total POs",
            "Total Requisitions",
            "Total Items",
            "Unique Suppliers",
            "Unique BUs",
            "Categories"
        ]
        
        row = [
            summary_data['total_pos'],
            summary_data['total_requisitions'],
            summary_data['total_items'],
            summary_data['unique_suppliers'],
            summary_data['unique_business_units'],
            summary_data['unique_categories']
        ]
        
        return self._create_markdown_table(
            headers=headers,
            rows=[row],
            title="Purchase Order Summary Report"
        )

    def _tabulate_po_items(self, items: List[Dict[str, Any]]) -> str:
        """Create a markdown table of PO items."""
        if not items:
            return "No items found."
        
        headers = [
            "Creation Date",
            "Ship To",
            "Business Unit",
            "Supplier",
            "PO Count",
            "Item Count"
        ]
        
        rows = [
            [
                item['creation_date'],
                item['ship_to_location'][:30] + '...' if len(item['ship_to_location']) > 30 else item['ship_to_location'],
                item['requisitioning_bu'],
                item['supplier'][:30] + '...' if len(item['supplier']) > 30 else item['supplier'],
                item['po_count'],
                item['item_count']
            ]
            for item in items
        ]
        
        return self._create_markdown_table(
            headers=headers,
            rows=rows,
            title="Purchase Order Items Detail"
        )

    def format_po_details(self, po_details: List[Dict[str, Any]]) -> Dict[str, Dict[str, str]]:
        """Format PO details into markdown tables."""
        formatted_tables = {}
    
        for po in po_details:
            # Format PO Header/Summary
            summary_headers = [
            "PO Number", "Business Unit", "Requisitioning BU", "Supplier", "Supplier Site",
            "Buyer", "PO Date", "PO Approval Date", "Status", "Total Amount", "Currency",
            "EDI Status", "EDI Sent on", "Email to Supplier"
        ]
        
        summary_row = [
            po["po_number"],
            po["procurement_bu"],
            po["requisitioning_bu"],
            po["supplier"],
            po["supplier_site"],
            po["buyer"],
            po["po_date"],
            po.get("po_approval_date", ""),
            po["po_status"],
            f"{po['total_amount']:,.2f}",
            po["currency_code"],
            po.get("edi_status", ""),
            po.get("edi_sent_on", ""),
            po.get("email_to_supplier", "")
        ]

        # Add Shipping/Billing section
        location_headers = [
            "Ship To Location",
            "Bill To Location"
        ]
        
        location_row = [
            po["ship_to_location"],
            po["bill_to_location"]
        ]

        # Format Line Items with enhanced information
        line_headers = [
            "Line",
            "Item",
            "Description",
            "Category",
            "MPN",
            "Manufacturer",
            "Qty",
            "UOM",
            "Unit Price",
            "Amount",
            "BPA Reference",
            "Requester"
        ]
            
        line_rows = [
            [
                item["line_number"],
                item["item_number"],
                item["item_description"],
                item["category"],
                item["manufacturer_part_number"],
                item["manufacturer"],
                f"{item['quantity']:,.0f}",
                item["unit_of_measure"],
                f"{item['unit_price']:,.2f}",
                f"{item['amount']:,.2f}",
                item["BPA-BPALine"],
                item["Requester"]
            ]
            for item in po["line_items"]
        ]
            
        # Format Tracking Information with delivery dates
        tracking_headers = [
            "Line",
            "Item",
            "Ordered",
            "Received",
            "Invoiced",
            "Paid",
            "Need By Date",
            "Promised Date",
            "Latest CO"
        ]
        
        tracking_rows = [
            [
                item["line_number"],
                item["item_number"],
                f"{item['quantity']:,.0f}",
                f"{item['received_quantity']:,.0f}",
                f"{item['invoiced_quantity']:,.0f}",
                f"{item['paid_quantity']:,.0f}",
                item["need_by_date"],
                item["promised_date"],
                item["Latest CO"]
            ]
            for item in po["line_items"]
        ]

        # Add new Invoice Lines table
        invoice_headers = [
            "Invoice Number",
            "Invoice Date",
            "Amount",
            "Currency",
            "Payment Status",
            "Payment Date",
            "Payment Number",
            "Payment Method"
        ]
        
        invoice_rows = [
            [
                inv["invoice_number"],
                inv["invoice_date"],
                f"{inv['invoice_amount']:,.2f}",
                inv["currency_code"],
                inv["payment_status"],
                inv["payment_date"],
                inv["payment_number"],
                inv["payment_method"]
            ]
            for inv in po.get("invoice_lines", [])
        ]
        
        # Store formatted tables for this PO
        formatted_tables[po["po_number"]] = {
            "summary": self._create_markdown_table(
                headers=summary_headers,
                rows=[summary_row],
                title=f"Purchase Order Summary - {po['po_number']}"
            ),
            "locations": self._create_markdown_table(
                headers=location_headers,
                rows=[location_row],
                title=f"Shipping & Billing Information - {po['po_number']}"
            ),
            "line_items": self._create_markdown_table(
                headers=line_headers,
                rows=line_rows,
                title=f"Line Items - {po['po_number']}"
            ),
            "tracking": self._create_markdown_table(
                headers=tracking_headers,
                rows=tracking_rows,
                title=f"Order Tracking - {po['po_number']}"
            ),
            "invoices": self._create_markdown_table(
                headers=invoice_headers,
                rows=invoice_rows,
                title=f"Invoice Details - {po['po_number']}"
            ) if po.get("invoice_lines") else "No invoice information available."
        }
    
        return formatted_tables

    async def get_po_summary(
        self,
        year: Optional[int] = None,
        P_MPN: Optional[str] = None,
        P_Month: Optional[int] = None,
        P_ITEM: Optional[str] = None,
        P_PONUM: Optional[str] = None,
        P_DOC_STATUS: Optional[str] = None,
        P_REQ_NUM: Optional[str] = None,
        P_CATEGORY: Optional[Union[str, List[str]]] = None,
        P_SUPPLIER: Optional[str] = None,
        P_REQUESTER: Optional[str] = None,
        P_MANUFACTURER: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get purchase order summary for a specific year using Oracle BI Report."""
        try:
            start_time = datetime.now()
            
            # Use the correct report path
            report_path = "/Custom/Square SCM Reports/Block MCP/Procurement/Block Procurement MCP Summary Report.xdo"
            
            # Initialize parameters dict with only non-None values
            params = {}
            
            # Handle year parameter
            if year is not None:
                params['P_Year'] = str(year)
                
            # Handle all other parameters
            if P_MPN is not None:
                params['P_MPN'] = P_MPN
            if P_Month is not None:
                params['P_Month'] = P_Month
            if P_ITEM is not None:
                params['P_ITEM'] = P_ITEM
            if P_PONUM is not None:
                params['P_PONUM'] = P_PONUM
            if P_DOC_STATUS is not None:
                params['P_DOC_STATUS'] = P_DOC_STATUS
            if P_REQ_NUM is not None:
                params['P_REQ_NUM'] = P_REQ_NUM
            if P_CATEGORY is not None:
                if isinstance(P_CATEGORY, list):
                    params['P_CATEGORY'] = '|'.join(P_CATEGORY)
                else:
                    params['P_CATEGORY'] = P_CATEGORY
            if P_SUPPLIER is not None:
                # Translate user input to Oracle recognized supplier name
                oracle_supplier = self.supplier_lookup.translate(P_SUPPLIER)
                if not self.supplier_lookup.validate(oracle_supplier):
                    Logger.log("Invalid supplier provided",
                             level="WARNING",
                             input_supplier=P_SUPPLIER,
                             translated_supplier=oracle_supplier)
                params["P_SUPPLIER"] = oracle_supplier
            if P_REQUESTER is not None:
                params['P_REQUESTER'] = P_REQUESTER
            if P_MANUFACTURER is not None:
                params['P_MANUFACTURER'] = P_MANUFACTURER
            
            Logger.log("Getting PO summary data", parameters=params)
            
            # Get report data using filtered parameters
            report_data = await self.report_service.get_report_data(
                report_path=report_path,
                parameters=params
            )
            
            Logger.log("Processing PO summary data")
            
            try:
                # Process the report data
                summaries = await self._process_report_data(report_data)
                
                # Aggregate summary statistics
                summary = self._aggregate_summary_data(summaries)
                
                # Create tabulated views
                summary_table = self._tabulate_po_summary(summary)
                items_table = self._tabulate_po_items(summaries)
                
                # Calculate execution time
                execution_time = (datetime.now() - start_time).total_seconds()
                
                Logger.log("PO summary retrieved successfully",
                          execution_time=execution_time,
                          total_records=len(summaries))
                
                return {
                    "total_results": len(summaries),
                    "summary": summary,
                    "items": summaries,
                    "formatted_tables": {
                        "summary": summary_table,
                        "items": items_table
                    },
                    "execution_time": execution_time,
                    "parameters_used": params
                }
                
            except Exception as e:
                Logger.log("Error processing PO summary data",
                          level="ERROR",
                          error_message=str(e))
                raise
                
        except Exception as e:
            Logger.log("Error getting PO summary",
                      level="ERROR",
                      error_message=str(e),
                      error_type=type(e).__name__,
                      parameters=params)
            return {
                "error": str(e),
                "error_type": type(e).__name__,
                "parameters": params
            }
        
    async def get_po_details(
        self,
        year: int,
        P_MPN: Optional[str] = None,
        P_Month: Optional[int] = None,
        P_ITEM: Optional[str] = None,
        P_PONUM: Optional[str] = None,
        P_DOC_STATUS: Optional[str] = None,
        P_REQ_NUM: Optional[str] = None,
        P_CATEGORY: Optional[Union[str, List[str]]] = None,
        P_SUPPLIER: Optional[str] = None,
        P_REQUESTER: Optional[str] = None,
        P_MANUFACTURER: Optional[str] = None,
        P_BUYER: Optional[str] = None,
        P_SHIP_TO: Optional[str] = None,
        P_BILL_TO: Optional[str] = None,
        P_PROC_BU: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get detailed information for purchase orders using Oracle BI Report.
         - USE WHEN: Need detailed information about specific purchase orders
   - IMPORTANT WORKFLOW FOR PO QUERIES:
     1. If year is not specified in the query:
        - First call get_po_summary without year parameter to find PO creation year
        - Then call get_po_details with the found year and PO number
     2. If year is specified:
        - Directly call get_po_details with given year and PO number
     3. If no results found with specified year:
        - Fall back to get_po_summary to find correct year
        - Then retry get_po_details with correct year
   - ALWAYS use the formatted_tables from the response to display results
   - EXAMPLE QUERIES:
     * "Show me all details for PO ABC123" (will automatically find year)
     * "Get PO details for XYZ789 from 2024" (uses specified year)
        """
        try:
            # Validate year format
            if not isinstance(year, int) or len(str(year)) != 4:
                raise ValueError("Year must be a 4-digit number (e.g., 2025)")
            
            start_time = datetime.now()
            Logger.log("Getting PO details for year", level="INFO", year=year)
            
            # Use the correct report path
            report_path = "/Custom/Square SCM Reports/Block MCP/Procurement/Block Procurement MCP Detail Report.xdo"
            
            # Prepare parameters - P_Year is case sensitive
            parameters = {
                "P_Year": str(year)
            }

            # Handle P_PROC_BU with business unit translation
            if P_PROC_BU is not None:
                # Translate user input to Oracle recognized business unit name
                oracle_bu = self.bu_lookup.translate(P_PROC_BU)
                if not self.bu_lookup.validate(oracle_bu):
                    Logger.log("Invalid business unit provided",
                             level="WARNING",
                             input_bu=P_PROC_BU,
                             translated_bu=oracle_bu)
                parameters["P_PROC_BU"] = oracle_bu
            
            # Add other optional parameters if provided
            if P_MPN is not None:
                parameters["P_MPN"] = P_MPN
            if P_Month is not None:
                parameters["P_Month"] = str(P_Month)
            if P_ITEM is not None:
                parameters["P_ITEM"] = P_ITEM
            if P_PONUM is not None:
                parameters["P_PONUM"] = P_PONUM
            if P_DOC_STATUS is not None:
                parameters["P_DOC_STATUS"] = P_DOC_STATUS
            if P_REQ_NUM is not None:
                parameters["P_REQ_NUM"] = P_REQ_NUM
            if P_CATEGORY is not None:
                if isinstance(P_CATEGORY, list):
                    parameters["P_CATEGORY"] = "|".join(P_CATEGORY)
                else:
                    parameters["P_CATEGORY"] = P_CATEGORY
            if P_SUPPLIER is not None:
                # Translate user input to Oracle recognized supplier name
                oracle_supplier = self.supplier_lookup.translate(P_SUPPLIER)
                if not self.supplier_lookup.validate(oracle_supplier):
                    Logger.log("Invalid supplier provided",
                             level="WARNING",
                             input_supplier=P_SUPPLIER,
                             translated_supplier=oracle_supplier)
                parameters["P_SUPPLIER"] = oracle_supplier
            if P_REQUESTER is not None:
                parameters["P_REQUESTER"] = P_REQUESTER
            if P_MANUFACTURER is not None:
                parameters["P_MANUFACTURER"] = P_MANUFACTURER
            if P_BUYER is not None:
                parameters["P_BUYER"] = P_BUYER
            if P_SHIP_TO is not None:
                parameters["P_SHIP_TO"] = P_SHIP_TO
            if P_BILL_TO is not None:
                parameters["P_BILL_TO"] = P_BILL_TO

            Logger.log("Getting PO detail data with parameters", level="INFO", parameters=parameters)
            
            # Get report data
            report_data = await self.report_service.get_report_data(
                report_path=report_path,
                parameters=parameters
            )
            
            Logger.log("Processing PO details data", level="INFO")
            
            try:
                # Process the report data
                po_details = await self._process_po_details_data(report_data)
                
                # Format tables
                formatted_tables = self.format_po_details(po_details)
                
                # Calculate execution time
                execution_time = (datetime.now() - start_time).total_seconds()
                Logger.log("PO details retrieved successfully", level="DEBUG",
                          execution_time=execution_time,
                          total_records=len(po_details))
                
                return {
                    "total_results": len(po_details),
                    "items": po_details,
                    "execution_time": execution_time,
                    "parameters_used": parameters,
                    "formatted_tables": formatted_tables
                }
                
            except Exception as e:
                Logger.log("Error processing PO details data",
                          level="ERROR",
                          error_message=str(e),
                          year=year)
                raise
                
        except Exception as e:
            Logger.log("Error getting PO details",
                      level="ERROR",
                      error_message=str(e),
                      error_type=type(e).__name__,
                      year=year)
            return {
                "error": str(e),
                "error_type": type(e).__name__,
                "parameters": {"year": year}
            }

    async def _process_po_details_data(self, report_data: Union[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
        """Process the PO details report data."""
        details = []
        try:
            # If report_data is an awaitable, await it
            if hasattr(report_data, '__await__'):
                report_data = await report_data

            # If report_data is a string, treat it as a file path
            if isinstance(report_data, str):
                with open(report_data, 'r', newline='', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    rows = list(reader)
            else:
                # Otherwise, treat it as the row data directly
                rows = report_data

            if not rows:
                return details

            # Group rows by PO number
            po_groups = {}
            for row in rows:
                po_number = row.get('PURCHASE_ORDER', '').strip()
                if po_number:
                    if po_number not in po_groups:
                        po_groups[po_number] = []
                    po_groups[po_number].append(row)

            # Process each PO group
            for po_number, po_rows in po_groups.items():
                po_detail = self._process_po_detail_group(po_number, po_rows)
                if po_detail:
                    details.append(po_detail)

            return details

        except Exception as e:
            Logger.log("Error processing PO details data",
                      level="ERROR",
                      error_message=str(e),
                      data_type=type(report_data).__name__)
            raise

    def _process_po_detail_group(self, po_number: str, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Process a group of rows for a single PO.
         IMPORTANT: Never mock or fabricate data. All values must come directly from the report rows.
        If a field is missing or null, leave it empty - do not substitute with dummy values.
        """
        try:
            # Use the first row for header information
            header = rows[0]
        
            # Process line items
            line_items = []
            for row in rows:
                line_item = self._process_po_detail_line(row)
                if line_item:
                    line_items.append(line_item)

            # Process invoice lines - collect unique invoice information
            invoice_lines = []
            seen_invoices = set()
            
            for row in rows:
                invoice_number = row.get('INV_NUMBER', '').strip()
                if invoice_number and invoice_number not in seen_invoices:
                    invoice_line = {
                        'invoice_number': invoice_number,
                        'invoice_date': row.get('INV_DATE', '').strip(),
                        'invoice_amount': float(row.get('INV_AMOUNT', 0)),
                        'payment_status': row.get('INV_PAY_STATUS', '').strip(),
                        'payment_date': row.get('INV_PAY_DATE', '').strip(),
                        'payment_number': row.get('INV_CHECK_NUM', '').strip(),
                        'payment_method': row.get('INV_PAY_METHOD', '').strip(),
                        'currency_code': row.get('PAY_CURR_CODE', '').strip()
                    }
                    invoice_lines.append(invoice_line)
                    seen_invoices.add(invoice_number)

            # Create the PO detail object
            po_detail = {
                "po_number": po_number,
                "procurement_bu": header.get('PROCUREMENT_BU', '').strip(),
                "requisitioning_bu": header.get('REQUISITIONING_BU', '').strip(),
                "supplier": header.get('SUPPLIER', '').strip(),
                "supplier_site": header.get('SUPPLIER_SITE', '').strip(),
                "buyer": header.get('BUYER', '').strip(),
                "po_date": header.get('CREATION_DATE', '').strip(),
                "po_approval_date": header.get('PO_APPRVL_DT', '').strip(),
                "currency_code": header.get('CURRENCY', '').strip(),
                "po_status": header.get('PO_STATUS', '').strip(),
                "total_amount": float(header.get('TOTAL_AMOUNT', 0)),
                "edi_status": header.get('EDI_CHG_PO_STS', '').strip() or header.get('EDI_CRT_PO_STS', '').strip(),
                "edi_sent_on": header.get('EDI_CHG_PO_DT', '').strip() or header.get('EDI_CRT_PO_DT', '').strip(),
                "email_to_supplier": header.get('EMAIL_COMM_TO_SUPP', '').strip(),
                "line_items": line_items,
                "invoice_lines": invoice_lines,  # Add invoice lines to PO detail
                "ship_to_location": header.get('SHIP_TO_LOCATION', '').strip(),
                "bill_to_location": header.get('BILL_TO_LOCATION', '').strip(),
            }

            return po_detail

        except Exception as e:
            Logger.log("Error processing PO detail group",
                  level="ERROR",
                  error_message=str(e),
                  po_number=po_number)
        return None

    def _process_po_detail_line(self, row: Dict[str, Any]) -> Dict[str, Any]:
            """Process a single line item from the PO details."""
            try:
                return {
                "line_number": int(row.get('LINE_NUMBER', 0)),
                "item_number": row.get('ITEM', '').strip(),
                "item_description": row.get('DESCRIPTION', '').strip(),
                "category": row.get('CATEGORY', '').strip(),
                "quantity": float(row.get('QTY', 0)),
                "unit_price": float(row.get('UNIT_PRICE', 0)),
                "amount": float(row.get('ORDERED_AMOUNT', 0)),
                "need_by_date": row.get('REQUESTED_DELIVERY_DATE', '').strip(),
                "promised_date": row.get('PROMISED_DELIVERY_DATE', '').strip(),
                "received_quantity": float(row.get('RECEIVED_QUANTITY', 0)),
                "invoiced_quantity": float(row.get('QUANTITY_BILLED', 0)),
                "paid_quantity": float(row.get('PAID_QUANTITY', 0)),
                "unit_of_measure": row.get('UOM', '').strip(),
                "manufacturer": row.get('MANUFACTURER', '').strip(),
                "manufacturer_part_number": row.get('MPN', '').strip(),
                "BPA-BPALine": row.get('BPA_LINE', '').strip(),
                "Latest CO": row.get('CO_NUM', '').strip(),
                "Requester": row.get('REQUESTER_NAME', '').strip()
            }

            except Exception as e:
                Logger.log("Error processing PO detail line",
                      level="ERROR",
                      error_message=str(e),
                      row_data=row)
            return None


    async def get_pr_po_apprvl_dtls(
        self,
        Doc_No: Optional[str] = None,
        Doc_Type: Optional[str] = None,
        BU: Optional[str] = None,
        SKU: Optional[str] = None,
        Supplier: Optional[str] = None,
        Creator: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get approval details for PR/PO documents using Oracle BI Report.
        Usage Notes:
        - When query includes "of [name]" -> Shows documents where [name] is the creator
        - When query includes "assigned to [name]" -> Shows documents where [name] is the assignee/approver
        - When query includes "assigned to me" -> Shows documents assigned to the current user
        - Results are displayed in formatted tables showing:
            1. Document Summary (Document, Type, Creation Date, OU, Supplier)
            2. Approval Workflow (Assignee, Manager, Days Elapsed, Status) 
            3. Line Items (Item, Quantity, Price, Location)
        
    Example Queries:
        - "Show documents pending approval of Alexa Cloud"
        - "Get pending approval documents created by John Smith"
        - "Display all documents pending approval where Sarah Jones is creator"
        - "Show documents pending approval assigned to Kristina Landreth"
        - "What are the documents pending approval that are assigned to me"
        - "Get all pending approvals assigned to Christopher Bougas"
        """
        try:
            start_time = datetime.now()
            Logger.log("Getting PR/PO approval details", level="INFO")
            
            # Use the correct report path
            report_path = "/Custom/Square SCM Reports/Block MCP/Procurement/Block Procurement PO-PR in approvers queue.xdo"
            
            # Initialize parameters dict
            parameters = {}
            
            # Add optional parameters if provided
            if Doc_No is not None:
                parameters["P_DOC_NO"] = Doc_No
            if Doc_Type is not None:
                parameters["P_DOC_TYPE"] = Doc_Type
            if BU is not None:
                # Translate user input to Oracle recognized business unit name
                oracle_bu = self.bu_lookup.translate(BU)
                if not self.bu_lookup.validate(oracle_bu):
                    Logger.log("Invalid business unit provided",
                             level="WARNING",
                             input_bu=BU,
                             translated_bu=oracle_bu)
                parameters["P_BU"] = oracle_bu
            if SKU is not None:
                parameters["P_SKU"] = SKU
            if Supplier is not None:
                # Translate user input to Oracle recognized supplier name
                oracle_supplier = self.supplier_lookup.translate(Supplier)
                if not self.supplier_lookup.validate(oracle_supplier):
                    Logger.log("Invalid supplier provided",
                             level="WARNING",
                             input_supplier=Supplier,
                             translated_supplier=oracle_supplier)
                parameters["P_SUPPLIER"] = oracle_supplier
            if Creator is not None:
                parameters["P_CREATOR"] = Creator

            Logger.log("Getting approval details with parameters", level="INFO", parameters=parameters)
            
            # Get report data
            report_data = await self.report_service.get_report_data(
                report_path=report_path,
                parameters=parameters
            )
            
            Logger.log("Processing approval details data", level="INFO")
            
            try:
                # Process the report data
                approval_details = await self._process_approval_details_data(report_data)
                
                # Format tables
                formatted_tables = self._format_approval_details(approval_details)
                
                # Calculate execution time
                execution_time = (datetime.now() - start_time).total_seconds()
                Logger.log("Approval details retrieved successfully", level="DEBUG",
                          execution_time=execution_time,
                          total_records=len(approval_details))
                
                return {
                    "total_results": len(approval_details),
                    "items": approval_details,
                    "execution_time": execution_time,
                    "parameters_used": parameters,
                    "formatted_tables": formatted_tables
                }
                
            except Exception as e:
                Logger.log("Error processing approval details data",
                          level="ERROR",
                          error_message=str(e))
                raise
                
        except Exception as e:
            Logger.log("Error getting approval details",
                      level="ERROR",
                      error_message=str(e),
                      error_type=type(e).__name__)
            return {
                "error": str(e),
                "error_type": type(e).__name__,
                "parameters": parameters
            }

    async def _process_approval_details_data(self, report_data: Union[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
        """Process the approval details report data."""
        details = []
        try:
            # If report_data is an awaitable, await it
            if hasattr(report_data, '__await__'):
                report_data = await report_data

            # If report_data is a string, treat it as a file path
            if isinstance(report_data, str):
                with open(report_data, 'r', newline='', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    rows = list(reader)
            else:
                # Otherwise, treat it as the row data directly
                rows = report_data

            # Process each row
            for row in rows:
                detail = self._process_approval_detail_row(row)
                if detail:
                    details.append(detail)

            return details

        except Exception as e:
            Logger.log("Error processing approval details data",
                      level="ERROR",
                      error_message=str(e))
            raise

    
    def _process_approval_detail_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Process a single row from the approval details."""
        try:
            # Convert numeric values and handle empty strings
            def safe_float(value: str) -> float:
                try:
                    return float(value) if value and value.strip() else 0.0
                except ValueError:
                    return 0.0

            def safe_int(value: str) -> int:
                try:
                    return int(value) if value and value.strip() else 0
                except ValueError:
                    return 0

            return {
                "document": row.get('DOCUMENT', '').strip(),
                "document_type": row.get('DOCUMENTTYPE', '').strip(),
                "line_num": safe_int(row.get('LINE_NUM', '0')),
                "document_creation_date": row.get('Document_Creation_Date', '').strip(),
                "document_submission_date": row.get('Document_Submission_Date', '').strip(),
                "assignment_date": row.get('Assignment_Date', '').strip(),
                "days_elapsed": safe_float(row.get('Days_Elapsed', '0')),
                "time_elapsed": row.get('Time_Elapsed', '').strip(),
                "description": row.get('Description', '').strip(),
                "assignee": row.get('Assignee', '').strip(),
                "username": row.get('Username', '').strip(),
                "assignee_email": row.get('Assignee_s_Email', '').strip(),
                "assignee_manager_id": row.get('Assignee_s_Manager_ID', '').strip(),
                "assignee_manager": row.get('Assignee_s_Manager', '').strip(),
                "assignee_user_id": row.get('Assignee_User_ID', '').strip(),
                "ou": row.get('OU', '').strip(),
                "item": row.get('ITEM', '').strip(),
                "quantity": safe_float(row.get('QUANTITY', '0')),
                "price": safe_float(row.get('PRICE', '0')),
                "extended_price": safe_float(row.get('EXTENDED_PRICE', '0')),
                "location_code": row.get('LOCN_CODE', '').strip(),
                "supplier": row.get('SUPPLIER', '').strip(),
                "change_order_desc": row.get('CHANGE_ORDER_DESC', '').strip(),
                "change_order_qty": safe_float(row.get('CHANGE_ORDER_QTY', '0')),
                "doc_creator": safe_float(row.get('DOC_CREATOR', '0'))
            }

        except Exception as e:
            Logger.log("Error processing approval detail row",
                      level="ERROR",
                      error_message=str(e),
                      row_data=row)
            return None

    def _format_approval_details(self, approval_details: List[Dict[str, Any]]) -> Dict[str, str]:
        """Format approval details into markdown tables."""
        if not approval_details:
            return {"main": "No approval details found."}

        # Document Summary Table
        doc_headers = [
            "Document",
            "Type",
            "Creation Date",
            "Submission Date",
            "OU",
            "Supplier",
            "Document Creator"
        ]

        doc_rows = [
            [
                detail["document"],
                detail["document_type"],
                detail["document_creation_date"],
                detail["document_submission_date"],
                detail["ou"],
                detail["supplier"],
                detail["doc_creator"]
            ]
            for detail in approval_details
        ]

        # Approval Workflow Table
        workflow_headers = [
            "Document",
            "Line",
            "Assignee",
            "Manager",
            "Assignment Date",
            "Days Elapsed",
            "Time Elapsed",
            "Status"
        ]

        workflow_rows = [
            [
                detail["document"],
                detail["line_num"],
                f"{detail['assignee']} ({detail['username']})",
                detail["assignee_manager"],
                detail["assignment_date"],
                f"{detail['days_elapsed']:.1f}",
                detail["time_elapsed"],
                detail["description"]
            ]
            for detail in approval_details
        ]

        # Line Items Table
        items_headers = [
            "Document",
            "Line",
            "Item",
            "Quantity",
            "Price",
            "Extended Price",
            "Location",
            "Change Order",
            "CO Qty"
        ]

        items_rows = [
            [
                detail["document"],
                detail["line_num"],
                detail["item"],
                f"{detail['quantity']:,.2f}",
                f"${detail['price']:,.2f}",
                f"${detail['extended_price']:,.2f}",
                detail["location_code"],
                detail["change_order_desc"],
                f"{detail['change_order_qty']:,.2f}" if detail['change_order_qty'] != 0 else ""
            ]
            for detail in approval_details
        ]

        return {
            "document_summary": self._create_markdown_table(
                headers=doc_headers,
                rows=doc_rows,
                title="Document Summary"
            ),
            "workflow": self._create_markdown_table(
                headers=workflow_headers,
                rows=workflow_rows,
                title="Approval Workflow Details"
            ),
            "line_items": self._create_markdown_table(
                headers=items_headers,
                rows=items_rows,
                title="Line Items Details"
            )
        }

        """Format approval details into markdown tables."""
        if not approval_details:
            return {"main": "No approval details found."}

        headers = [
            "Document Number",
            "Type",
            "BU",
            "Status",
            "Creator",
            "Creation Date",
            "Supplier",
            "SKU",
            "Approver",
            "Approval Status",
            "Approval Date",
            "Level"
        ]

        rows = [
            [
                detail["document_number"],
                detail["document_type"],
                detail["business_unit"],
                detail["document_status"],
                detail["doc_creator"],
                detail["creation_date"],
                detail["supplier"],
                detail["sku"],
                detail["approver"],
                detail["approval_status"],
                detail["approval_date"],
                detail["approval_level"]
            ]
            for detail in approval_details
        ]

        return {
            "main": self._create_markdown_table(
                headers=headers,
                rows=rows,
                title="PR/PO Approval Details"
            )
        }

    async def get_supplier_configs(
        self,
        Supplier: Optional[str] = None        
    ) -> Dict[str, Any]:
        """Get supplier configurations and settings from Oracle BI Report.
        
        IMPORTANT USAGE INSTRUCTIONS:
        1. Query Pattern Recognition:
           - When user asks about specific supplier -> Use P_SUPPLIER parameter
           - When user asks about specific business unit -> Use P_BU parameter
           - When user asks about both supplier and BU -> Use both parameters
           - For other queries -> Use standard parameters (Supplier, BU, SKU)
        
        2. Data Display Requirements:
           - ALWAYS show all three tables from formatted_tables in this order:
             a. Supplier Summary (supplier_summary)
             b. Configuration Details (config_details)
             c. Business Rules (business_rules)
           - Never skip or omit any table
           - Keep all columns as provided in the response
           
        3. Data Integrity:
           - Never fabricate or mock data
           - If no data found, clearly indicate "No results found"
           - Show complete data from report output
           - Handle empty/null values appropriately
        
        Args:
            Supplier: Optional supplier name filter
            BU: Optional business unit filter
            SKU: Optional SKU number filter
            P_SUPPLIER: Optional specific supplier name
            P_BU: Optional specific business unit
        """
        try:
            start_time = datetime.now()
            Logger.log("Getting supplier configurations", level="INFO")
            
            # Use the correct report path
            report_path = "/Custom/Square SCM Reports/Block MCP/Procurement/Supplier Contacts and B2B Config Report.xdo"
            
            # Initialize parameters dict
            parameters = {}
            
            # Add optional parameters if provided
            if Supplier is not None:
                # Translate user input to Oracle recognized supplier name
                oracle_supplier = self.supplier_lookup.translate(Supplier)
                if not self.supplier_lookup.validate(oracle_supplier):
                    Logger.log("Invalid supplier provided",
                             level="WARNING",
                             input_supplier=Supplier,
                             translated_supplier=oracle_supplier)
                parameters["P_SUPPLIER"] = oracle_supplier            
            
            Logger.log("Getting supplier configs with parameters", level="INFO", parameters=parameters)
            
            # Get report data
            report_data = await self.report_service.get_report_data(
                report_path=report_path,
                parameters=parameters
            )
            
            Logger.log("Processing supplier configuration data", level="INFO")
            
            try:
                # Process the report data
                supplier_configs = await self._process_supplier_config_data(report_data)
                
                # Format tables
                formatted_tables = self._format_supplier_configs(supplier_configs)
                
                # Calculate execution time
                execution_time = (datetime.now() - start_time).total_seconds()
                Logger.log("Supplier configurations retrieved successfully", level="DEBUG",
                          execution_time=execution_time,
                          total_records=len(supplier_configs))
                
                return {
                    "total_results": len(supplier_configs),
                    "items": supplier_configs,
                    "execution_time": execution_time,
                    "parameters_used": parameters,
                    "formatted_tables": formatted_tables
                }
                
            except Exception as e:
                Logger.log("Error processing supplier configuration data",
                          level="ERROR",
                          error_message=str(e))
                raise
                
        except Exception as e:
            Logger.log("Error getting supplier configurations",
                      level="ERROR",
                      error_message=str(e),
                      error_type=type(e).__name__)
            return {
                "error": str(e),
                "error_type": type(e).__name__,
                "parameters": parameters
            }

    async def _process_supplier_config_data(self, report_data: Union[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
        """Process the supplier configuration report data."""
        configs = []
        try:
            # If report_data is an awaitable, await it
            if hasattr(report_data, '__await__'):
                report_data = await report_data

            # If report_data is a string, treat it as a file path
            if isinstance(report_data, str):
                with open(report_data, 'r', newline='', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    rows = list(reader)
            else:
                # Otherwise, treat it as the row data directly
                rows = report_data

            # Process each row
            for row in rows:
                config = self._process_supplier_config_row(row)
                if config:
                    configs.append(config)

            return configs

        except Exception as e:
            Logger.log("Error processing supplier configuration data",
                      level="ERROR",
                      error_message=str(e))
            raise

    def _process_supplier_config_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Process a single row from the supplier configurations with the new field layout."""
        try:
            return {
                # Supplier Details
                "SUPPLIER_NAME": row.get('SUPPLIER_NAME', '').strip(),
                "SUPPLIER_NUMBER": row.get('SUPPLIER_NUMBER', '').strip(),
                "PERSON_FIRST_NAME": row.get('PERSON_FIRST_NAME', '').strip(),
                "PERSON_LAST_NAME": row.get('PERSON_LAST_NAME', '').strip(),
                "USERNAME": row.get('USERNAME', '').strip(),
                "ACCESS_LEVEL": row.get('ACCESS_LEVEL', '').strip(),
                "ACCESS_TO": row.get('ACCESS_TO', '').strip(),
                "ROLE": row.get('ROLE', '').strip(),
                "EMAIL_ADDRESS": row.get('EMAIL_ADDRESS', '').strip(),
                
                # Supplier Site Details
                "VENDOR_SITE_CODE": row.get('VENDOR_SITE_CODE', '').strip(),
                "NAME": row.get('NAME', '').strip(),  # Proc BU
                "PURCHASING_SITE_FLAG": row.get('PURCHASING_SITE_FLAG', '').strip(),
                "RFQ_ONLY_SITE_FLAG": row.get('RFQ_ONLY_SITE_FLAG', '').strip(),
                "PAY_SITE_FLAG": row.get('PAY_SITE_FLAG', '').strip(),
                "PRIMARY_PAY_SITE_FLAG": row.get('PRIMARY_PAY_SITE_FLAG', '').strip(),
                "EFFECTIVE_START_DATE": row.get('EFFECTIVE_START_DATE', '').strip(),
                "EFFECTIVE_END_DATE": row.get('EFFECTIVE_END_DATE', '').strip(),
                "SUPPLIER_NOTIF_METHOD": row.get('SUPPLIER_NOTIF_METHOD', '').strip(),
                "Supplier Email Address": row.get('PO_COMM_EMAIL', '').strip(),
                # Supplier Site B2B/EDI Details
                "SERVICE_PROVIDER_NAME": row.get('SERVICE_PROVIDER_NAME', '').strip(),
                "B2B_COMM_METHOD_CODE": row.get('B2B_COMM_METHOD_CODE', '').strip(),
                "DOCS": row.get('DOCS', '').strip(),
                
                # Keep track of last update info
                "last_update_date": row.get('LAST_UPDATE_DATE', '').strip(),
                "last_updated_by": row.get('LAST_UPDATED_BY', '').strip()
            }

        except Exception as e:
            Logger.log("Error processing supplier config row",
                      level="ERROR",
                      error_message=str(e),
                      row_data=row)
            return None

    def _format_supplier_configs(self, supplier_configs: List[Dict[str, Any]]) -> Dict[str, str]:
        """Format supplier configurations into three main sections:
        1. Supplier Details
        2. Supplier Site Details
        3. Supplier Site B2B/EDI Details
        """
        if not supplier_configs:
            return {"main": "No supplier configurations found."}

        # Supplier Details Table
        supplier_details_headers = [
            "Field",
            "Value"
        ]

        supplier_details_rows = [
            ["Supplier Name", supplier_configs[0].get("SUPPLIER_NAME", "")],
            ["Supplier Number", supplier_configs[0].get("SUPPLIER_NUMBER", "")],
            ["Contact First Name", supplier_configs[0].get("PERSON_FIRST_NAME", "")],
            ["Contact Last Name", supplier_configs[0].get("PERSON_LAST_NAME", "")],
            ["Portal User Name", supplier_configs[0].get("USERNAME", "")],
            ["Portal User Access", supplier_configs[0].get("ACCESS_LEVEL", "")],
            ["Portal User Access To", supplier_configs[0].get("ACCESS_TO", "")],
            ["Portal User Roles Access", supplier_configs[0].get("ROLE", "")],
            ["Contact Email", supplier_configs[0].get("EMAIL_ADDRESS", "")]
        ]

        # Supplier Site Details Table
        site_details_headers = [
            "Field",
            "Value"
        ]

        site_details_rows = [
            ["Supplier Site", supplier_configs[0].get("VENDOR_SITE_CODE", "")],
            ["Proc BU", supplier_configs[0].get("NAME", "")],
            ["Purchasing Site Flag", supplier_configs[0].get("PURCHASING_SITE_FLAG", "")],
            ["RFQ Site Flag", supplier_configs[0].get("RFQ_ONLY_SITE_FLAG", "")],
            ["Pay Site Flag", supplier_configs[0].get("PAY_SITE_FLAG", "")],
            ["Primary Pay Site Flag", supplier_configs[0].get("PRIMARY_PAY_SITE_FLAG", "")],
            ["Eff Start Date", supplier_configs[0].get("EFFECTIVE_START_DATE", "")],
            ["Eff End Date", supplier_configs[0].get("EFFECTIVE_END_DATE", "")],
            ["Supplier Notification Method", supplier_configs[0].get("SUPPLIER_NOTIF_METHOD", "")],
            ["Supplier Email Address", supplier_configs[0].get("Supplier Email Address", "")]
        ]

        # Supplier Site B2B/EDI Details Table
        b2b_details_headers = [
            "Field",
            "Value"
        ]

        b2b_details_rows = [
            ["Supplier Site", supplier_configs[0].get("VENDOR_SITE_CODE", "")],
            ["Proc BU", supplier_configs[0].get("NAME", "")],
            ["Service Provider Name", supplier_configs[0].get("SERVICE_PROVIDER_NAME", "")],
            ["B2B Comm Method", supplier_configs[0].get("B2B_COMM_METHOD_CODE", "")],
            ["B2B Docs", supplier_configs[0].get("DOCS", "")]
        ]

        return {
            "supplier_details": self._create_markdown_table(
                headers=supplier_details_headers,
                rows=supplier_details_rows,
                title="Supplier Details"
            ),
            "site_details": self._create_markdown_table(
                headers=site_details_headers,
                rows=site_details_rows,
                title="Supplier Site Details"
            ),
            "b2b_details": self._create_markdown_table(
                headers=b2b_details_headers,
                rows=b2b_details_rows,
                title="Supplier Site B2B/EDI Details"
            )
        }

def get_oracle_procurement() -> OracleProcurementManager:
    """Factory function to create and return an OracleProcurementManager instance."""
    return OracleProcurementManager()