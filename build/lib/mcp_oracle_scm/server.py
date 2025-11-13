"""Oracle SCM MCP Server"""

from datetime import datetime, timedelta
from typing import Optional, Dict, Any, Union, List
from mcp.server import FastMCP
import logging
import os
from dotenv import load_dotenv

# Import modules
from mcp_oracle_scm.order_management.order_service import (
    OracleOrderManager, 
    EnvironmentConfig,
    get_oracle_om
)
from mcp_oracle_scm.inventory.inventory_service import (
    OracleInventoryManager,
    get_oracle_inventory_manager
)
from mcp_oracle_scm.procurement.procurement_service import get_oracle_procurement
from mcp_oracle_scm.product_management.item_service import get_item_service
from mcp_oracle_scm.common.report_service import OracleReportService
from mcp_oracle_scm.config.environment import get_env_config
from mcp_oracle_scm.config.logger_config import LoggerConfig as Logger

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d %(levelname)s:%(name)s:%(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Create an MCP server
instructions = """
Oracle SCM MCP Server

IMPORTANT: BEFORE ANY TOOL USE
1. You MUST read these complete instructions first
2. You MUST validate the tool selection based on the instruction provided for each tool
DO NOT PROCEED WITH TOOL CALLS UNTIL YOU HAVE COMPLETED THESE STEPS.

This MCP server provides tools to interact with Oracle Supply Chain Management system.


Available Modules:

1. Order Management:

    General Instruction:
         You MUST display theresult in tabular format for better understanding.
         While answering customer's ask try to route to correct tool by carefully considering the keywords.
         e.g. 
            => Query related to order source (retail, edi, cpq, sfdc, manual, shop, ecom, bc etc) or order type, always route to "get_order_count" tool. And do NOT call any other tools.
            => Query for a single order with order number / source order number / customer order number, route to "check_single_order_details" tool.
                But if a single order is queried for specific type or source, this tool won't be able to get the details.
            => Query for any line level details (like quantity for specific sku in certain organization  etc), call "extract_order_line_details" tool
            => For generic order count by region / Business Unit, call "get_order_count" tool
            => For any query related to stuck orders, call "get_open_orders" tool
            => For any query related to Open Order, first check whether either of "get_order_count" or "get_order_line_summary" tool can bring the relevant info. If not, then only run "get_open_orders"
            => For any back order related query call "get_back_orders" tool
            => For any order line level summary info, call "get_order_line_summary" tool ONLY
            => If a query made specific to SKU, don't call "get_order_count" or "check_single_order_details" tool

        These are just few examples. Don't get limited to these asks only.

    Available Tools:

       * get_order_count: 

           - USE WHEN: Need to count orders for a specific time period, business unit, source, or order type and display the result to user in tabular format
           - EXAMPLE QUERIES: "How many orders in last 30 days?", "Get order count for CA", "How many SHOP orders created for DBS", "get me the details of last years 'Sales Force' Orders"
           - Parameters for BI Report:
                > offset_days: For time period (e.g., 7, 30, 90)
                > p_bu: Business Unit filter (e.g., 'US', 'UK', 'Canada'). User might not pass exact business unit. So use # Common Business Unit Mapping shared below to derive correct p_bu from customer input.
                    
                > p_source: Order source filter (e.g., 'SHOP', 'EDI', 'SFDC', 'OPS'). Use # Common Order Source Mapping shared below to derive correct p_source from customer input. Whie displaying output, show customer info from the report.
                    
                > p_order_type: Type filter (e.g., 'ECOM_NORMAL_SHIPONLY'). Use # Common Order Type Mapping shared below to derive correct p_order_type from customer input.

       * check_single_order_details: Get details for a specific order number with simplified output

       * get_order_line_summary: Get order line summary by running Oracle BI Report. Always show the output in tabular format for better understanding. Do NOT call this tool if user ask is about a specific order type or order source (retail, cpq, manual etc)
          - USE WHEN: Need summary of order lines by warehouse, especially for inventory analysis. Do NOT use this tool when user is asking data specific to an order source (e.g. manual, retail, cpq, sfdc, ecom, bc, shop etc)
          - EXAMPLE QUERIES: "Show order line summary for last week", "Get total quantity order for a SKU", "get the count of total shipped orders for US for SKU XYZ", "how many orders are waiting to be shipped",
          - Parameters for BI Report:
                > offset_days: Time period to look back
                > p_sku: Filter by specific SKU
                > p_warehouse: Filter by warehouse code. Use # Common Warehouse Mapping shared below to derive correct 3 digit warehouse from customer input

       * extract_order_line_details: Extract order line details by running Oracle BI Report and group report output based on user need. Do NOT call this tool if user ask is about a specific order type or order source (retail, cpq, manual etc).
                                     When query is made for certain order source or order type, route to "get_order_count" tool
          - USE WHEN: Need to details of order lines by warehouse, especially for inventory analysis like total ordered quantity in a specific warehouse
          - EXAMPLE QUERIES: "Show orders created in last week for SKU XYZ", "Get total quantity order for a SKU", "get the details of total shipped orders for US for SKU XYZ", "which orders are waiting to be shipped"
          - Parameters for BI Report:
                > offset_days: Time period to look back
                > p_sku: Filter by specific SKU
                > p_warehouse: Filter by warehouse code. Use # Common Warehouse Mapping shared below to derive correct 3 digit warehouse from customer input

       * get_open_orders:  Check the details of current open stuck sales orders using BI Report and present the output to user in tabular format for better understanding.
                           Do NOT call this tool when user specifically asks about Back Orders.
                           For query related to Open Orders, first check whether either of "get_order_count" or "get_order_line_summary" tool can bring the relevant info. If NOT, then only run "get_open_orders"
          - USE WHEN: Need to find unfulfilled or stuck or pending orders
          - EXAMPLE QUERIES: "Show all open orders", "Find pending orders for SKU ABC", "show the details of stuck orders for each BU", "Show current open order age for US"
          - Parameters for BI Report:
                > offset_days: Time period for search
                > p_sku: Filter by SKU
                > p_warehouse: Filter by warehouse. Use # Common Warehouse Mapping shared below to derive correct 3 digit warehouse from customer input

       * get_back_orders: Get Square Backorder Details by running Oracle BI Report and present the output to user in tabular format for better understanding.
                          Don't call this tool unless user specifically ask about back orders. Never run this BI report for more than 30 days.
                          If customer asks to run it for longer period (say a quarter / 60 days) notify them to "Run Historical BackOrder Report from Oracle BI Publisher, due to data volume issue"
          - USE WHEN: Need to analyze backorders or delayed fulfillment.
                      Properly read the SHIP_FROM_ORG field from the BI report output to group the back orders by the actual warehouse codes
                      Present the summary with the correct warehouse information (SHIP_FROM_ORG field from the report output) instead of defaulting to "Unknown"
          - EXAMPLE QUERIES: "Show current backorder details", "Get backorder report for last month for each geo", "How does the age of the back orders look like in US", "show the volume of backorders for sku ABC"
          - Parameters for BI Report:
                > p_from_sales_ord_date: Start date (MM-DD-YYYY)
                > p_to_sales_ord_date: End date (MM-DD-YYYY)
                > p_warehouse: Filter by warehouse.  Use # Common Warehouse Mapping shared below to derive correct 3 digit warehouse from customer input
                > p_item: Filter by specific item/SKU


    Common mappings to be used by the above mentioned 6 Order Management tools:

       # Common Business Unit Mapping: (while determining BI report input parameter "p_bu" in "get_order_count" tool, use this mapping.)
            USA => US
            CVU => US
            United States => US
            Domestic => US
            America => US
            US => US
            States => US
            Ceva US => US
            US => US
            CA => Canada
            canada => Canada
            CANADA => Canada
            CVC => Canada
            IMC => Canada
            Ceva Canada => Canada
            Canada => Canada
            GBR => UK
            GB => UK
            United Kingdom => UK
            London => UK
            Britain => UK
            UK => UK
            AU => Australia
            AUS => Australia
            Sydney => Australia
            Arvato => Australia
            ARV => Australia
            DBS Australia => Australia
            SYD => Australia
            Australia => Australia
            Europe => Ireland
            EU => Ireland
            Netherland => Ireland
            NLD => Ireland
            IE => Ireland
            Ireland => Ireland
            Paris => Ireland
            France => Ireland
            SCH => Japan
            DBS => Japan
            DBS Japan => Japan
            JP => Japan
            Japan => Japan
            Bitkey => Bitcoin HW US
            Bitkey US => Bitcoin HW US
            Bitcoin => Bitcoin HW US
            Bitcoin US => Bitcoin HW US
            Bitkey domestic => Bitcoin HW US
            Bitcoin Domestic => Bitcoin HW US
            BK US => Bitcoin HW US
            MLU => Bitcoin HW US
            BK domestic => Bitcoin HW US
            Moduslink US => Bitcoin HW US
            Moduslink => Bitcoin HW US
            ML => Bitcoin HW US
            ML US => Bitcoin HW US
            Moduslink domestic => Bitcoin HW US
            Bitcoin Hardware => Bitcoin HW US
            Bitcoin HW => Bitcoin HW US
            Bitkey HW => Bitcoin HW US
            Bitkey Hardware => Bitcoin HW US
            Bitcoin HW US => Bitcoin HW US
            Bitcoin HW NL => Bitcoin HW NL
            Bitkey Intl => Bitcoin HW NL
            Bitcoin INTL => Bitcoin HW NL
            Bitcoin International => Bitcoin HW NL
            Bitkey International => Bitcoin HW NL
            ML Intl => Bitcoin HW NL
            ML International => Bitcoin HW NL
            MLI => Bitcoin HW NL
            BK International => Bitcoin HW NL
            Moduslink Intl => Bitcoin HW NL
            Moduslink International => Bitcoin HW NL
            Proto Global => Proto Global
            Proto => Proto Global
            ASE => Proto Global
            FMY => Proto Global
            Mining => Proto Global
            R2 => Proto Global
            MC2 => Proto Global

        # Common Warehouse Mapping:
            User might not always provide correct 3-letter warehouse code to be passed. So read customer input and use the below 'customer input' => 'p_warehouse' mapping to decide the warehouse.
            'p_warehouse' parameter is being used by all these three tools: "get_order_line_summary", "extract_order_line_details", "get_open_orders", "get_back_orders"
            USA => CVU
            CVU => CVU
            United States => CVU
            Domestic => CVU
            America => CVU
            US => CVU
            States => CVU
            Ceva US => CVU
            US => CVU
            Jusda => JDU
            CA => IMC
            canada => IMC
            CANADA => IMC
            CVC => IMC
            IMC => IMC
            Ceva Canada => IMC
            Canada => IMC
            GBR => GBR
            GB => GBR
            United Kingdom => GBR
            London => GBR
            Britain => GBR
            UK => GBR
            AU => ARV
            AUS => ARV
            Sydney => ARV
            Arvato => ARV
            ARV => ARV
            DBS Australia => ARV
            SYD => ARV
            Australia => ARV
            Europe => NLD
            EU => NLD
            Netherland => NLD
            NLD => NLD
            IE => NLD
            Ireland => NLD
            Paris => NLD
            France => NLD
            SCH => SCH
            DBS => SCH
            DBS Japan => SCH
            JP => SCH
            Japan => SCH
            Bitkey => MLU
            Bitkey US => MLU
            Bitcoin => MLU
            Bitcoin US => MLU
            Bitkey domestic => MLU
            Bitcoin Domestic => MLU
            BK US => MLU
            MLU => MLU
            BK domestic => MLU
            Moduslink US => MLU
            Moduslink => MLU
            ML => MLU
            ML US => MLU
            Moduslink domestic => MLU
            Bitcoin Hardware => MLU
            Bitcoin HW => MLU
            Bitkey HW => MLU
            Bitkey Hardware => MLU
            Bitcoin HW US => MLU
            Bitcoin HW NL => MLI
            Bitkey Intl => MLI
            Bitcoin INTL => MLI
            Bitcoin International => MLI
            Bitkey International => MLI
            ML Intl => MLI
            ML International => MLI
            MLI => MLI
            BK International => MLI
            Moduslink Intl => MLI
            Moduslink International => MLI
            Singapore Mining => SGM
            Singapore D2C => SGU
            Foxconn Malaysia => FMY
            Foxconn MY => FMY
            Mining MY => FMY
            Mining San Jose => FSJ
            Mining SJC => FSJ
            Foxconn San Jose => FSJ

        # Common Order Source Mapping (while determining BI report input parameter "p_source" in "get_order_count" tool, use this mapping.)
            Manual => OPS
            OPS => OPS
            SHOP => SHOP
            Ecom => SHOP
            E-Comm => SHOP
            Ecommerce => SHOP
            B2C => SHOP
            BigCommerce => BC
            BC => BC
            BigComm => BC
            Retail => EDI
            Distributor => EDI
            EDI => EDI
            B2B => EDI
            CPQ => SFDC
            Enterprise => SFDC
            SalesForce => SFDC
            SF => SFDC
            SFDC => SFDC
            GSHEET => GSHEET

        # Common Order Type Mapping (while determining BI report input parameter "p_order_type" in "get_order_count" tool, use this mapping.)
            ECOM NORMAL ZERO SHIPONLY => ECOM_NORMAL_ZERO_SHIPONLY
            SQ SHIP ONLY => SQ_SHIP_ONLY
            RETAIL NORMAL SHIPONLY => RETAIL_NORMAL_SHIPONLY
            SQ SCRAP => SQ_SCRAP
            SQ WARRANTY => SQ_WARRANTY
            TRANSFER ORDER SHIPONLY => TRANSFER_ORDER_SHIPONLY
            ECOM NORMAL SHIPONLY => ECOM_NORMAL_SHIPONLY
            SQ EFFA ORDERS => SQ_EFFA
            SQ Scrap Unavlbl => SQ_SCRAP_UNAVL
            ENTERPRISE NORMAL SHIPONLY => ENTERPRISE_NORMAL_SHIPONLY
            SQ P00 ORDERS => SQ_P00_ORDERS
            ECOM NORMAL STANDARD => ECOM_NORMAL_STANDARD>
            ECOM WARRANTY SHIPONLY => ECOM_WARRANTY_SHIPONLY
            RETAIL NORMAL STANDARD => RETAIL_NORMAL_STANDARD

    

2. Inventory Management:
   - lookup_inventory_transaction_details: Look up detailed inventory transactions using BI Report - Block Inventory Transaction Details Report.
=> In case Inventory Transaction Details Report has output more than 10 Records, then always display the Top 10 records with most "Transaction Quantity". Additionally show as message as "Note: Showing first 10 rows of total inventory records.
 If you want to see complete data please check the Block Inventory Transactions Detail Report downloaded in you local Downloads folder".


 ==> Analytics - User may ask for checking volume based on certain transaction type, in that case first download the Block Inventory Transaction Details Report.
 Then get the counts by filtering data from the output file "Block Inventory Transaction Details Report" by using below mapping based on either "TRANSACTION_TYPE" or "SOURCE_TYPE".
     Outbound Shipments => Filter with ""TRANSACTION_TYPE" as "Sales Order Issue"
     Shipments => Filter with ""TRANSACTION_TYPE" as "Sales Order Issue"
     Receipts => Filter with "TRANSACTION_TYPE" as Purchase Order Receipt" and "Warehouse Receipt".
     PO Receipts => Filter with "TRANSACTION_TYPE" as "Purchase Order Receipt"
     Returns => Filter with "TRANSACTION_TYPE" as "RMA Receipt"
     RMA => Filter with "TRANSACTION_TYPE" as "RMA Receipt"
     
   
   - lookup_inventory_summary: Look up enhanced inventory summary with balance information by using Block Inventory Transactions Summary Report.
 => INVENTORY_REPORT_COLUMNS = [
    "Item Number", "Organization", "Subinventory", 
    "Opening On-Hand Balance", "Total Receipts", "Total Shipments", 
    "Total Adjustments", "Ending On-Hand Balance", 
    "Pending Transaction Count", "Pending Transaction Quantity", 
    "Completed Transaction Count", "Completed Transaction Quantity", 
    "Available to Reserve"
]

 => If you plan to show the date in the Inventory Summary table while using lookup_inventory_summary tool, then show both Start Date and End Date.

 =>INVENTORY_REPORT_RULES = {
    "always_show_raw_data_first": True,
    "consolidate_subinventories": False,
    "format_numbers_as_integers": True,
    "include_analytics_section": True
}

 => In case user asks about showing current On hand levels or current Inventory levels for a given Warehouse, then use the lookup_inventory_summary to download the Block Inventory Transactions Summary Report for that Warehouse with both p_date_start and p_date_end as today's date . Then use below  columns to show on hand levels -

    INVENTORY_REPORT_COLUMNS =[
    "Item Number", "Organization", "Subinventory", 
   "Ending On-Hand Balance", 
    "Available to Reserve"
]

 => In case Inventory Summary Report has output more than 15 Records, then always display the Top 15 records with most "Completed Transaction Quantity". Additionally show as message as "Note: Showing first 15 rows of total inventory records.
 If you want to see complete data please check the Block Inventory Transactions Summary Report downloaded in you local Downloads folder".

    - lookup_inventory_transactions: Look up inventory transactions using BI Report.  To analyze transaction patterns by type and subinventory.

# Format settings for inventory data display
INVENTORY_DISPLAY_SETTINGS = {
    "format_numbers_as_integers": True,
    "decimal_places": 0,
    "show_commas_in_thousands": True,
    "quantity_fields": [
        "transaction_quantity",
        "total_receipts",
        "total_shipments",
        "total_adjustments",
        "opening_on_hand_balance",
        "ending_on_hand_balance",
        "pending_transaction_quantity",
        "completed_transaction_quantity"
    ]
}

# Example function that could be implemented in the service
def format_quantity(value):
    "Format quantity values as integers without decimal points"
    if INVENTORY_DISPLAY_SETTINGS["format_numbers_as_integers"]:
        return int(float(value))
    return value

# Usage instructions for the assistant

When displaying transaction quantities or inventory balances, always:
1. Convert all numeric values to integers using int(float(value))
2. Do not show decimal points in quantity fields
3. Format large numbers with commas for readability
4. Apply this formatting to all fields listed in INVENTORY_DISPLAY_SETTINGS["quantity_fields"]

#Common mappings to be used by the above mentioned 3 Inventory Management tools:
  User might not always provide correct 3-letter warehouse code to be passed. So read customer input and use the below 'customer input' => 'p_wh_code' mapping to decide the warehouse.
            'p_wh_code' parameter is being used by all these three tools: "lookup_inventory_transactions", "lookup_inventory_transaction_details", "lookup_inventory_summary"
            USA => CVU
            CVU => CVU
            United States => CVU
            Domestic => CVU
            America => CVU
            US => CVU
            States => CVU
            Ceva US => CVU
            US => CVU
            CA => IMC
            canada => IMC
            CANADA => IMC
            CVC => IMC
            IMC => IMC
            Ceva Canada => IMC
            Canada => IMC
            GBR => GBR
            GB => GBR
            United Kingdom => GBR
            London => GBR
            Britain => GBR
            UK => GBR
            AU => ARV
            AUS => ARV
            Sydney => ARV
            Arvato => ARV
            ARV => ARV
            DBS Australia => ARV
            SYD => ARV
            Australia => ARV
            Europe => NLD
            EU => NLD
            Netherland => NLD
            NLD => NLD
            IE => NLD
            Ireland => NLD
            Paris => NLD
            France => NLD
            SCH => SCH
            DBS => SCH
            DBS Japan => SCH
            JP => SCH
            Japan => SCH
            Bitkey => MLU
            Bitkey US => MLU
            Bitcoin => MLU
            Bitcoin US => MLU
            Bitkey domestic => MLU
            Bitcoin Domestic => MLU
            BK US => MLU
            MLU => MLU
            BK domestic => MLU
            Moduslink US => MLU
            Moduslink => MLU
            ML => MLU
            ML US => MLU
            Moduslink domestic => MLU
            Bitcoin Hardware => MLU
            Bitcoin HW => MLU
            Bitkey HW => MLU
            Bitkey Hardware => MLU
            Bitcoin HW US => MLU
            Bitcoin HW NL => MLI
            Bitkey Intl => MLI
            Bitcoin INTL => MLI
            Bitcoin International => MLI
            Bitkey International => MLI
            ML Intl => MLI
            ML International => MLI
            MLI => MLI
            BK International => MLI
            Moduslink Intl => MLI
            Moduslink International => MLI
            Proto => FMY
            Foxconn Malaysia =>FMY
            San Jose =>FSJ
 

3. Procurement:
    
    General Instruction:
        While answering customer's procurement-related queries, route to the correct tool by considering these guidelines:
        => For PO summaries and statistics, use "get_po_summary" tool
        => For detailed PO information, use "get_po_details" tool
        => For approval workflow queries, use "get_pr_po_apprvl_dtls" tool
        => For supplier configuration/config queries, use "get_supplier_configs" tool
       IMPORTANT: NEVER mock data - only return actual data from the Oracle BI Reports.
       If no data is found, just say you don't have it.  
    Available Tools:

       * get_po_summary:
           - USE WHEN: Need to get purchase order summaries and statistics
           - EXAMPLE QUERIES: 
               * "Show me all POs from 2025"
               * "Get PO summary for supplier XYZ"
               * "Get PO summary for supplier XYZ and category XYZ"
               * "How many POs were created in March 2025?"
               * "Show POs for item ABC-123"
           - Parameters:
               > year: The year to filter POs (e.g., 2025)
               > P_MPN: Manufacturer Part Number filter
               > P_Month: Month number (1-12)
               > P_ITEM: Item number filter
               > P_PONUM: Purchase Order Number
               > P_DOC_STATUS: Document Status
               > P_REQ_NUM: Requisition Number
               > P_CATEGORY: Category code(s)
               > P_SUPPLIER: Supplier name
               > P_REQUESTER: Requester name
               > P_MANUFACTURER: Manufacturer name

       * get_po_details:
           - USE WHEN: Need detailed information about specific purchase orders
           - EXAMPLE QUERIES:
               * "Show me all details for POs in 2025"
               * "Get complete PO information for supplier XYZ in 2025"
               * "Get PO summary for supplier XYZ and category XYZ"
               * "What are the line items for PO number ABC123?"
           - Parameters:
               > year: Required 4-digit year (e.g., 2025)
               > P_MPN: Manufacturer Part Number
               > P_Month: Month number (1-12)
               > P_ITEM: Item number
               > P_PONUM: PO number
               > P_DOC_STATUS: Document status
               > P_REQ_NUM: Requisition number
               > P_CATEGORY: Category code(s)
               > P_SUPPLIER: Supplier name
               > P_REQUESTER: Requester name
               > P_MANUFACTURER: Manufacturer name
               > P_BUYER: Buyer name
               > P_SHIP_TO: Ship-to location
               > P_BILL_TO: Bill-to location
               > P_PROC_BU: Procurement Business Unit

       * get_pr_po_apprvl_dtls:
           - USE WHEN: Need to check approval status and workflow for PR/PO documents
           - EXAMPLE QUERIES:
               * "Show me approval status for document ABC123"
               * "Who needs to approve PR XYZ?"
               * "Show all documents pending approval in US BU"
           - Parameters:
               > Doc_No: Document number
               > Doc_Type: Document type (PR or PO)
               > BU: Business Unit
               > SKU: SKU number
               > Supplier: Supplier name
               > Creator: Document creator
        Always Display Results in Tabular Format:
        - get_po_summary: Display results using the formatted tables provided in the response
        - get_po_details: Show all sections (Summary, Locations, Line Items, Invoices) in their respective table formats
        - get_pr_po_apprvl_dtls: Use the formatted approval workflow tables

     * get_supplier_configs:
            IMPORTANT: NEVER mock data - only return actual supplier configurations from the Oracle BI Report.
        If no data is found, clearly indicate this to the user rather than creating placeholder data.

        - USE WHEN: Need to retrieve supplier configurations, settings, and contact information
        - EXAMPLE QUERIES: 
            * "Show me supplier configurations for XYZ company"
            * "Get supplier settings and contacts for ABC supplier"
            * "What are the B2B/EDI settings for supplier 123?"
            * "Show me all business units configured for supplier XYZ"
        - Parameters:
            > supplier: Optional supplier name to filter by
        - Response Format:
            The tool returns:
            - Supplier Details (Name, Number, Site Code)
            - Business Unit Configurations (by region/country)
            - Contact Information (key contacts and roles)
            - Portal Users and Access Levels
            - B2B/EDI Settings
            - Document Types Enabled
        - Display Format:
            Always show results in these sections:
            1. Basic Supplier Information
            2. Business Unit Configurations (one section per BU)
            3. Portal Users Table
            4. Key Contacts Table
            - Use markdown tables with clear headers
            - Group related information together
            - Show dates in consistent format

    
    2. Table Sections to Include:
        For PO Details:
        | Section | Table Contents |
        |---------|----------------|
        | Summary | PO Number, BU, Supplier, Status, Dates, Amounts |
        | Locations | Ship-to, Bill-to locations |
        | Line Items | Item details, quantities, prices |
        | Invoices | Invoice numbers, dates, amounts, payment status |
        
        For PO Summary:
        | Section | Table Contents |
        |---------|----------------|
        | Summary Stats | Total POs, Total Amount, Status Breakdown |
        | PO List | PO Numbers, Dates, Suppliers, Amounts |
        
        For Approval Details:
        | Section | Table Contents |
        |---------|----------------|
        | Document Info | Doc Number, Type, Status, Creator |
        | Approval Flow | Approver, Level, Status, Date |

    3. Formatting Rules:
        - Use markdown tables with headers
        - Align numbers right, text left
        - Format currency with commas and 2 decimal places
        - Show dates in consistent format (DD-MMM-YYYY)
        
    Common Document Status Values:
        APPROVED
        PENDING APPROVAL
        REJECTED
        WITHDRAWN
        IN PROCESS
        INCOMPLETE

    Important Notes:
        1. Year Parameter:
           - Always required for PO queries
           - Must be a 4-digit number (e.g., 2025), if year is blank defaults to current year
        
        2. Performance Tips:
           - Use specific filters when possible
           - Avoid retrieving large date ranges without filters
        
        3. Business Units:
           - Use the Common Business Unit Mapping shared above
           - Invalid BU names will be logged with warnings

4. Product Management:
    
    General Instruction:
        While answering customer's product-related queries, route to the correct tool by considering these guidelines:
        => For item details, specifications, and attributes, use "lookup_item_details" tool
        => Pay attention to organization codes and categories when querying items
        => Consider D2C status when relevant for item queries

    Available Tools:

       * lookup_item_details:
           - USE WHEN: Need to retrieve detailed information about items, including specifications, attributes, and status
           - EXAMPLE QUERIES: 
               * "Show me details for item ABC-123"
               * "Get all items in category XYZ"
               * "List D2C enabled items in US warehouse"
               * "What's the current price for SKU ABC-123?"
           - Parameters:
               > p_item_number: Optional item number to filter by
               > p_org: Optional organization/warehouse code to filter by
               > p_category: Optional item category to filter by
               > offset_days: Optional number of days to offset the search
               > p_d2c: Optional filter for D2C enabled items ('Y' or 'N')

    Tool Response Format:
        The lookup_item_details tool returns:
        - Item Category and Classification
        - Item Number/SKU/Product Code
        - Description and Specifications
        - Organization/Warehouse Assignment
        - Creation and Last Update Information
        - Ring Fencing and D2C Status
        - SKU Price and Cost Information
        - Item Status and Lifecycle Stage

    Important Notes:
        1. Organization Parameter:
           - Use the Common Warehouse Mapping shared above
           - Invalid organization codes will be logged with warnings
        
        2. Performance Tips:
           - Use specific filters when possible
           - Combine filters to narrow results
           - Consider date ranges carefully

    Future Capabilities (Coming Soon):
        - Product lifecycle management
        - Bill of materials management
        - Product catalog bulk operations
        - Item attribute management
        - Category hierarchy management

5. Supply Chain Planning (Coming Soon):
   - Demand planning
   - Supply planning
   - Production scheduling

Environment Configuration:
- ORACLE_ENV: Optional environment selector (DEV1, TEST, PROD). Defaults to DEV1.
  This determines which Oracle instance and configuration set to use.

The server handles authentication and provides formatted responses for easy integration.
""".strip()

# Initialize the MCP server
mcp = FastMCP(
    "mcp_oracle_scm",
    instructions=instructions
)


######## ORDER MANAGMENT TOOLS BELOW ##########

@mcp.tool()
async def get_order_count(
    offset_days: Optional[int] = None,
        p_bu: Optional[str] = None,
        p_source: Optional[str] = None,
        p_order_type: Optional[str] = None
) -> Dict[str, Any]:
    """Get total order count by running Oracle BI Report.
    
    Args:
        offset_days: Number of days to look back (default: 7)
        p_bu: Business Unit to filter by (default: 'US')
        p_source: Order source to filter by (default: 'SHOP')
        p_order_type: Order type to filter by (default: 'ECOM_NORMAL_SHIPONLY')
    """
    try:
        Logger.log("Getting order count",
                  level="INFO",
                  offset_days=offset_days,
                  p_bu=p_bu,
                  p_source=p_source,
                  p_order_type=p_order_type)
        
        oracle_om = get_oracle_om()
        result = await oracle_om.get_order_count(
            offset_days=offset_days,
            p_bu=p_bu,
            p_source=p_source,
            p_order_type=p_order_type
        )
        
        Logger.log("Order count retrieved",
                  level="INFO",
                  parameters={
                      "offset_days": offset_days,
                      "p_bu": p_bu,
                      "p_source": p_source,
                      "p_order_type": p_order_type
                  })
        
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


@mcp.tool()
async def check_single_order_details(
    order_number: str
) -> Dict[str, Any]:
    """Get details for a specific order number.
    
    This tool searches for an order using OrderNumber, CustomerPONumber, or SourceTransactionNumber
    and returns simplified details for the first match found.
    
    Args:
        order_number: The order number to search for
    """
    try:
        Logger.log("Checking single order details",
                  level="INFO",
                  order_number=order_number)
        
        oracle_om = get_oracle_om()
        result = await oracle_om.check_single_order_details(order_number)
        
        Logger.log("Order details retrieved",
                  level="INFO",
                  order_number=order_number,
                  found_items=len(result.get("items", [])))
        
        return result
    except Exception as e:
        Logger.log("Error checking order details",
                  level="ERROR",
                  error=str(e),
                  order_number=order_number)
        return {
            "error": str(e),
            "error_type": type(e).__name__,
            "order_number": order_number
        }


@mcp.tool()
async def get_order_line_summary(
    offset_days: Optional[int] = None,
    p_sku: Optional[str] = None,
    p_warehouse: Optional[str] = None
) -> Dict[str, Any]:
    """Get order line summary by running Oracle BI Report.
    
    This tool runs a BI report to get order line data and summarizes the results by warehouse.
    
    Args:
        offset_days: Number of days to look back (default: 7)
        p_sku: Optional SKU number to filter by (e.g., 'A-SKU-0525')
        p_warehouse: Optional warehouse code to filter by (e.g., 'MLU', 'CVU')
    """
    try:
        Logger.log("Getting order line summary",
                  level="INFO",
                  offset_days=offset_days,
                  p_sku=p_sku,
                  p_warehouse=p_warehouse)
        
        # Get the Oracle Order Manager
        oracle_om = get_oracle_om()
        
        # Call the method with explicit parameter names to ensure they're passed correctly
        result = await oracle_om.get_order_line_summary(
            offset_days=offset_days,
            p_sku=p_sku,
            p_warehouse=p_warehouse
        )
        
        Logger.log("Order line summary retrieved",
                  level="INFO",
                  parameters={
                      "offset_days": offset_days,
                      "p_sku": p_sku,
                      "p_warehouse": p_warehouse
                  })
        
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

@mcp.tool()
async def get_open_orders(
    offset_days: Optional[int] = None,
    p_sku: Optional[str] = None,
    p_warehouse: Optional[str] = None
) -> Dict[str, Any]:
    """Get open orders summary by running Oracle BI Report.
    
    This tool runs a BI report to get open order data and summarizes the results by warehouse.
    
    Args:
        offset_days: Number of days to look back (default: 7)
        p_sku: Optional SKU number to filter by (e.g., 'A-SKU-0525')
        p_warehouse: Optional warehouse code to filter by (e.g., 'MLU', 'CVU')
    """
    try:
        Logger.log("Getting open orders",
                  level="INFO",
                  offset_days=offset_days,
                  p_sku=p_sku,
                  p_warehouse=p_warehouse)
        
        oracle_om = get_oracle_om()
        result = await oracle_om.get_open_orders(
            offset_days=offset_days,
            p_sku=p_sku,
            p_warehouse=p_warehouse
        )
        
        Logger.log("Open orders retrieved",
                  level="INFO",
                  parameters={
                      "offset_days": offset_days,
                      "p_sku": p_sku,
                      "p_warehouse": p_warehouse
                  })
        
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

@mcp.tool()
async def extract_order_line_details(
    offset_days: Optional[int] = None,
    p_sku: Optional[str] = None,
    p_warehouse: Optional[str] = None
) -> Dict[str, Any]:
    """Extract order line details by running Oracle BI Report.
    
    This tool runs a BI report to get detailed order line data, saves it to a CSV file and read data to answer customer's question.
    
    Args:
        offset_days: Number of days to look back (default: 7)
        p_sku: Optional SKU number to filter by (e.g., 'A-SKU-0525')
        p_warehouse: Optional warehouse code to filter by (e.g., 'MLU', 'CVU')
    """
    try:
        Logger.log("Extracting order line details",
                  level="INFO",
                  offset_days=offset_days,
                  p_sku=p_sku,
                  p_warehouse=p_warehouse)
        
        oracle_om = get_oracle_om()
        result = await oracle_om.extract_order_line_details(
            offset_days=offset_days,
            p_sku=p_sku,
            p_warehouse=p_warehouse
        )
        
        Logger.log("Order line details extracted",
                  level="INFO",
                  parameters={
                      "offset_days": offset_days,
                      "p_sku": p_sku,
                      "p_warehouse": p_warehouse
                  })
        
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

@mcp.tool()
async def get_back_orders(
    p_from_sales_ord_date: Optional[str] = None,
    p_to_sales_ord_date: Optional[str] = None,
    p_warehouse: Optional[str] = None,
    p_item: Optional[str] = None
) -> Dict[str, Any]:
    """Get back orders by running Oracle BI Report.
    
    This tool runs a BI report to get back order data and summarizes the results.
    
    Args:
        p_from_sales_ord_date: From date in format 'MM-DD-YYYY' (e.g., '02-24-1924')
        p_to_sales_ord_date: To date in format 'MM-DD-YYYY' (e.g., '02-24-1925')
        p_warehouse: Optional warehouse code to filter by (e.g., 'CVU')
        p_item: Optional SKU number to filter by (e.g., 'A-SKU-0525')
    """
    try:
        Logger.log("Getting back orders",
                  level="INFO",
                  p_from_sales_ord_date=p_from_sales_ord_date,
                  p_to_sales_ord_date=p_to_sales_ord_date,
                  p_warehouse=p_warehouse,
                  p_item=p_item)
        
        oracle_om = get_oracle_om()
        result = await oracle_om.get_back_orders(
            p_from_sales_ord_date=p_from_sales_ord_date,
            p_to_sales_ord_date=p_to_sales_ord_date,
            p_warehouse=p_warehouse,
            p_item=p_item
        )
        
        Logger.log("Back orders retrieved",
                  level="INFO",
                  parameters={
                      "p_from_sales_ord_date": p_from_sales_ord_date,
                      "p_to_sales_ord_date": p_to_sales_ord_date,
                      "p_warehouse": p_warehouse,
                      "p_item": p_item
                  })
        
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

######## INVENTORY MANAGEMENT TOOLS BELOW ##########        

@mcp.tool()
async def lookup_inventory_transactions(
    p_wh_code: str,
    p_date_start: str,
    p_date_end: str,
    p_item: Optional[str] = None,
    p_subinventory_code: Optional[str] = None,
    p_transaction_type: Optional[str] = None
) -> Dict[str, Any]:
    """Look up inventory transactions using the Oracle BI Report.
    
    Args:
        p_wh_code: Warehouse code to filter by (organization code)
        p_date_start: Start date for transactions (format: MM-DD-YYYY)
        p_date_end: End date for transactions (format: MM-DD-YYYY)
        p_item: Optional item number to filter by
        p_subinventory_code: Optional subinventory code to filter by
        p_transaction_type: Optional transaction type to filter by
    """
    try:
        inventory_manager = get_oracle_inventory_manager()
        return await inventory_manager.lookup_inventory_transactions(
            p_wh_code, p_date_start, p_date_end, p_item, p_subinventory_code, p_transaction_type
        )
    except Exception as e:
        logger.error(f"Error in lookup_inventory_transactions: {str(e)}")
        return {
            "error": str(e),
            "error_type": type(e).__name__,
            "parameters": {
                "p_wh_code": p_wh_code,
                "p_date_start": p_date_start,
                "p_date_end": p_date_end,
                "p_item": p_item,
                "p_subinventory_code": p_subinventory_code,
                "p_transaction_type": p_transaction_type
            }
        }

@mcp.tool()
async def lookup_inventory_transaction_details(
    p_wh_code: str,
    p_date_start: str,
    p_date_end: str,
    p_item_number: Optional[str] = None,
    p_subinventory_code: Optional[str] = None,
    p_transaction_type: Optional[str] = None
) -> Dict[str, Any]:
    """Look up detailed inventory transactions using the Oracle BI Report.
    
    Args:
        p_wh_code: Warehouse code to filter by (organization code)
        p_date_start: Start date for transactions (format: MM-DD-YYYY)
        p_date_end: End date for transactions (format: MM-DD-YYYY)
        p_item_number: Optional item number to filter by
        p_subinventory_code: Optional subinventory code to filter by
        p_transaction_type: Optional transaction type to filter by
    """
    try:
        inventory_manager = get_oracle_inventory_manager()
        return await inventory_manager.lookup_inventory_transaction_details(
            p_wh_code, p_date_start, p_date_end, p_item_number, p_subinventory_code, p_transaction_type
        )
    except Exception as e:
        logger.error(f"Error in lookup_inventory_transaction_details: {str(e)}")
        return {
            "error": str(e),
            "error_type": type(e).__name__,
            "parameters": {
                "p_wh_code": p_wh_code,
                "p_date_start": p_date_start,
                "p_date_end": p_date_end,
                "p_item_number": p_item_number,
                "p_subinventory_code": p_subinventory_code,
                "p_transaction_type": p_transaction_type
            }
        }

@mcp.tool()
async def lookup_inventory_summary(
    p_wh_code: str,
    p_date_start: str,
    p_date_end: str,
    p_item: Optional[str] = None,
    p_subinventory_code: Optional[str] = None
) -> Dict[str, Any]:
    """Look up enhanced inventory summary report with balance information.
    
    Args:
        p_wh_code: Warehouse code to filter by (organization code)
        p_date_start: Start date for transactions (format: MM-DD-YYYY)
        p_date_end: End date for transactions (format: MM-DD-YYYY)
        p_item: Optional item number to filter by
        p_subinventory_code: Optional subinventory code to filter by
    """
    try:
        inventory_manager = get_oracle_inventory_manager()
        return await inventory_manager.lookup_inventory_summary(
            p_wh_code, p_date_start, p_date_end, p_item, p_subinventory_code
        )
    except Exception as e:
        logger.error(f"Error in lookup_inventory_summary: {str(e)}")
        return {
            "error": str(e),
            "error_type": type(e).__name__,
            "parameters": {
                "p_wh_code": p_wh_code,
                "p_date_start": p_date_start,
                "p_date_end": p_date_end,
                "p_item": p_item,
                "p_subinventory_code": p_subinventory_code
            }
        }

######## PROCUREMENT TOOLS BELOW ##########

@mcp.tool()
async def get_po_summary(
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
    """Get purchase order summary for a specific year using Oracle BI Report.
    
    This tool retrieves a comprehensive summary of purchase orders with various filter options.
    
    Args:
        year: Optional[int] - The year to filter POs (e.g., 2025)
        P_MPN: Optional[str] - Manufacturer Part Number filter
        P_Month: Optional[int] - Month number filter (1-12)
        P_ITEM: Optional[str] - Item number filter
        P_PONUM: Optional[str] - Purchase Order Number filter
        P_DOC_STATUS: Optional[str] - Document Status filter
        P_REQ_NUM: Optional[str] - Requisition Number filter
        P_CATEGORY: Optional[Union[str, List[str]]] - Category code filter(s)
        P_SUPPLIER: Optional[str] - Supplier name filter
        P_REQUESTER: Optional[str] - Requester name filter
        P_MANUFACTURER: Optional[str] - Manufacturer name filter
        
    Returns:
        Dictionary containing:
        - total_results: Number of POs found
        - summary: Summary statistics
        - items: List of PO summaries
        - execution_time: Time taken to execute the query
        - parameters_used: Parameters used in the query
        - error: Error information if any
    """
    try:
        oracle_proc = get_oracle_procurement()
        return await oracle_proc.get_po_summary(
            year=year,
            P_MPN=P_MPN,            
            P_Month=P_Month,
            P_ITEM=P_ITEM,
            P_PONUM=P_PONUM,
            P_DOC_STATUS=P_DOC_STATUS,
            P_REQ_NUM=P_REQ_NUM,
            P_CATEGORY=P_CATEGORY,
            P_SUPPLIER=P_SUPPLIER,
            P_REQUESTER=P_REQUESTER,
            P_MANUFACTURER=P_MANUFACTURER
        )
    except Exception as e:
        
        Logger.log("Error in get_po_summary:",
                  level="INFO",
                  parameters={
                      "year": year,
                "P_MPN": P_MPN,                
                "P_Month": P_Month,
                "P_ITEM": P_ITEM,
                "P_PONUM": P_PONUM,
                "P_DOC_STATUS": P_DOC_STATUS,
                "P_REQ_NUM": P_REQ_NUM,
                "P_CATEGORY": P_CATEGORY,
                "P_SUPPLIER": P_SUPPLIER,
                "P_REQUESTER": P_REQUESTER,
                "P_MANUFACTURER": P_MANUFACTURER
                  })
        return {
            "error": str(e),
            "error_type": type(e).__name__,
            "parameters": {
                "year": year,
                "P_MPN": P_MPN,                
                "P_Month": P_Month,
                "P_ITEM": P_ITEM,
                "P_PONUM": P_PONUM,
                "P_DOC_STATUS": P_DOC_STATUS,
                "P_REQ_NUM": P_REQ_NUM,
                "P_CATEGORY": P_CATEGORY,
                "P_SUPPLIER": P_SUPPLIER,
                "P_REQUESTER": P_REQUESTER,
                "P_MANUFACTURER": P_MANUFACTURER
            }
        }

@mcp.tool()
async def get_po_details(
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
    P_PROC_BU: Optional[str] = None,
    P_BILL_TO: Optional[str] = None
) -> Dict[str, Any]:
    """Get detailed purchase order information for a specific year using Oracle BI Report.
    
    This tool retrieves detailed information about purchase orders with various filter options.
    
    Args:
        year: Required 4-digit year (e.g., 2025)
        P_MPN: Optional manufacturer part number filter
        P_Month: Optional month number filter (1-12)
        P_ITEM: Optional item number filter
        P_PONUM: Optional purchase order number filter
        P_DOC_STATUS: Optional document status filter
        P_REQ_NUM: Optional requisition number filter
        P_CATEGORY: Optional category code filter (string or list of strings)
        P_SUPPLIER: Optional supplier name filter
        P_REQUESTER: Optional requester name filter
        P_MANUFACTURER: Optional manufacturer name filter
        P_BUYER: Optional buyer name filter
        P_SHIP_TO: Optional ship-to location filter
        P_BILL_TO: Optional bill-to location filter
        P_PROC_BU: Optional Procurement BU location filter
    
    Returns:
        Dictionary containing:
        - total_results: Number of records found
        - items: List of PO details including:
          * Business unit information
          * Supplier details
          * PO line items
          * Invoice and payment details
          * Change order information
        - execution_time: Time taken to execute the query
        - parameters_used: Parameters used in the query
        - error: Error information if any
    """
    try:
        oracle_proc = get_oracle_procurement()
        return await oracle_proc.get_po_details(
            year=year,
            P_MPN=P_MPN,            
            P_Month=P_Month,
            P_ITEM=P_ITEM,
            P_PONUM=P_PONUM,
            P_DOC_STATUS=P_DOC_STATUS,
            P_PROC_BU=P_PROC_BU,
            P_REQ_NUM=P_REQ_NUM,
            P_CATEGORY=P_CATEGORY,
            P_SUPPLIER=P_SUPPLIER,
            P_REQUESTER=P_REQUESTER,
            P_MANUFACTURER=P_MANUFACTURER,
            P_BUYER=P_BUYER,
            P_SHIP_TO=P_SHIP_TO,
            P_BILL_TO=P_BILL_TO
        )
    except Exception as e:        
        Logger.log("Error in get_po_details:",
                  level="INFO",
                  parameters={
                "year": year,
                "P_MPN": P_MPN,                
                "P_Month": P_Month,
                "P_ITEM": P_ITEM,
                "P_PONUM": P_PONUM,
                "P_DOC_STATUS": P_DOC_STATUS,
                "P_PROC_BU": P_PROC_BU,
                "P_REQ_NUM": P_REQ_NUM,
                "P_CATEGORY": P_CATEGORY,
                "P_SUPPLIER": P_SUPPLIER,
                "P_REQUESTER": P_REQUESTER,
                "P_MANUFACTURER": P_MANUFACTURER,
                "P_BUYER": P_BUYER,
                "P_SHIP_TO": P_SHIP_TO,
                "P_BILL_TO": P_BILL_TO
                  })
        return {
            "error": str(e),
            "error_type": type(e).__name__,
            "parameters": {
                "year": year,
                "P_MPN": P_MPN,                
                "P_Month": P_Month,
                "P_ITEM": P_ITEM,
                "P_PONUM": P_PONUM,
                "P_DOC_STATUS": P_DOC_STATUS,
                "P_PROC_BU": P_PROC_BU,
                "P_REQ_NUM": P_REQ_NUM,
                "P_CATEGORY": P_CATEGORY,
                "P_SUPPLIER": P_SUPPLIER,
                "P_REQUESTER": P_REQUESTER,
                "P_MANUFACTURER": P_MANUFACTURER,
                "P_BUYER": P_BUYER,
                "P_SHIP_TO": P_SHIP_TO,
                "P_BILL_TO": P_BILL_TO
            }
        }

@mcp.tool()
async def get_pr_po_apprvl_dtls(
    Doc_No: Optional[str] = None,
    Doc_Type: Optional[str] = None,
    BU: Optional[str] = None,
    SKU: Optional[str] = None,
    Supplier: Optional[str] = None,
    Creator: Optional[str] = None
) -> Dict[str, Any]:
    """Get approval details for PR/PO documents using Oracle BI Report.
    
    This tool retrieves approval details for Purchase Requisitions and Purchase Orders
    with various filter options.
    
    Args:
        Doc_No: Optional document number filter
        Doc_Type: Optional document type filter (PR or PO)
        BU: Optional business unit filter
        SKU: Optional SKU number filter
        Supplier: Optional supplier name filter
        Creator: Optional document creator filter
    
    Returns:
        Dictionary containing:
        - total_results: Number of records found
        - items: List of approval details including:
          * Document information (number, type, status)
          * Business unit
          * Creator and creation date
          * Supplier and SKU details
          * Approval information (approver, status, date, level)
        - execution_time: Time taken to execute the query
        - parameters_used: Parameters used in the query
        - formatted_tables: Markdown formatted tables of the results
        - error: Error information if any
    """
    try:
        oracle_proc = get_oracle_procurement()
        return await oracle_proc.get_pr_po_apprvl_dtls(
            Doc_No=Doc_No,
            Doc_Type=Doc_Type,
            BU=BU,
            SKU=SKU,
            Supplier=Supplier,
            Creator=Creator
        )
    except Exception as e:
        
        Logger.log("Error in get_pr_po_apprvl_dtls:",
                  level="INFO",
                  parameters={
                "Doc_No": Doc_No,
                "Doc_Type": Doc_Type,
                "BU": BU,
                "SKU": SKU,
                "Supplier": Supplier,
                "Creator": Creator
                  })
        return {
            "error": str(e),
            "error_type": type(e).__name__,
            "parameters": {
                "Doc_No": Doc_No,
                "Doc_Type": Doc_Type,
                "BU": BU,
                "SKU": SKU,
                "Supplier": Supplier,
                "Creator": Creator
            }
        }
@mcp.tool()
async def get_supplier_configs(
    supplier: Optional[str] = None
) -> Dict[str, Any]:
    """Get supplier configurations and settings from Oracle BI Report.
    
    Args:
        supplier: Optional supplier name filter
        bu: Optional business unit filter
        sku: Optional SKU number filter
        p_supplier: Optional specific supplier name
        p_bu: Optional specific business unit
    
    Returns:
        Dictionary containing:
        - total_results: Number of configurations found
        - items: List of supplier configurations
        - execution_time: Time taken to execute the query
        - parameters_used: Parameters used in the query
        - formatted_tables: Markdown formatted tables of the results
        - error: Error information if any
    """
    try:
        oracle_proc = get_oracle_procurement()
        return await oracle_proc.get_supplier_configs(
            Supplier=supplier
        )
    except Exception as e:
        
        Logger.log("Error in get_supplier_configs:",
                  level="INFO",
                  parameters={
                "Supplier": supplier
                  })
        return {
            "error": str(e),
            "error_type": type(e).__name__,
            "parameters": {
               "Supplier": supplier
            }
        }
    
######## PRODUCT MANAGEMENT TOOLS BELOW ##########

@mcp.tool()
async def lookup_item_details(
    p_item_number: Optional[str],
    p_org: Optional[str],
    p_category: Optional[str],
    offset_days: Optional[int],
    p_d2c: Optional[str]
) -> Dict[str, Any]:
    """Look up item details using Oracle BI Report.
    
    This tool retrieves detailed information about items with various filter options.
    
    Args:
        p_item_number: Optional item number to filter by
        p_org: Optional organization/warehouse code to filter by
        p_category: Optional item category to filter by
        offset_days: Optional number of days to offset the search
        p_d2c: Optional filter for D2C enabled items ('Y' or 'N')
    
    Returns:
        Dictionary containing:
        - total_items: Number of items found
        - items: List of item details including:
          * Item Category
          * Item Number/SKU/Product
          * Description
          * Organization/Warehouse
          * Creation and update information
          * Ring fencing and D2C status
          * SKU Price
        - grouped_items: Items grouped by category and warehouse
        - parameters_used: Parameters used in the query
        - error: Error information if any
    """
    try:
        Logger.log("Looking up item details",
                  level="INFO",
                  p_item_number=p_item_number,
                  p_org=p_org,
                  p_category=p_category,
                  offset_days=offset_days,
                  p_d2c=p_d2c)
        
        item_service = get_item_service()
        result = await item_service.lookup_item_details(
            p_item_number=p_item_number,
            p_org=p_org,
            p_category=p_category,
            offset_days=offset_days,
            p_d2c=p_d2c
        )
        
        Logger.log("Item details retrieved",
                  level="INFO",
                  parameters={
                      "p_item_number": p_item_number,
                      "p_org": p_org,
                      "p_category": p_category,
                      "offset_days": offset_days,
                      "p_d2c": p_d2c
                  })
        
        return result
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
                "p_item_number": p_item_number,
                "p_org": p_org,
                "p_category": p_category,
                "offset_days": offset_days,
                "p_d2c": p_d2c
            }
        }


@mcp.resource("config://app")
def get_config() -> Dict[str, Any]:
    """Static configuration data"""
    env_config = get_env_config()
    return {
        "name": "Oracle SCM MCP Server",
        "version": "1.0.0",
        "description": "MCP server for Oracle SCM integration",
        "environment": {
            "current": env_config['env'],
            "available": ["DEV1", "TEST", "PROD"]
        },
        "modules": {
            "order_management": {
                "status": "active",
                "tools": {
                    "check_single_order_details": {
                        "description": "Get simplified details for a specific order number",
                        "features": [
                            "Searches using OrderNumber, CustomerPONumber, or SourceTransactionNumber",
                            "Returns simplified order details with key information",
                            "Includes API URL for debugging"
                        ]
                    },
                    "get_order_count": {
                        "description": "Get total order count based on search criteria (improved version)",
                        "features": [
                            "Supports filtering by date range, order type, source, and country",
                            "Returns only the count for performance optimization",
                            "Includes all parameters used in the search",
                            "Handles order source and country name mapping automatically"
                        ]
                    },
                    "extract_order_line_details": {
                        "description": "Extract order line details by running Oracle BI Report",
                        "features": [
                            "Runs BI report to get detailed order line data",
                            "Downloads report in CSV format",
                            "Supports filtering by SKU and warehouse",
                            "Provides summary statistics by warehouse and status"
                        ]
                    },
                    "get_order_line_summary": {
                        "description": "Get order line summary by running Oracle BI Report",
                        "features": [
                            "Runs BI report to get order line data",
                            "Summarizes results by warehouse and status",
                            "Supports filtering by SKU and warehouse",
                            "Shows quantities for SKU-specific queries"
                        ]
                    },
                    "get_open_orders": {
                        "description": "Get open orders summary by running Oracle BI Report",
                        "features": [
                            "Runs BI report to get open order data",
                            "Summarizes results by warehouse and SKU",
                            "Supports filtering by SKU and warehouse",
                            "Shows order counts and quantities"
                        ]
                    },
                    "get_back_orders": {
                        "description": "Get back orders summary by running Oracle BI Report",
                        "features": [
                            "Runs BI report to get back order data",
                            "Summarizes results by warehouse and SKU",
                            "Supports filtering by date range, SKU and warehouse",
                            "Shows back order counts and quantities"
                        ]
                    }
                }
            },
            "inventory_management": {
                "status": "active",
                "tools": {
                    "lookup_inventory_transactions": {
                        "description": "Look up inventory transactions using BI Report",
                        "features": [
                            "Supports filtering by warehouse, date range, item, subinventory, and transaction type",
                            "Returns both pending and completed transactions",
                            "Provides summary statistics and grouped data"
                        ]
                    },
                    "lookup_inventory_transaction_details": {
                        "description": "Look up detailed inventory transactions using BI Report",
                        "features": [
                            "Supports filtering by warehouse, date range, item, subinventory, and transaction type",
                            "Returns detailed transaction information",
                            "Complements the summary view with transaction-level details"
                        ]
                    },
                    "lookup_inventory_summary": {
                        "description": "Look up enhanced inventory summary with balance information",
                        "features": [
                            "Supports filtering by warehouse, date range, item, and subinventory",
                            "Returns opening/ending balances, receipts, shipments, and adjustments",
                            "Provides summary statistics and grouped data by item and subinventory"
                        ]
                    }
                }
            },
            "procurement": {
                 "status": "active",
                "tools": {
                    "get_po_summary": {
                        "description": "Get purchase order summary for a specific year",
                        "features": [
                            "Retrieves comprehensive summary of purchase orders",
                            "Supports filtering by year, MPN, month, item, PO number, status, and more",
                            "Returns summary statistics including total POs, requisitions, items",
                            "Provides formatted tables for easy data visualization",
                            "Includes supplier and business unit information"
                                    ]
                        },
                    "get_po_details": {
                        "description": "Get detailed purchase order information",
                        "features": [
                            "Retrieves detailed information about purchase orders",
                            "Includes business unit information, supplier details, PO line items",
                            "Shows invoice and payment details",
                            "Tracks change order information",
                            "Supports multiple filter options",
                            "Provides shipping and billing location details",
                            "Includes manufacturer and part number information"
                        ]
                    },
                    "get_pr_po_apprvl_dtls": {
                        "description": "Get approval details for PR/PO documents",
                        "features": [
                            "Retrieves approval workflow information for Purchase Requisitions and Orders",
                            "Shows document status and approval chain",
                            "Includes creator and creation date information",
                            "Displays supplier and SKU details",
                            "Tracks approval status, dates, and levels",
                            "Supports filtering by document number, type, BU, SKU, supplier, and creator"
                        ]
                    },
                    "get_supplier_configs": {
                        "description": "Get detailed supplier configurations and settings from Oracle SCM system",
                        "features": [
                              "Retrieves comprehensive supplier profile data and settings",
                              "Shows payment terms, methods, and financial configurations",
                              "Displays delivery terms, shipping methods, and operational settings",
                              "Includes compliance status, certifications, and risk metrics",
                              "Tracks business unit relationships and site-specific configurations",
                              "Shows EDI capabilities and system integration settings",
                              "Supports filtering by supplier name, business unit, and SKU",
                              "Provides formatted tables for easy data visualization",
                              "Returns execution time and parameter usage information",
                              "Handles multiple supplier locations and regional settings"
    ]
}
    }
},
            "product_management": {
                "status": "active",
                "tools": {
                    "lookup_item_details": {
                        "description": "Look up item details in Oracle SCM",
                        "features": [
                            "Retrieves detailed information about items",
                            "Supports filtering by item number, organization, category",
                            "Includes D2C and ring fencing status",
                            "Groups items by category and warehouse",
                            "Provides creation and update tracking",
                            "Shows SKU pricing information"
                        ]
                    }
                }
            },
            "supply_chain_planning": {
                "status": "planned",
                "tools": {}
            }
        }
    }

# If running directly, start the server
def main():
    mcp.run()

if __name__ == "__main__":
    main()
