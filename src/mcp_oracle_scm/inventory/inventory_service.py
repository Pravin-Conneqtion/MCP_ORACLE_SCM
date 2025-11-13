"""Oracle Inventory Management Module"""

from datetime import datetime
from typing import Optional, Dict, Any, Union, List
import csv
from mcp_oracle_scm.common.report_service import OracleReportService
from mcp_oracle_scm.config.environment import get_env_config
from mcp_oracle_scm.config.logger_config import LoggerConfig as Logger

class OracleInventoryManager:
    """Oracle Inventory Manager for inventory-related operations."""
    
    def __init__(self):
        """Initialize using environment configuration."""
        config = get_env_config()
        self.base_url = config['base_url']
        Logger.log("Initialized OracleInventoryManager",
                  level="INFO",
                  base_url=self.base_url)
        
    async def lookup_inventory_summary(
        self,
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
            start_time = datetime.now()
            Logger.log("Looking up inventory summary",
                      level="INFO",
                      p_wh_code=p_wh_code,
                      p_date_start=p_date_start,
                      p_date_end=p_date_end,
                      p_item=p_item,
                      p_subinventory_code=p_subinventory_code)
            
            # Initialize report service
            report_service = OracleReportService()
            
            # Get report path for inventory summary
            report_path = "/Custom/Square SCM Reports/Block MCP/Inventory Management/Block Inventory Transactions Summary Report.xdo"
            
            # Prepare parameters
            parameters = {
                'P_WH_CODE': p_wh_code,
                'P_DATE_START': p_date_start,
                'P_DATE_END': p_date_end
            }
            
            # Add optional parameters if provided
            if p_item is not None:
                parameters['P_ITEM'] = p_item
            if p_subinventory_code is not None:
                parameters['P_SUBINVENTORY_CODE'] = p_subinventory_code
                
            Logger.log("Report parameters prepared",
                      level="INFO",
                      parameters=parameters)
                
            # Run report and get file path
            report_file = await report_service.get_report_data(report_path, parameters)
            Logger.log("Processing inventory summary report",
                      level="INFO",
                      file=report_file)
            
            # Process the report file
            inventory_items = []
            
            try:
                with open(report_file, 'r', newline='', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        try:
                            # Process each inventory item row
                            inventory_item = self._process_inventory_summary_row(row)
                            inventory_items.append(inventory_item)
                                
                        except Exception as e:
                            Logger.log("Error processing inventory summary row",
                                     level="ERROR",
                                     error=str(e),
                                     row=row)
                            continue
                            
            except Exception as e:
                Logger.log("Error reading inventory summary file",
                          level="ERROR",
                          error=str(e),
                          file=report_file)
                raise
            
            # Calculate summary statistics
            total_items = len(inventory_items)
            total_opening_balance = sum(float(item.get('opening_on_hand_balance', 0) or 0) for item in inventory_items)
            total_ending_balance = sum(float(item.get('ending_on_hand_balance', 0) or 0) for item in inventory_items)
            total_receipts = sum(float(item.get('total_receipts', 0) or 0) for item in inventory_items)
            total_shipments = sum(float(item.get('total_shipments', 0) or 0) for item in inventory_items)
            total_adjustments = sum(float(item.get('total_adjustments', 0) or 0) for item in inventory_items)
            
            # Group inventory items by subinventory
            grouped_items = self._group_inventory_items(inventory_items)
            
            elapsed = (datetime.now() - start_time).total_seconds()
            Logger.log("Inventory summary completed",
                      level="INFO",
                      elapsed_seconds=elapsed,
                      total_items=total_items)
            
            # Create response with summary and details
            return {
                "total_results": total_items,
                "summary": {
                    "total_items": total_items,
                    "total_opening_balance": total_opening_balance,
                    "total_ending_balance": total_ending_balance,
                    "total_receipts": total_receipts,
                    "total_shipments": total_shipments,
                    "total_adjustments": total_adjustments,
                    "warehouse_code": p_wh_code,
                    "date_range": f"{p_date_start} to {p_date_end}"
                },
                "parameters_used": {
                    "p_wh_code": p_wh_code,
                    "p_date_start": p_date_start,
                    "p_date_end": p_date_end,
                    "p_item": p_item,
                    "p_subinventory_code": p_subinventory_code
                },
                "grouped_items": grouped_items,
                "inventory_items": inventory_items
            }
            
        except Exception as e:
            Logger.log("Error looking up inventory summary",
                      level="ERROR",
                      error=str(e),
                      parameters={
                          "p_wh_code": p_wh_code,
                          "p_date_start": p_date_start,
                          "p_date_end": p_date_end,
                          "p_item": p_item,
                          "p_subinventory_code": p_subinventory_code
                      })
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

    async def lookup_inventory_transactions(
        self,
        p_wh_code: str,
        p_date_start: str,
        p_date_end: str,
        p_item: Optional[str] = None,
        p_subinventory_code: Optional[str] = None,
        p_transaction_type: Optional[str] = None
    ) -> Dict[str, Any]:
        """Look up inventory transactions summary using the Oracle BI Report.
        
        Args:
            p_wh_code: Warehouse code to filter by (organization code)
            p_date_start: Start date for transactions (format: MM-DD-YYYY)
            p_date_end: End date for transactions (format: MM-DD-YYYY)
            p_item: Optional item number to filter by
            p_subinventory_code: Optional subinventory code to filter by
            p_transaction_type: Optional transaction type to filter by
        """
        try:
            start_time = datetime.now()
            Logger.log("Looking up inventory transactions",
                      level="INFO",
                      p_wh_code=p_wh_code,
                      p_date_start=p_date_start,
                      p_date_end=p_date_end,
                      p_item=p_item,
                      p_subinventory_code=p_subinventory_code,
                      p_transaction_type=p_transaction_type)
            
            # Initialize report service
            report_service = OracleReportService()
            
            # Get report path for inventory transactions
            report_path = "/Custom/Square SCM Reports/Block MCP/Inventory Management/Block Inventory Transactions Summary Report.xdo"
            
            # Prepare parameters
            parameters = {
                'P_WH_CODE': p_wh_code,
                'P_DATE_START': p_date_start,
                'P_DATE_END': p_date_end
            }
            
            # Add optional parameters if provided
            if p_item is not None:
                parameters['P_ITEM'] = p_item
            if p_subinventory_code is not None:
                parameters['P_SUBINVENTORY_CODE'] = p_subinventory_code
            if p_transaction_type is not None:
                parameters['P_TRANSACTION_TYPE'] = p_transaction_type
                
            Logger.log("Report parameters prepared",
                      level="INFO",
                      parameters=parameters)
                
            # Run report and get file path
            report_file = await report_service.get_report_data(report_path, parameters)
            Logger.log("Processing report file",
                      level="INFO",
                      file=report_file)
            
            # Process the report file
            transactions = []
            pending_count = 0
            completed_count = 0
            pending_quantity = 0
            completed_quantity = 0
            
            try:
                with open(report_file, 'r', newline='', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        try:
                            # Process each transaction row
                            transaction = self._process_transaction_row(row)
                            transactions.append(transaction)
                            
                            # Update counters based on transaction status
                            if transaction['transaction_status'] == 'Pending':
                                pending_count += 1
                                pending_quantity += float(transaction.get('transaction_quantity', 0) or 0)
                            else:  # Completed
                                completed_count += 1
                                completed_quantity += float(transaction.get('transaction_quantity', 0) or 0)
                                
                        except Exception as e:
                            Logger.log("Error processing transaction row",
                                     level="ERROR",
                                     error=str(e),
                                     row=row)
                            continue
                            
            except Exception as e:
                Logger.log("Error reading report file",
                          level="ERROR",
                          error=str(e),
                          file=report_file)
                raise
            
            # Calculate summary statistics
            total_count = pending_count + completed_count
            total_quantity = pending_quantity + completed_quantity
            
            # Group transactions by item and subinventory
            grouped_transactions = self._group_transactions(transactions)
            
            elapsed = (datetime.now() - start_time).total_seconds()
            Logger.log("Inventory transactions completed",
                      level="INFO",
                      elapsed_seconds=elapsed,
                      total_transactions=total_count,
                      pending_count=pending_count,
                      completed_count=completed_count)
            
            # Create response with summary and details
            return {
                "total_results": total_count,
                "summary": {
                    "total_transactions": total_count,
                    "pending_transactions": pending_count,
                    "completed_transactions": completed_count,
                    "total_quantity": total_quantity,
                    "pending_quantity": pending_quantity,
                    "completed_quantity": completed_quantity,
                    "warehouse_code": p_wh_code,
                    "date_range": f"{p_date_start} to {p_date_end}"
                },
                "parameters_used": {
                    "p_wh_code": p_wh_code,
                    "p_date_start": p_date_start,
                    "p_date_end": p_date_end,
                    "p_item": p_item,
                    "p_subinventory_code": p_subinventory_code,
                    "p_transaction_type": p_transaction_type
                },
                "grouped_transactions": grouped_transactions,
                "transactions": transactions
            }
            
        except Exception as e:
            Logger.log("Error looking up inventory transactions",
                      level="ERROR",
                      error=str(e),
                      parameters={
                          "p_wh_code": p_wh_code,
                          "p_date_start": p_date_start,
                          "p_date_end": p_date_end,
                          "p_item": p_item,
                          "p_subinventory_code": p_subinventory_code,
                          "p_transaction_type": p_transaction_type
                      })
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
    
    def _process_transaction_row(self, row: Dict[str, str]) -> Dict[str, Any]:
        """Process a single transaction row from the report.
        
        Args:
            row: A dictionary representing a row from the CSV report
            
        Returns:
            A processed transaction dictionary with standardized fields
        """
        try:
            # Convert empty strings to None
            processed_row = {k: (v if v != "" else None) for k, v in row.items()}
            
            # Convert numeric fields
            if 'TRANSACTION_QUANTITY' in processed_row and processed_row['TRANSACTION_QUANTITY']:
                try:
                    processed_row['TRANSACTION_QUANTITY'] = float(processed_row['TRANSACTION_QUANTITY'])
                except (ValueError, TypeError):
                    Logger.log("Invalid transaction quantity",
                             level="WARNING",
                             value=processed_row['TRANSACTION_QUANTITY'])
                    
            if 'PRIMARY_QUANTITY' in processed_row and processed_row['PRIMARY_QUANTITY']:
                try:
                    processed_row['PRIMARY_QUANTITY'] = float(processed_row['PRIMARY_QUANTITY'])
                except (ValueError, TypeError):
                    Logger.log("Invalid primary quantity",
                             level="WARNING",
                             value=processed_row['PRIMARY_QUANTITY'])
            
            # Standardize field names (convert to lowercase)
            return {k.lower(): v for k, v in processed_row.items()}
            
        except Exception as e:
            Logger.log("Error processing transaction row",
                      level="ERROR",
                      error=str(e),
                      row=row)
            raise
    
    def _group_transactions(self, transactions: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Group transactions by item and subinventory.
        
        Args:
            transactions: List of transaction dictionaries
            
        Returns:
            A dictionary with transactions grouped by item and subinventory
        """
        try:
            by_item = {}
            by_subinventory = {}
            by_transaction_type = {}
            
            # Group by item
            for transaction in transactions:
                item = transaction.get('item_number')
                if item:
                    if item not in by_item:
                        by_item[item] = {
                            'pending_count': 0,
                            'completed_count': 0,
                            'pending_quantity': 0,
                            'completed_quantity': 0,
                            'total_count': 0,
                            'total_quantity': 0
                        }
                    
                    status = transaction.get('transaction_status')
                    quantity = float(transaction.get('transaction_quantity', 0) or 0)
                    
                    if status == 'Pending':
                        by_item[item]['pending_count'] += 1
                        by_item[item]['pending_quantity'] += quantity
                    else:  # Completed
                        by_item[item]['completed_count'] += 1
                        by_item[item]['completed_quantity'] += quantity
                    
                    by_item[item]['total_count'] += 1
                    by_item[item]['total_quantity'] += quantity
            
            # Group by subinventory
            for transaction in transactions:
                subinventory = transaction.get('subinventory_code')
                if subinventory:
                    if subinventory not in by_subinventory:
                        by_subinventory[subinventory] = {
                            'pending_count': 0,
                            'completed_count': 0,
                            'pending_quantity': 0,
                            'completed_quantity': 0,
                            'total_count': 0,
                            'total_quantity': 0
                        }
                    
                    status = transaction.get('transaction_status')
                    quantity = float(transaction.get('transaction_quantity', 0) or 0)
                    
                    if status == 'Pending':
                        by_subinventory[subinventory]['pending_count'] += 1
                        by_subinventory[subinventory]['pending_quantity'] += quantity
                    else:  # Completed
                        by_subinventory[subinventory]['completed_count'] += 1
                        by_subinventory[subinventory]['completed_quantity'] += quantity
                    
                    by_subinventory[subinventory]['total_count'] += 1
                    by_subinventory[subinventory]['total_quantity'] += quantity
            
            # Group by transaction type
            for transaction in transactions:
                txn_type = transaction.get('transaction_type_name')
                if txn_type:
                    if txn_type not in by_transaction_type:
                        by_transaction_type[txn_type] = {
                            'pending_count': 0,
                            'completed_count': 0,
                            'pending_quantity': 0,
                            'completed_quantity': 0,
                            'total_count': 0,
                            'total_quantity': 0
                        }
                    
                    status = transaction.get('transaction_status')
                    quantity = float(transaction.get('transaction_quantity', 0) or 0)
                    
                    if status == 'Pending':
                        by_transaction_type[txn_type]['pending_count'] += 1
                        by_transaction_type[txn_type]['pending_quantity'] += quantity
                    else:  # Completed
                        by_transaction_type[txn_type]['completed_count'] += 1
                        by_transaction_type[txn_type]['completed_quantity'] += quantity
                    
                    by_transaction_type[txn_type]['total_count'] += 1
                    by_transaction_type[txn_type]['total_quantity'] += quantity
            
            Logger.log("Transactions grouped successfully",
                      level="INFO",
                      item_count=len(by_item),
                      subinventory_count=len(by_subinventory),
                      transaction_type_count=len(by_transaction_type))
            
            return {
                'by_item': by_item,
                'by_subinventory': by_subinventory,
                'by_transaction_type': by_transaction_type
            }
            
        except Exception as e:
            Logger.log("Error grouping transactions",
                      level="ERROR",
                      error=str(e))
            raise
    
    def _process_inventory_summary_row(self, row: Dict[str, str]) -> Dict[str, Any]:
        """Process a single inventory summary row from the report.
        
        Args:
            row: A dictionary representing a row from the CSV report
            
        Returns:
            A processed inventory item dictionary with standardized fields
        """
        try:
            # Convert empty strings to None
            processed_row = {k: (v if v != "" else None) for k, v in row.items()}
            
            # Convert numeric fields
            numeric_fields = [
                'OPENING_ON_HAND_BALANCE', 'ENDING_ON_HAND_BALANCE', 
                'TOTAL_RECEIPTS', 'TOTAL_SHIPMENTS', 'TOTAL_ADJUSTMENTS'
            ]
            
            for field in numeric_fields:
                if field in processed_row and processed_row[field]:
                    try:
                        processed_row[field] = float(processed_row[field])
                    except (ValueError, TypeError):
                        Logger.log("Invalid numeric value",
                                 level="WARNING",
                                 field=field,
                                 value=processed_row[field])
            
            # Standardize field names (convert to lowercase)
            return {k.lower(): v for k, v in processed_row.items()}
            
        except Exception as e:
            Logger.log("Error processing inventory summary row",
                      level="ERROR",
                      error=str(e),
                      row=row)
            raise
    
    def _group_inventory_items(self, items: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Group inventory items by subinventory and item.
        
        Args:
            items: List of inventory item dictionaries
            
        Returns:
            A dictionary with items grouped by subinventory and item
        """
        try:
            by_subinventory = {}
            by_item = {}
            
            # Group by subinventory
            for item in items:
                subinventory = item.get('subinventory')
                if subinventory:
                    if subinventory not in by_subinventory:
                        by_subinventory[subinventory] = {
                            'opening_balance': 0,
                            'ending_balance': 0,
                            'receipts': 0,
                            'shipments': 0,
                            'adjustments': 0,
                            'item_count': 0
                        }
                    
                    by_subinventory[subinventory]['opening_balance'] += float(item.get('opening_on_hand_balance', 0) or 0)
                    by_subinventory[subinventory]['ending_balance'] += float(item.get('ending_on_hand_balance', 0) or 0)
                    by_subinventory[subinventory]['receipts'] += float(item.get('total_receipts', 0) or 0)
                    by_subinventory[subinventory]['shipments'] += float(item.get('total_shipments', 0) or 0)
                    by_subinventory[subinventory]['adjustments'] += float(item.get('total_adjustments', 0) or 0)
                    by_subinventory[subinventory]['item_count'] += 1
            
            # Group by item
            for item in items:
                item_number = item.get('item_number')
                if item_number:
                    if item_number not in by_item:
                        by_item[item_number] = {
                            'opening_balance': 0,
                            'ending_balance': 0,
                            'receipts': 0,
                            'shipments': 0,
                            'adjustments': 0,
                            'subinventory_count': 0
                        }
                    
                    by_item[item_number]['opening_balance'] += float(item.get('opening_on_hand_balance', 0) or 0)
                    by_item[item_number]['ending_balance'] += float(item.get('ending_on_hand_balance', 0) or 0)
                    by_item[item_number]['receipts'] += float(item.get('total_receipts', 0) or 0)
                    by_item[item_number]['shipments'] += float(item.get('total_shipments', 0) or 0)
                    by_item[item_number]['adjustments'] += float(item.get('total_adjustments', 0) or 0)
                    by_item[item_number]['subinventory_count'] += 1
            
            Logger.log("Inventory items grouped successfully",
                      level="INFO",
                      subinventory_count=len(by_subinventory),
                      item_count=len(by_item))
            
            return {
                'by_subinventory': by_subinventory,
                'by_item': by_item
            }
            
        except Exception as e:
            Logger.log("Error grouping inventory items",
                      level="ERROR",
                      error=str(e))
            raise
        
    async def lookup_inventory_transaction_details(
        self,
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
            start_time = datetime.now()
            Logger.log("Looking up inventory transaction details",
                      level="INFO",
                      p_wh_code=p_wh_code,
                      p_date_start=p_date_start,
                      p_date_end=p_date_end,
                      p_item_number=p_item_number,
                      p_subinventory_code=p_subinventory_code,
                      p_transaction_type=p_transaction_type)
            
            # Initialize report service
            report_service = OracleReportService()
            
            # Get report path for inventory transaction details
            report_path = "/Custom/Square SCM Reports/Block MCP/Inventory Management/Block Inventory Transaction Details Report.xdo"
            
            # Prepare parameters
            parameters = {
                'P_WH_CODE': p_wh_code,
                'P_DATE_START': p_date_start,
                'P_DATE_END': p_date_end
            }
            
            # Add optional parameters if provided
            if p_item_number is not None:
                parameters['P_ITEM_NUMBER'] = p_item_number
            if p_subinventory_code is not None:
                parameters['P_SUBINVENTORY_CODE'] = p_subinventory_code
            if p_transaction_type is not None:
                parameters['P_TRANSACTION_TYPE'] = p_transaction_type
                
            Logger.log("Report parameters prepared",
                      level="INFO",
                      parameters=parameters)
                
            # Run report and get file path
            report_file = await report_service.get_report_data(report_path, parameters)
            Logger.log("Processing details report file",
                      level="INFO",
                      file=report_file)
            
            # Process the report file
            transactions = []
            
            try:
                with open(report_file, 'r', newline='', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        try:
                            # Process each transaction row
                            transaction = self._process_transaction_row(row)
                            transactions.append(transaction)
                                
                        except Exception as e:
                            Logger.log("Error processing transaction detail row",
                                     level="ERROR",
                                     error=str(e),
                                     row=row)
                            continue
                            
            except Exception as e:
                Logger.log("Error reading details report file",
                          level="ERROR",
                          error=str(e),
                          file=report_file)
                raise
            
            # Calculate summary statistics
            total_count = len(transactions)
            
            elapsed = (datetime.now() - start_time).total_seconds()
            Logger.log("Transaction details lookup completed",
                      level="INFO",
                      elapsed_seconds=elapsed,
                      total_transactions=total_count)
            
            # Create response with details
            return {
                "total_results": total_count,
                "parameters_used": {
                    "p_wh_code": p_wh_code,
                    "p_date_start": p_date_start,
                    "p_date_end": p_date_end,
                    "p_item_number": p_item_number,
                    "p_subinventory_code": p_subinventory_code,
                    "p_transaction_type": p_transaction_type
                },
                "transaction_details": transactions
            }
            
        except Exception as e:
            Logger.log("Error looking up inventory transaction details",
                      level="ERROR",
                      error=str(e),
                      parameters={
                          "p_wh_code": p_wh_code,
                          "p_date_start": p_date_start,
                          "p_date_end": p_date_end,
                          "p_item_number": p_item_number,
                          "p_subinventory_code": p_subinventory_code,
                          "p_transaction_type": p_transaction_type
                      })
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

def get_oracle_inventory_manager() -> OracleInventoryManager:
    """Get configured Oracle Inventory Manager client."""
    try:
        return OracleInventoryManager()
    except Exception as e:
        Logger.log("Failed to initialize Oracle Inventory Manager",
                  level="ERROR",
                  error=str(e))
        raise Exception(
            "Failed to initialize Oracle Inventory Manager. Please check environment configuration."
        ) from e