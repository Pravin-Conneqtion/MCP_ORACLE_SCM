"""Order Management Utilities"""

from typing import Dict, Any, List
from datetime import datetime
from mcp_oracle_scm.config.logger_config import LoggerConfig as Logger

def process_order_report_row(row: Dict[str, str]) -> Dict[str, Any]:
    """Process a single row from the open orders report into a standardized format."""
    try:
        # Handle BOM in column names by finding the ORDER_DATE column
        order_date_key = next(key for key in row.keys() if key.endswith('ORDER_DATE'))
        
        # Convert date string to ISO format
        # Input format: "MM/DD/YYYY HH:MM:SS"
        order_date = datetime.strptime(row[order_date_key].strip('"'), "%m/%d/%Y %H:%M:%S")
        order_date_iso = order_date.isoformat()
        
        # Format line item
        line_item = {
            "item_number": row['ITEM_NUMBER'].strip('"'),
            "description": row['DESCRIPTION'].strip('"'),
            "ordered_quantity": int(row['ORDERED_QTY']),
            "warehouse": row['ORGANIZATION_CODE'].strip('"')
        }
        
        order_data = {
            "order_number": row['ORDER_NUMBER'].strip('"'),
            "order_date": order_date_iso,
            "order_id": row['ORDER_ID'].strip('"'),
            "source_order_id": row['SOURCE_ORDER_ID'].strip('"'),
            "shipping_method": row['SHIPPING_METHOD'].strip('"'),
            "line": line_item
        }
        
        Logger.log("Order report row processed",
                  level="DEBUG",
                  order_number=order_data["order_number"],
                  item_number=line_item["item_number"])
        
        return order_data
    except Exception as e:
        Logger.log("Error processing order report row",
                  level="ERROR",
                  error=str(e),
                  row_data=row)
        raise

def format_order_response(order_data: Dict[str, Any]) -> Dict[str, Any]:
    """Format order response data into a standardized structure."""
    try:
        Logger.log("Formatting order response",
                  level="DEBUG",
                  order_number=order_data.get('OrderNumber'))
        
        # Get all line statuses for analysis
        line_statuses = []
        line_warehouses = set()
        if 'lines' in order_data:
            for line in order_data.get('lines', []):
                if line.get('Status'):
                    line_statuses.append(line.get('Status'))
                if line.get('RequestedFulfillmentOrganizationCode'):
                    line_warehouses.add(line.get('RequestedFulfillmentOrganizationCode'))
        
        # Determine overall status from line statuses
        overall_status = "Unknown"
        status_counts = {}
        if line_statuses:
            # Count occurrences of each status
            for status in line_statuses:
                status_counts[status] = status_counts.get(status, 0) + 1
            
            # Logic to determine overall status based on line statuses
            if all(status == "Shipped" for status in line_statuses):
                overall_status = "Fully Shipped"
            elif all(status == "Cancelled" for status in line_statuses):
                overall_status = "Fully Cancelled"
            elif any(status == "Shipped" for status in line_statuses):
                overall_status = "Partially Shipped"
            elif any(status == "Not Started" for status in line_statuses):
                overall_status = "Not Started"
            else:
                overall_status = "In Progress"

        Logger.log("Order status determined",
                  level="DEBUG",
                  overall_status=overall_status,
                  status_counts=status_counts)

        formatted_order = {
            "order_number": order_data.get('OrderNumber'),
            "source_order_number": order_data.get('SourceTransactionNumber'),
            "source_transaction_system": order_data.get('SourceTransactionSystem'),
            "source_transaction_id": order_data.get('SourceTransactionId'),
            "purchase_order_number": order_data.get('CustomerPONumber'),
            "business_unit": order_data.get('BusinessUnitName', 'Not Assigned'),
            "status": {
                "header_status": order_data.get('Status', 'Unknown'),
                "order_status": overall_status,
                "line_status_details": status_counts or {}
            },
            "order_type": order_data.get('TransactionType', 'Standard'),
            "order_date": order_data.get('TransactionOn'),
            "created_date": order_data.get('CreationDate'),
            "created_by": order_data.get('CreatedBy'),
            "customer_info": {
                "party_name": order_data.get('BuyingPartyName'),
                "party_number": order_data.get('BuyingPartyNumber'),
                "contact_name": order_data.get('BuyingPartyContactName'),
                "contact_email": order_data.get('BuyingPartyContactEmail')
            }
        }

        # Add line items if present
        if 'lines' in order_data:
            formatted_order['lines'] = [{
                "line_number": line.get('LineNumber'),
                "product": {
                    "number": line.get('ProductNumber'),
                    "description": line.get('ProductDescription')
                },
                "quantity": {
                    "ordered": line.get('OrderedQuantity'),
                    "shipped": line.get('ShippedQuantity'),
                    "cancelled": line.get('CancelledQuantity'),
                    "uom": line.get('OrderedUOMCode')
                },
                "price": {
                    "unit": line.get('UnitSellingPrice'),
                    "total": line.get('LineTotalAmount')
                },
                "status": line.get('Status'),
                "status_code": line.get('StatusCode'),
                "fulfill_line_id": line.get('FulfillLineId'),
                "warehouse": line.get('RequestedFulfillmentOrganizationCode', 'Not Assigned'),
                "dates": {
                    "order_date": order_data.get('TransactionOn'),
                    "created_date": order_data.get('CreationDate'),
                    "requested_ship_date": line.get('RequestedShipDate'),
                    "schedule_ship_date": line.get('ScheduleShipDate'),
                    "fulfillment_date": line.get('FulfillmentDate'),
                    "last_update_date": line.get('LastUpdateDate')
                }
            } for line in order_data.get('lines', [])]

            # Add line summary
            formatted_order['line_summary'] = {
                "total_lines": len(formatted_order['lines']),
                "status_summary": status_counts,
                "warehouses": list(line_warehouses) if line_warehouses else ['Not Assigned'],
                "fulfillment_progress": {
                    "total_ordered_lines": len(line_statuses),
                    "shipped_lines": sum(1 for status in line_statuses if status == "Shipped"),
                    "cancelled_lines": sum(1 for status in line_statuses if status == "Cancelled"),
                    "in_progress_lines": sum(1 for status in line_statuses if status not in ["Shipped", "Cancelled", "Not Started"])
                }
            }

        Logger.log("Order response formatted",
                  level="DEBUG",
                  order_number=formatted_order["order_number"],
                  total_lines=formatted_order.get('line_summary', {}).get('total_lines', 0))

        return formatted_order

    except Exception as e:
        Logger.log("Error formatting order response",
                  level="ERROR",
                  error=str(e),
                  order_number=order_data.get('OrderNumber'))
        raise

def format_order_summary(orders: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Create a summary of multiple orders."""
    try:
        Logger.log("Creating order summary",
                  level="DEBUG",
                  order_count=len(orders))
        
        total_quantity = sum(order["total_quantity"] for order in orders)
        warehouses = set()
        items = set()
        for order in orders:
            warehouses.update(order["warehouses"])
            for line in order.get("lines", []):
                items.add(line["item_number"])

        summary = {
            "total_orders": len(orders),
            "total_quantity": total_quantity,
            "unique_warehouses": sorted(list(warehouses)),
            "unique_items": sorted(list(items))
        }

        Logger.log("Order summary created",
                  level="DEBUG",
                  total_orders=summary["total_orders"],
                  total_quantity=summary["total_quantity"],
                  warehouse_count=len(summary["unique_warehouses"]),
                  item_count=len(summary["unique_items"]))

        return summary
    except Exception as e:
        Logger.log("Error creating order summary",
                  level="ERROR",
                  error=str(e),
                  order_count=len(orders))
        raise