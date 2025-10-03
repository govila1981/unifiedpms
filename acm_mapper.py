"""
ACM Mapper Module - UPDATED WITH PROPER DATE FORMATTING
Trade Date remains datetime, Settle Date is simple date format
"""

import pandas as pd
import numpy as np
from datetime import datetime
try:
    from zoneinfo import ZoneInfo
except ImportError:
    import pytz
    ZoneInfo = lambda x: pytz.timezone(x)
    
from typing import Dict, List, Tuple, Optional
import logging
from pathlib import Path
import io

logger = logging.getLogger(__name__)


class ACMMapper:
    """Maps processed trades to ACM ListedTrades format - Updated Date Formatting"""
    
    # HARDCODED DEFAULT SCHEMA
    DEFAULT_COLUMNS = [
        "Trade Date",
        "Settle Date",
        "Account Id",
        "Counterparty Code",
        "Identifier",
        "Identifier Type",
        "Quantity",
        "Trade Price",
        "Price",
        "Instrument Type",
        "Strike Price",
        "Lot Size",
        "Strategy",
        "Executing Broker Name",
        "Trade Venue",
        "Notes",
        "Transaction Type",
        "Brokerage",
        "Taxes",
        "Comms",  # Pure brokerage from broker reconciliation
        "Broker Taxes",  # Taxes from broker reconciliation
        "Broker Trade Date"  # Trade date from broker file
    ]
    
    DEFAULT_MANDATORY = [
        "Account Id",
        "Identifier",
        "Quantity",
        "Transaction Type"
    ]
    
    # MAPPING RULES (for schema export)
    DEFAULT_MAPPINGS = {
        "Trade Date": "Current datetime (Singapore) - MM/DD/YYYY HH:MM:SS",
        "Settle Date": "Current date (Singapore) - MM/DD/YYYY",
        "Account Id": "Column 0 (Scheme)",
        "Counterparty Code": "Column 13 (CP Code)",
        "Identifier": "Bloomberg_Ticker",
        "Identifier Type": "Fixed: 'Bloomberg Yellow Key'",
        "Quantity": "abs(Column 12 - Lots Traded)",
        "Trade Price": "Column 3 (Avg Price)",
        "Price": "Column 3 (Avg Price)",
        "Instrument Type": "Column 4 (Instr)",
        "Strike Price": "Column 8",
        "Lot Size": "Column 7",
        "Strategy": "Strategy (from Stage 1)",
        "Executing Broker Name": "Column 1 (TM Name)",
        "Trade Venue": "Blank",
        "Notes": "Column 2 (A/E)",
        "Transaction Type": "Computed from B/S + Opposite?"
    }
    
    def __init__(self, schema_file: str = None):
        """
        Initialize ACM Mapper
        
        Args:
            schema_file: Optional path to custom schema Excel file
        """
        self.schema_file = schema_file
        self.columns_order = self.DEFAULT_COLUMNS.copy()
        self.mandatory_columns = set(self.DEFAULT_MANDATORY)
        self.mapping_rules = self.DEFAULT_MAPPINGS.copy()
        
        try:
            self.singapore_tz = ZoneInfo("Asia/Singapore")
        except:
            import pytz
            self.singapore_tz = pytz.timezone("Asia/Singapore")
        
        # Try to load custom schema if provided
        if schema_file and Path(schema_file).exists():
            if self.load_schema(schema_file):
                logger.info(f"Loaded custom schema from {schema_file}")
            else:
                logger.warning("Failed to load custom schema, using defaults")
        else:
            logger.info("Using hardcoded default ACM schema")
    
    def load_schema(self, schema_file: str) -> bool:
        """
        Load custom schema from Excel file
        
        Args:
            schema_file: Path to Excel file with 'Columns' sheet
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Read the Columns sheet
            df = pd.read_excel(schema_file, sheet_name="Columns")
            df.columns = [c.strip() for c in df.columns]
            
            # Get column order
            self.columns_order = df["Column"].astype(str).tolist()
            
            # Get mandatory columns
            mandatory_mask = df["Mandatory"].astype(str).str.strip().str.lower() == "yes"
            self.mandatory_columns = set(df.loc[mandatory_mask, "Column"].astype(str).tolist())
            
            # Try to load mapping rules if they exist
            if "Mapping" in df.columns:
                self.mapping_rules = {}
                for idx, row in df.iterrows():
                    col = str(row["Column"])
                    mapping = str(row["Mapping"]) if pd.notna(row["Mapping"]) else ""
                    self.mapping_rules[col] = mapping
            
            logger.info(f"Loaded custom schema with {len(self.columns_order)} columns, "
                       f"{len(self.mandatory_columns)} mandatory")
            return True
            
        except Exception as e:
            logger.error(f"Error loading custom schema: {e}")
            return False
    
    def generate_schema_excel(self) -> bytes:
        """
        Generate an Excel file with the current schema
        Can be used to export hardcoded schema or modified schema
        
        Returns:
            Bytes of the Excel file
        """
        # Create schema dataframe
        schema_data = []
        for col in self.columns_order:
            schema_data.append({
                "Column": col,
                "Mandatory": "Yes" if col in self.mandatory_columns else "No",
                "Mapping": self.mapping_rules.get(col, ""),
                "Data Type": self._get_data_type(col),
                "Description": self._get_description(col)
            })
        
        schema_df = pd.DataFrame(schema_data)
        
        # Create transaction type rules dataframe
        trans_rules_data = [
            {"B/S": "Buy", "Opposite?": "Yes", "Transaction Type": "BuyToCover"},
            {"B/S": "Buy", "Opposite?": "No", "Transaction Type": "Buy"},
            {"B/S": "Sell", "Opposite?": "Yes", "Transaction Type": "Sell"},
            {"B/S": "Sell", "Opposite?": "No", "Transaction Type": "SellShort"}
        ]
        trans_rules_df = pd.DataFrame(trans_rules_data)
        
        # Create Excel file in memory
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            # Write main schema
            schema_df.to_excel(writer, sheet_name='Columns', index=False)
            
            # Write transaction rules
            trans_rules_df.to_excel(writer, sheet_name='Transaction Rules', index=False)
            
            # Write instructions
            instructions = pd.DataFrame({
                'Instructions': [
                    'This file defines the ACM ListedTrades output schema.',
                    '',
                    'Columns Sheet:',
                    '- Column: The name of the output column',
                    '- Mandatory: Whether the field must be populated (Yes/No)',
                    '- Mapping: Source field or calculation rule',
                    '- Data Type: Expected data type',
                    '- Description: Field description',
                    '',
                    'Transaction Rules Sheet:',
                    '- Defines how Transaction Type is determined from B/S and Opposite? flags',
                    '',
                    'Date Formatting:',
                    '- Trade Date: Full datetime with time (MM/DD/YYYY HH:MM:SS)',
                    '- Settle Date: Date only (MM/DD/YYYY)',
                    '- All other dates: Simple date format',
                    '',
                    'To customize:',
                    '1. Modify the Column names or order',
                    '2. Change Mandatory flags as needed',
                    '3. Update Mapping rules if source columns differ',
                    '4. Save and upload this file to use custom schema'
                ]
            })
            instructions.to_excel(writer, sheet_name='Instructions', index=False, header=False)
            
            # Auto-adjust column widths
            for sheet_name in writer.sheets:
                worksheet = writer.sheets[sheet_name]
                for column in worksheet.columns:
                    max_length = 0
                    column_letter = column[0].column_letter
                    for cell in column:
                        try:
                            if len(str(cell.value)) > max_length:
                                max_length = len(str(cell.value))
                        except:
                            pass
                    adjusted_width = min(max_length + 2, 50)
                    worksheet.column_dimensions[column_letter].width = adjusted_width
        
        output.seek(0)
        return output.read()
    
    def _get_data_type(self, column: str) -> str:
        """Get data type for a column"""
        type_map = {
            "Trade Date": "DateTime",  # Only Trade Date is DateTime
            "Settle Date": "Date",      # Settle Date is just Date
            "Quantity": "Number",
            "Trade Price": "Number",
            "Price": "Number",
            "Strike Price": "Number",
            "Lot Size": "Number",
        }
        return type_map.get(column, "Text")
    
    def _get_description(self, column: str) -> str:
        """Get description for a column"""
        desc_map = {
            "Trade Date": "Trade execution datetime (MM/DD/YYYY HH:MM:SS)",
            "Settle Date": "Settlement date (MM/DD/YYYY)",
            "Account Id": "Trading account identifier",
            "Counterparty Code": "Counterparty identifier",
            "Identifier": "Security identifier (Bloomberg ticker)",
            "Identifier Type": "Type of identifier used",
            "Quantity": "Number of lots traded (absolute value)",
            "Trade Price": "Execution price",
            "Price": "Price (duplicate of Trade Price)",
            "Instrument Type": "Type of instrument (OPTSTK/OPTIDX/FUTSTK/FUTIDX)",
            "Strike Price": "Option strike price",
            "Lot Size": "Contract lot size",
            "Strategy": "Trading strategy (FULO/FUSH)",
            "Executing Broker Name": "Name of executing broker",
            "Trade Venue": "Execution venue (usually blank)",
            "Notes": "Additional notes or comments",
            "Transaction Type": "Buy/Sell/BuyToCover/SellShort"
        }
        return desc_map.get(column, "")
    
    def map_transaction_type(self, bs: str, opposite: str) -> str:
        """
        Map B/S and Opposite? to Transaction Type
        """
        b = str(bs).strip().lower() if pd.notna(bs) else ""
        o = str(opposite).strip().lower() if pd.notna(opposite) else ""
        truthy = {"yes", "y", "true", "1"}
        
        if b.startswith("b"):
            return "BuyToCover" if o in truthy else "Buy"
        elif b.startswith("s"):
            return "Sell" if o in truthy else "SellShort"
        return ""
    
    def process_mapping(self, input_df: pd.DataFrame) -> pd.DataFrame:
        """
        Process mapping from Stage 1 output to ACM format
        """
        # Make a copy
        input_df = input_df.copy()
        
        # Initialize output with schema columns
        n = len(input_df)
        out = pd.DataFrame({col: [""] * n for col in self.columns_order})
        
        # Get current timestamps
        now_sg = datetime.now(self.singapore_tz)

        # UPDATED DATE FORMATTING:
        # Trade Date: Use from input if available (from broker recon), otherwise current time
        if "Trade Date" in input_df.columns and input_df["Trade Date"].notna().any():
            # Use trade date from input (broker reconciliation)
            trade_date_str = input_df["Trade Date"].astype(str)
        else:
            # Use current datetime
            trade_date_str = now_sg.strftime("%m/%d/%Y %H:%M:%S")

        # Settle Date: Date only, no time
        settle_date_str = now_sg.strftime("%m/%d/%Y")

        # ==================
        # APPLY MAPPINGS
        # ==================

        # Dates - UPDATED FORMATTING
        if "Trade Date" in out.columns:
            if isinstance(trade_date_str, str):
                out["Trade Date"] = trade_date_str  # Single value for all rows
            else:
                out["Trade Date"] = trade_date_str  # Series from input
        if "Settle Date" in out.columns:
            out["Settle Date"] = settle_date_str  # Date only
        
        # Account ID
        if "Account Id" in out.columns:
            if 0 in input_df.columns:
                out["Account Id"] = input_df[0].astype(str)
            elif "Scheme" in input_df.columns:
                out["Account Id"] = input_df["Scheme"].astype(str)
        
        # Counterparty Code
        if "Counterparty Code" in out.columns:
            if 13 in input_df.columns:
                out["Counterparty Code"] = input_df[13].astype(str)
            elif "CP Code" in input_df.columns:
                out["Counterparty Code"] = input_df["CP Code"].astype(str)
        
        # Identifier
        if "Identifier" in out.columns:
            if "Bloomberg_Ticker" in input_df.columns:
                out["Identifier"] = input_df["Bloomberg_Ticker"].astype(str)
        
        # Identifier Type
        if "Identifier Type" in out.columns:
            out["Identifier Type"] = "Bloomberg Yellow Key"
        
        # Quantity
        if "Quantity" in out.columns:
            if 12 in input_df.columns:
                out["Quantity"] = pd.to_numeric(input_df[12], errors="coerce").abs()
            elif "Lots Traded" in input_df.columns:
                out["Quantity"] = pd.to_numeric(input_df["Lots Traded"], errors="coerce").abs()
        
        # Prices
        price_val = None
        if 3 in input_df.columns:
            price_val = pd.to_numeric(input_df[3], errors="coerce")
        elif "Avg Price" in input_df.columns:
            price_val = pd.to_numeric(input_df["Avg Price"], errors="coerce")
        
        if price_val is not None:
            if "Trade Price" in out.columns:
                out["Trade Price"] = price_val
            if "Price" in out.columns:
                out["Price"] = price_val
        
        # Instrument Type
        if "Instrument Type" in out.columns:
            if 4 in input_df.columns:
                out["Instrument Type"] = input_df[4].astype(str)
            elif "Instr" in input_df.columns:
                out["Instrument Type"] = input_df["Instr"].astype(str)
        
        # Strike Price
        if "Strike Price" in out.columns:
            if 8 in input_df.columns:
                out["Strike Price"] = pd.to_numeric(input_df[8], errors="coerce")
            elif "Strike Price" in input_df.columns:
                out["Strike Price"] = pd.to_numeric(input_df["Strike Price"], errors="coerce")
        
        # Lot Size
        if "Lot Size" in out.columns:
            if 7 in input_df.columns:
                out["Lot Size"] = pd.to_numeric(input_df[7], errors="coerce")
            elif "Lot Size" in input_df.columns:
                out["Lot Size"] = pd.to_numeric(input_df["Lot Size"], errors="coerce")
        
        # Strategy
        if "Strategy" in out.columns:
            if "Strategy" in input_df.columns:
                out["Strategy"] = input_df["Strategy"].astype(str)
        
        # Executing Broker
        if "Executing Broker Name" in out.columns:
            if 1 in input_df.columns:
                out["Executing Broker Name"] = input_df[1].astype(str)
            elif "TM Name" in input_df.columns:
                out["Executing Broker Name"] = input_df["TM Name"].astype(str)
        
        # Trade Venue
        if "Trade Venue" in out.columns:
            out["Trade Venue"] = ""
        
        # Notes
        if "Notes" in out.columns:
            if 2 in input_df.columns:
                out["Notes"] = input_df[2].astype(str)
            elif "A/E" in input_df.columns:
                out["Notes"] = input_df["A/E"].astype(str)
        
        # Transaction Type
        if "Transaction Type" in out.columns:
            bs_col = None
            if 10 in input_df.columns:
                bs_col = 10
            elif "B/S" in input_df.columns:
                bs_col = "B/S"
            
            opposite_col = "Opposite?" if "Opposite?" in input_df.columns else None
            
            if bs_col is not None:
                if opposite_col:
                    out["Transaction Type"] = [
                        self.map_transaction_type(bs, op)
                        for bs, op in zip(input_df[bs_col], input_df[opposite_col])
                    ]
                else:
                    out["Transaction Type"] = [
                        self.map_transaction_type(bs, "No")
                        for bs in input_df[bs_col]
                    ]

        # Brokerage (from broker reconciliation)
        if "Brokerage" in out.columns:
            if "Pure Brokerage AMT" in input_df.columns:
                out["Brokerage"] = pd.to_numeric(input_df["Pure Brokerage AMT"], errors="coerce").fillna(0)

        # Taxes (from broker reconciliation)
        if "Taxes" in out.columns:
            if "Total Taxes" in input_df.columns:
                out["Taxes"] = pd.to_numeric(input_df["Total Taxes"], errors="coerce").fillna(0)

        # NEW: Enhanced columns from trade processing (EOD mode with broker reconciliation)
        # Comms - Pure brokerage from broker reconciliation (proportionally split for split trades)
        if "Comms" in out.columns:
            if "Comms" in input_df.columns:
                out["Comms"] = pd.to_numeric(input_df["Comms"], errors="coerce").fillna("")

        # Broker Taxes - Taxes from broker reconciliation (proportionally split for split trades)
        if "Broker Taxes" in out.columns:
            if "Taxes" in input_df.columns:
                out["Broker Taxes"] = pd.to_numeric(input_df["Taxes"], errors="coerce").fillna("")

        # Broker Trade Date - Trade date from broker file (same for all splits)
        if "Broker Trade Date" in out.columns:
            if "TD" in input_df.columns:
                out["Broker Trade Date"] = input_df["TD"].astype(str).replace('nan', '').replace('', '')

        # Clean up
        out = out.fillna("")
        for col in out.columns:
            out[col] = out[col].replace('nan', '')
        
        logger.info(f"Mapped {len(out)} records to ACM format")
        return out
    
    def validate_output(self, output_df: pd.DataFrame) -> List[Dict]:
        """Validate the output dataframe"""
        errors = []
        
        for col in self.mandatory_columns:
            if col not in output_df.columns:
                errors.append({
                    "row": 0,
                    "column": col,
                    "reason": "mandatory column missing"
                })
                continue
            
            col_values = output_df[col].astype(str).str.strip()
            blank_mask = (col_values == "") | (col_values.str.lower() == "nan")
            
            for idx in output_df.index[blank_mask]:
                errors.append({
                    "row": int(idx) + 1,
                    "column": col,
                    "reason": "mandatory field is blank"
                })
        
        return errors
    
    def process_trades_to_acm(self, processed_trades_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Main method to process trades to ACM format"""
        logger.info("Processing trades to ACM format")
        
        mapped_df = self.process_mapping(processed_trades_df)
        errors = self.validate_output(mapped_df)
        
        if errors:
            errors_df = pd.DataFrame(errors)
        else:
            errors_df = pd.DataFrame(columns=["row", "column", "reason"])
        
        return mapped_df, errors_df
