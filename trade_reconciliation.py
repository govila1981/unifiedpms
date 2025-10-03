"""
Trade Reconciliation Module
Matches clearing broker trades with executing broker trades
"""

import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List, Tuple, Optional
import logging
from pathlib import Path
import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment
from Trade_Parser import TradeParser
from broker_parser import get_parser_for_broker, decrypt_excel_file
from broker_config import detect_broker_from_filename, get_broker_by_code
from account_config import get_account_name, is_known_account

logger = logging.getLogger(__name__)


class TradeReconciler:
    """Reconciles clearing trades with executing broker trades"""

    def __init__(self, output_dir: str = "./output", account_prefix: str = ""):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.account_prefix = account_prefix
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")  # Fallback
        self.trade_date_str = None  # Will be set from trade data (DD-MMM-YYYY format)

    def set_trade_date(self, trade_date_str: str):
        """Set the trade date string for file naming"""
        self.trade_date_str = trade_date_str
        logger.info(f"Trade reconciler using trade date: {trade_date_str}")

    def reconcile(self, clearing_file, broker_files: List, futures_mapping_file: str = "futures mapping.csv") -> Dict:
        """
        Reconcile clearing trades with broker trades

        Args:
            clearing_file: Clearing broker trade file (uploaded file object)
            broker_files: List of broker file objects
            futures_mapping_file: Path to futures mapping CSV

        Returns:
            Dict with results and file paths
        """
        try:
            # Step 1: Parse clearing file with Bloomberg tickers
            logger.info("Step 1: Parsing clearing file...")
            clearing_df = self._parse_clearing_file(clearing_file, futures_mapping_file)
            if clearing_df.empty:
                return {'success': False, 'error': 'Failed to parse clearing file'}

            logger.info(f"Parsed {len(clearing_df)} clearing trades")

            # Step 1.5: Detect account from CP codes and set prefix (if not already set)
            if not self.account_prefix:
                clearing_cp_codes = clearing_df['CP Code'].dropna().astype(str).str.strip().str.upper().unique()
                detected_accounts = set()
                for cp_code in clearing_cp_codes:
                    if cp_code and cp_code != '':
                        account_name = get_account_name(cp_code)
                        if account_name != 'Unknown':
                            detected_accounts.add(account_name)

                # Set account prefix based on detection
                if len(detected_accounts) == 1:
                    account_name = list(detected_accounts)[0]
                    self.account_prefix = f"{account_name}_"
                    logger.info(f"Auto-detected account: {account_name} (prefix: {self.account_prefix})")
                elif len(detected_accounts) > 1:
                    # Multiple accounts in one file - use first one
                    account_name = sorted(detected_accounts)[0]
                    self.account_prefix = f"{account_name}_"
                    logger.warning(f"Multiple accounts detected: {detected_accounts}. Using: {account_name}")
                else:
                    # No known account detected
                    self.account_prefix = ""
                    logger.warning("No known account detected from CP codes")
            else:
                logger.info(f"Using provided account prefix: {self.account_prefix}")

            # Step 2: Parse all broker files
            logger.info("Step 2: Parsing broker files...")
            all_broker_trades = []
            parse_errors = []

            for broker_file in broker_files:
                try:
                    # Try to detect broker from filename first
                    broker_info = detect_broker_from_filename(broker_file.name)

                    # If not found from filename, detect from file content
                    if not broker_info:
                        logger.info(f"Detecting broker from file content: {broker_file.name}")
                        broker_file.seek(0)
                        broker_info = self._detect_broker_from_content(broker_file)
                        broker_file.seek(0)  # Reset for parsing

                    # Check if detection returned diagnostic info
                    if isinstance(broker_info, dict) and broker_info.get('error') == 'detection_failed':
                        columns = broker_info.get('columns', [])
                        first_row = broker_info.get('first_row', {})
                        read_error = broker_info.get('read_error')

                        if read_error:
                            error_msg = f"Could not read {broker_file.name} as Excel/CSV.\n  Error: {read_error}"
                        elif columns:
                            error_msg = f"Could not detect broker from {broker_file.name}.\n  Columns: {', '.join(str(c) for c in columns[:15])}{'...' if len(columns) > 15 else ''}\n  Sample: {list(first_row.values())[:3] if first_row else 'N/A'}"
                        else:
                            error_msg = f"Could not detect broker from {broker_file.name} (unknown structure)"

                        logger.warning(error_msg)
                        parse_errors.append(error_msg)
                        continue

                    if not broker_info:
                        error_msg = f"Could not detect broker type from file: {broker_file.name} (file may be unreadable or empty)"
                        logger.warning(error_msg)
                        parse_errors.append(error_msg)
                        continue

                    parser = get_parser_for_broker(broker_info['broker_id'], futures_mapping_file)
                    if not parser:
                        error_msg = f"No parser available for broker: {broker_info['broker_id']}"
                        logger.warning(error_msg)
                        parse_errors.append(error_msg)
                        continue

                    logger.info(f"Parsing {broker_file.name} as {broker_info['name']} (code: {broker_info['broker_code']})")
                    broker_file.seek(0)
                    broker_df = parser.parse_file(broker_file)

                    if not broker_df.empty:
                        broker_df['broker_name'] = broker_info['name']
                        broker_df['broker_id'] = broker_info['broker_id']
                        all_broker_trades.append(broker_df)
                        logger.info(f"✅ Parsed {len(broker_df)} trades from {broker_info['name']} (file: {broker_file.name})")
                    else:
                        error_msg = f"No trades found in file: {broker_file.name} (broker: {broker_info['name']})"
                        logger.warning(error_msg)
                        parse_errors.append(error_msg)

                except Exception as e:
                    error_msg = f"Error parsing {broker_file.name}: {str(e)}"
                    logger.exception(error_msg)
                    parse_errors.append(error_msg)

            if not all_broker_trades:
                error_detail = f"No broker trades parsed from {len(broker_files)} file(s). Details: {'; '.join(parse_errors)}"
                logger.error(error_detail)
                return {
                    'success': False,
                    'error': error_detail,
                    'parse_errors': parse_errors,  # Individual error messages
                    'file_count': len(broker_files)
                }

            broker_df_combined = pd.concat(all_broker_trades, ignore_index=True)
            logger.info(f"Total broker trades: {len(broker_df_combined)}")

            # Step 2.5: Validate CP Codes match between clearing and broker files
            logger.info("Step 2.5: Validating CP Codes...")
            clearing_cp_codes = set(clearing_df['CP Code'].dropna().astype(str).str.strip().str.upper())
            broker_cp_codes = set(broker_df_combined['cp_code'].dropna().astype(str).str.strip().str.upper())

            # Remove empty strings
            clearing_cp_codes.discard('')
            broker_cp_codes.discard('')

            cp_code_mismatch = False
            error_messages = []

            # Check for CP codes in clearing not in broker
            clearing_only = clearing_cp_codes - broker_cp_codes
            if clearing_only:
                error_messages.append(f"CP Code(s) in clearing file but not in broker files: {', '.join(sorted(clearing_only))}")
                cp_code_mismatch = True

            # Check for CP codes in broker not in clearing
            broker_only = broker_cp_codes - clearing_cp_codes
            if broker_only:
                error_messages.append(f"CP Code(s) in broker files but not in clearing file: {', '.join(sorted(broker_only))}")
                cp_code_mismatch = True

            if cp_code_mismatch:
                error_msg = "CP Code validation failed. " + " | ".join(error_messages)
                logger.error(error_msg)
                return {
                    'success': False,
                    'error': error_msg,
                    'error_type': 'cp_code_mismatch',
                    'clearing_cp_codes': list(clearing_cp_codes),
                    'broker_cp_codes': list(broker_cp_codes)
                }

            logger.info(f"CP Code validation passed. CP Codes: {', '.join(sorted(clearing_cp_codes))}")

            # Step 3: Match trades
            logger.info("Step 3: Matching trades...")
            matched, unmatched_clearing, unmatched_broker = self._match_trades(
                clearing_df, broker_df_combined
            )

            logger.info(f"Matched: {len(matched)}, Unmatched clearing: {len(unmatched_clearing)}, Unmatched broker: {len(unmatched_broker)}")

            # Step 4: Generate outputs
            logger.info("Step 4: Generating output files...")
            enhanced_clearing_file = self._generate_enhanced_clearing(clearing_df, broker_df_combined, matched)
            recon_report_file = self._generate_reconciliation_report(
                matched, unmatched_clearing, unmatched_broker, clearing_df, broker_df_combined
            )

            # Get account name for return
            account_display = self.account_prefix.rstrip('_') if self.account_prefix else 'Unknown'

            return {
                'success': True,
                'matched_count': len(matched),
                'unmatched_clearing_count': len(unmatched_clearing),
                'unmatched_broker_count': len(unmatched_broker),
                'total_clearing': len(clearing_df),
                'total_broker': len(broker_df_combined),
                'match_rate': len(matched) / len(clearing_df) * 100 if len(clearing_df) > 0 else 0,
                'enhanced_clearing_file': enhanced_clearing_file,
                'reconciliation_report': recon_report_file,
                'account_name': account_display
            }

        except Exception as e:
            logger.error(f"Error in reconciliation: {e}")
            return {'success': False, 'error': str(e)}

    def _detect_broker_from_content(self, file_obj) -> Optional[Dict]:
        """Detect broker from actual data in file (broker names/codes), not just file structure"""
        try:
            file_obj.seek(0)

            # Try to decrypt if password-protected
            decrypted_file = decrypt_excel_file(file_obj)
            if decrypted_file:
                file_obj = decrypted_file
                logger.info("File was password-protected, using decrypted version for detection")

            # Try reading as Excel
            try:
                df = pd.read_excel(file_obj, nrows=100)  # Read first 100 rows for detection
                logger.info(f"Successfully read Excel file for detection. Columns: {list(df.columns)}")
            except Exception as e:
                # Try reading as CSV
                file_obj.seek(0)
                try:
                    df = pd.read_csv(file_obj, nrows=100)
                    logger.info(f"Successfully read CSV file for detection. Columns: {list(df.columns)}")
                except Exception as csv_error:
                    logger.error(f"Could not read file as Excel or CSV: Excel error: {e}, CSV error: {csv_error}")
                    return {
                        'error': 'detection_failed',
                        'columns': [],
                        'first_row': None,
                        'read_error': f"Excel: {str(e)[:100]}, CSV: {str(csv_error)[:100]}"
                    }

            # PRIORITY 1: Look for broker codes AND broker names in data columns (MOST RELIABLE)
            logger.info("Step 1: Checking broker code/name columns in file data...")

            # 1a. Check broker code columns (try exact names first, then fuzzy match)
            exact_broker_code_columns = ['Broker Code', 'BrokerNSECode', 'Broker NSE Code', 'TM Code', 'TM_Code', 'Broker_Code', 'Member Code', 'Member_Code', 'Broker', 'Member']
            broker_code_counts = {}  # Track which broker codes appear most frequently

            # First try exact column names
            columns_to_check = []
            for col in exact_broker_code_columns:
                if col in df.columns:
                    columns_to_check.append(col)

            # If no exact match, try fuzzy matching (columns containing "broker" or "member" or "tm")
            if not columns_to_check:
                logger.info("No exact broker code column match, trying fuzzy search...")
                for col in df.columns:
                    col_lower = str(col).lower()
                    if any(keyword in col_lower for keyword in ['broker', 'member', 'tm code', 'tm_code']):
                        logger.info(f"  Found potential broker code column: '{col}'")
                        columns_to_check.append(col)

            if columns_to_check:
                logger.info(f"Checking columns for broker codes: {columns_to_check}")

            for col in columns_to_check:
                logger.info(f"Reading broker codes from column '{col}'...")
                # Get all non-null broker codes
                broker_codes = df[col].dropna()

                # Log first few values for debugging
                sample_values = broker_codes.head(5).tolist()
                logger.info(f"  Sample values: {sample_values}")

                for broker_code_str in broker_codes:
                    try:
                        broker_code_str = str(broker_code_str).strip()

                        # Remove any leading zeros and convert to int
                        if broker_code_str.replace('.', '').replace('-', '').isdigit():
                            broker_code = abs(int(float(broker_code_str)))

                            # Only count valid broker codes (4-5 digits)
                            if 1000 <= broker_code <= 99999:
                                # Count occurrences
                                if broker_code not in broker_code_counts:
                                    broker_code_counts[broker_code] = 0
                                broker_code_counts[broker_code] += 1
                    except Exception as e:
                        logger.debug(f"  Could not parse broker code '{broker_code_str}': {e}")
                        continue

            # Find most common broker code
            if broker_code_counts:
                most_common_code = max(broker_code_counts, key=broker_code_counts.get)
                occurrences = broker_code_counts[most_common_code]

                logger.info(f"Broker code frequency: {broker_code_counts}")
                broker_info = get_broker_by_code(most_common_code)
                if broker_info:
                    logger.info(f"✓ Detected {broker_info['name']} from broker code {most_common_code} ({occurrences} row(s) with this code)")
                    return broker_info
                else:
                    logger.warning(f"Found broker code {most_common_code} ({occurrences} occurrences) but it's not in registry")
            else:
                logger.info(f"No broker code columns found. Checked: {broker_code_columns}")

            # 1b. Check Broker Name column
            if 'Broker Name' in df.columns:
                logger.info("Found 'Broker Name' column, checking broker names in data...")
                broker_name_counts = {}

                broker_names = df['Broker Name'].dropna().astype(str).str.upper().str.strip()

                for broker_name in broker_names:
                    # Map broker names to codes
                    matched_code = None
                    if 'EQUIRUS' in broker_name:
                        matched_code = 13017
                    elif 'ANTIQUE' in broker_name:
                        matched_code = 12987
                    elif 'KOTAK' in broker_name:
                        matched_code = 8081
                    elif 'ICICI' in broker_name:
                        matched_code = 7730
                    elif 'IIFL' in broker_name:
                        matched_code = 10975
                    elif 'AXIS' in broker_name:
                        matched_code = 13872
                    elif 'EDELWEISS' in broker_name or 'NUVAMA' in broker_name:
                        matched_code = 11933
                    elif 'MORGAN' in broker_name:
                        matched_code = 10542

                    if matched_code:
                        if matched_code not in broker_name_counts:
                            broker_name_counts[matched_code] = 0
                        broker_name_counts[matched_code] += 1

                # Find most common broker from names
                if broker_name_counts:
                    most_common_code = max(broker_name_counts, key=broker_name_counts.get)
                    occurrences = broker_name_counts[most_common_code]

                    broker_info = get_broker_by_code(most_common_code)
                    if broker_info:
                        logger.info(f"✓ Detected {broker_info['name']} from Broker Name column ({occurrences} row(s) with this broker)")
                        return broker_info

            # PRIORITY 2: Column structure detection (LAST RESORT - only for files without broker codes/names)
            logger.info("Step 3: Falling back to column structure detection (less reliable)...")

            # Morgan Stanley: Special case - doesn't have broker code column
            # Signature columns: 'Commission (Taxable Value)', 'Central GST*', 'State GST**', 'WAP'
            morgan_indicators = ['Commission (Taxable Value)', 'Central GST*', 'State GST**', 'WAP']
            if all(col in df.columns for col in morgan_indicators):
                broker_info = get_broker_by_code(10542)  # Morgan Stanley
                if broker_info:
                    logger.info(f"✓ Detected Morgan Stanley from column structure (no broker code column)")
                    return broker_info

            # Kotak: 'Scrip', 'Instrument', 'Lots traded', 'Traded Price', 'Brokerage'
            kotak_indicators = ['Scrip', 'Lots traded', 'Traded Price', 'Brokerage']
            if all(col in df.columns for col in kotak_indicators):
                broker_info = get_broker_by_code(8081)
                if broker_info:
                    logger.warning(f"Detected Kotak from column structure (no broker code found in data)")
                    return broker_info

            # IIFL: 'CustodianCode', 'OptionType', 'BuySellStatus', 'ConfPrice', 'BrokValue'
            iifl_indicators = ['CustodianCode', 'OptionType', 'BuySellStatus', 'ConfPrice', 'BrokValue']
            if all(col in df.columns for col in iifl_indicators):
                broker_info = get_broker_by_code(10975)
                if broker_info:
                    logger.warning(f"Detected IIFL from column structure (no broker code found in data)")
                    return broker_info

            # Axis: 'CP Code', 'Buy/Sell', 'OptType', 'Contract Lot', 'Total Charges'
            axis_indicators = ['CP Code', 'Buy/Sell', 'OptType', 'Contract Lot', 'Total Charges']
            if all(col in df.columns for col in axis_indicators):
                broker_info = get_broker_by_code(13872)
                if broker_info:
                    logger.warning(f"Detected Axis from column structure (no broker code found in data)")
                    return broker_info

            # Edelweiss: 'Market Lot Size', 'No Of Traded Lots', 'OptType', 'Net Amount'
            edelweiss_indicators = ['Market Lot Size', 'No Of Traded Lots', 'OptType', 'Net Amount']
            if all(col in df.columns for col in edelweiss_indicators):
                broker_info = get_broker_by_code(11933)
                if broker_info:
                    logger.warning(f"Detected Edelweiss from column structure (no broker code found in data)")
                    return broker_info

            # Equirus/Antique: IDENTICAL formats - CANNOT detect by structure alone
            # These MUST be detected by broker name/code in data
            equirus_antique_indicators = ['Scrip Code', 'Call / Put', 'Pure Brokerage AMT', 'CP Code']
            if all(col in df.columns for col in equirus_antique_indicators):
                logger.error(f"File has Equirus/Antique format but no broker name or code found in data")
                logger.error(f"Cannot distinguish between Equirus and Antique without broker identification")
                logger.error(f"Please ensure file contains 'Broker Code' column or broker name in data")
                return {
                    'error': 'detection_failed',
                    'columns': list(df.columns),
                    'first_row': df.iloc[0].to_dict() if len(df) > 0 else None,
                    'read_error': 'Equirus/Antique format detected but cannot determine which broker - missing broker code/name in data'
                }

            logger.error("Could not detect broker from file")
            logger.error(f"File columns: {list(df.columns)}")
            logger.error(f"First row sample: {df.iloc[0].to_dict() if len(df) > 0 else 'No data'}")

            return {
                'error': 'detection_failed',
                'columns': list(df.columns),
                'first_row': df.iloc[0].to_dict() if len(df) > 0 else None
            }

        except Exception as e:
            logger.error(f"Error detecting broker from content: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None

    def _parse_clearing_file(self, file_obj, futures_mapping_file: str) -> pd.DataFrame:
        """Parse clearing file and add Bloomberg tickers"""
        try:
            # Try to decrypt if password-protected
            file_obj.seek(0)
            decrypted_file = decrypt_excel_file(file_obj)
            if decrypted_file:
                file_obj = decrypted_file

            # Save uploaded file temporarily
            temp_path = self.output_dir / f"temp_clearing_{self.timestamp}.xlsx"
            file_obj.seek(0)
            with open(temp_path, 'wb') as f:
                f.write(file_obj.read())

            # Read as DataFrame to preserve all columns
            df = pd.read_excel(temp_path)

            # Use TradeParser to generate Bloomberg tickers
            parser = TradeParser(futures_mapping_file)
            file_obj.seek(0)
            trades = parser.parse_trade_file(str(temp_path))

            # Create mapping of trade attributes to Bloomberg ticker
            ticker_map = {}
            for trade in trades:
                key = (
                    trade.symbol,
                    trade.expiry_date.strftime('%d/%m/%Y'),
                    trade.security_type,
                    trade.strike_price,
                    abs(trade.position_lots)  # Use absolute value for matching
                )
                ticker_map[key] = trade.bloomberg_ticker

            # Load futures mapping for symbol → ticker conversion
            symbol_to_ticker = {}
            try:
                mapping_df = pd.read_csv(futures_mapping_file, skiprows=3)
                for idx, row in mapping_df.iterrows():
                    symbol = row.get('Symbol')
                    ticker = row.get('Ticker')
                    if pd.notna(symbol) and pd.notna(ticker):
                        symbol_to_ticker[str(symbol).strip().upper()] = str(ticker).strip()
                logger.info(f"Loaded {len(symbol_to_ticker)} symbol-to-ticker mappings")
            except Exception as e:
                logger.warning(f"Could not load symbol-to-ticker mapping: {e}")

            # Add Bloomberg ticker column to DataFrame
            def get_ticker_for_row(row):
                try:
                    # Parse row to match trade attributes
                    symbol = str(row.get('Symbol', '')).strip().upper()
                    expiry_str = str(row.get('Expiry Dt', ''))
                    expiry = pd.to_datetime(expiry_str).strftime('%d/%m/%Y')
                    strike = float(row.get('Strike Price', 0))
                    instr = str(row.get('Instr', '')).strip().upper()
                    option_type = str(row.get('Option Type', '')).strip().upper()
                    lots = abs(float(row.get('Lots Traded', 0)))

                    # Determine security type
                    if 'FUT' in instr:
                        security_type = 'Futures'
                    elif option_type in ['CE', 'C', 'CALL']:
                        security_type = 'Call'
                    elif option_type in ['PE', 'P', 'PUT']:
                        security_type = 'Put'
                    else:
                        return None

                    key = (symbol, expiry, security_type, strike, lots)
                    return ticker_map.get(key)
                except:
                    return None

            df['Bloomberg Ticker'] = df.apply(get_ticker_for_row, axis=1)

            # Replace Symbol column with Ticker from futures mapping
            def get_ticker_from_symbol(symbol):
                symbol_upper = str(symbol).strip().upper()
                return symbol_to_ticker.get(symbol_upper, symbol)  # Return original if not found

            df['Symbol'] = df['Symbol'].apply(get_ticker_from_symbol)

            # Normalize fields for matching
            df['cp_code_normalized'] = df['CP Code'].astype(str).str.strip().str.upper()
            df['broker_code_normalized'] = df['TM Code'].apply(lambda x: abs(int(x)))
            df['side_normalized'] = df['B/S'].astype(str).str.strip().apply(
                lambda x: 'Buy' if x.upper().startswith('B') else 'Sell'
            )
            df['quantity'] = df['Qty'].astype(int)
            df['price'] = df['Avg Price'].astype(float)
            df['ticker_normalized'] = df['Bloomberg Ticker'].astype(str).str.upper().str.strip()

            # Add lots column for matching if it exists in the clearing file
            if 'Lots Traded' in df.columns:
                df['lots'] = df['Lots Traded'].astype(float)

            # Clean up temp file
            temp_path.unlink()

            return df

        except Exception as e:
            logger.error(f"Error parsing clearing file: {e}")
            return pd.DataFrame()

    def _match_trades(self, clearing_df: pd.DataFrame, broker_df: pd.DataFrame) -> Tuple[List, List, List]:
        """
        Match clearing trades with broker trades

        Returns:
            (matched_pairs, unmatched_clearing_indices, unmatched_broker_indices)
        """
        matched = []
        # Use DataFrame indices, not positional indices
        unmatched_clearing = list(clearing_df.index)
        unmatched_broker = list(broker_df.index)

        # Normalize broker tickers
        broker_df['ticker_normalized'] = broker_df['bloomberg_ticker'].str.upper().str.strip()
        broker_df['cp_code_normalized'] = broker_df['cp_code'].str.upper().str.strip()

        # Match each clearing trade
        for clear_idx, clear_row in clearing_df.iterrows():
            # Skip if no Bloomberg ticker
            if pd.isna(clear_row['Bloomberg Ticker']):
                continue

            # Find potential matches in broker data
            # Match on: ticker, CP code, broker code, side, quantity AND lots
            broker_matches = broker_df[
                (broker_df['ticker_normalized'] == clear_row['ticker_normalized']) &
                (broker_df['cp_code_normalized'] == clear_row['cp_code_normalized']) &
                (broker_df['broker_code'] == clear_row['broker_code_normalized']) &
                (broker_df['side'] == clear_row['side_normalized']) &
                (broker_df['quantity'] == clear_row['quantity'])
            ]

            # Additional check on lots if available in both dataframes
            if 'lots' in broker_df.columns and 'lots' in clearing_df.columns:
                clear_lots = abs(clear_row.get('lots', 0))
                if clear_lots > 0:  # Only apply lots filter if clearing has lots data
                    broker_matches = broker_matches[
                        broker_matches['lots'].abs() == clear_lots
                    ]

            # Check price tolerance (0.001%)
            if len(broker_matches) > 0:
                price_tolerance = 0.00001  # 0.001%
                broker_matches = broker_matches[
                    (broker_matches['price'] - clear_row['price']).abs() / clear_row['price'] < price_tolerance
                ]

            # If match found, use first match
            if len(broker_matches) > 0:
                broker_idx = broker_matches.index[0]

                matched.append({
                    'clearing_idx': clear_idx,
                    'broker_idx': broker_idx
                })

                # Remove from unmatched lists
                if clear_idx in unmatched_clearing:
                    unmatched_clearing.remove(clear_idx)
                if broker_idx in unmatched_broker:
                    unmatched_broker.remove(broker_idx)

        return matched, unmatched_clearing, unmatched_broker

    def _generate_enhanced_clearing(self, clearing_df: pd.DataFrame, broker_df: pd.DataFrame, matched: List[Dict]) -> str:
        """Generate enhanced clearing file with broker data"""
        try:
            # Create output DataFrame with all clearing columns
            output_df = clearing_df.copy()

            # Add new columns (initialized as None/empty) with exact header names
            output_df['Comms'] = pd.NA
            output_df['Taxes'] = pd.NA
            output_df['TD'] = ''

            # Fill matched rows with broker data
            for match in matched:
                clear_idx = match['clearing_idx']
                broker_idx = match['broker_idx']

                # Get broker row
                broker_row = broker_df.iloc[broker_idx]

                # Fill in broker data - preserve exact values
                pure_brok = broker_row.get('pure_brokerage', 0)
                total_tax = broker_row.get('total_taxes', 0)
                trade_dt = broker_row.get('trade_date', '')

                # Log for debugging
                if pd.isna(trade_dt) or trade_dt == '' or trade_dt is None:
                    logger.warning(f"Missing trade_date for clearing idx {clear_idx}, broker idx {broker_idx}")

                output_df.loc[clear_idx, 'Comms'] = pure_brok
                output_df.loc[clear_idx, 'Taxes'] = total_tax
                output_df.loc[clear_idx, 'TD'] = trade_dt if pd.notna(trade_dt) else ''

            # Remove internal/duplicate columns that were added during parsing
            # IMPORTANT: Keep 'Bloomberg Ticker' - it's needed for Trade Parser to parse the enhanced file
            internal_cols = ['cp_code_normalized', 'broker_code_normalized', 'side_normalized',
                           'quantity', 'price', 'ticker_normalized']
            cols_to_remove = [col for col in internal_cols if col in output_df.columns]
            if cols_to_remove:
                output_df = output_df.drop(columns=cols_to_remove)

            # Get original clearing columns (excluding internal ones, but keeping Bloomberg Ticker)
            original_cols = [col for col in clearing_df.columns if col not in internal_cols]

            # Final column order: original columns + 3 new columns
            new_cols = ['Comms', 'Taxes', 'TD']
            final_cols = original_cols + new_cols

            # Ensure all columns exist before reordering
            for col in final_cols:
                if col not in output_df.columns:
                    if col in new_cols:
                        # Already added above
                        pass
                    else:
                        logger.warning(f"Column {col} missing from output_df")

            output_df = output_df[[col for col in final_cols if col in output_df.columns]]

            # Format Expiry Dt as DD/MM/YYYY string
            if 'Expiry Dt' in output_df.columns:
                output_df['Expiry Dt'] = pd.to_datetime(output_df['Expiry Dt'], dayfirst=True, errors='coerce').dt.strftime('%d/%m/%Y')

            # Format TD as DD/MM/YYYY string, but preserve empty strings
            if 'TD' in output_df.columns:
                # Convert TD to date string, but preserve empty strings
                def convert_td(val):
                    if pd.isna(val) or val == '' or val is None:
                        return ''
                    try:
                        # Use dayfirst=True to parse as DD/MM/YYYY
                        parsed_date = pd.to_datetime(val, dayfirst=True, errors='coerce')
                        if pd.notna(parsed_date):
                            return parsed_date.strftime('%d/%m/%Y')
                        return val
                    except:
                        return val
                output_df['TD'] = output_df['TD'].apply(convert_td)

            # Save to CSV with full precision for numeric columns
            # Use trade date if available, otherwise timestamp
            date_str = self.trade_date_str if self.trade_date_str else self.timestamp
            filename = f"{self.account_prefix}clearing_enhanced_{date_str}.csv"
            filepath = self.output_dir / filename
            output_df.to_csv(filepath, index=False, float_format='%.10g')

            logger.info(f"Generated enhanced clearing file: {filename} with {len(matched)} matched trades")
            return str(filepath)

        except Exception as e:
            logger.error(f"Error generating enhanced clearing: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return ""

    def _find_match_failure_reason(self, clear_row, broker_df: pd.DataFrame) -> str:
        """Find why a clearing trade didn't match with any broker trade"""
        reasons = []

        # Check each criterion
        ticker_matches = broker_df[broker_df['ticker_normalized'] == clear_row['ticker_normalized']]
        if len(ticker_matches) == 0:
            reasons.append(f"No broker trade with ticker {clear_row['ticker_normalized']}")
            return "; ".join(reasons)

        cp_matches = ticker_matches[ticker_matches['cp_code_normalized'] == clear_row['cp_code_normalized']]
        if len(cp_matches) == 0:
            broker_cp_codes = ticker_matches['cp_code_normalized'].unique().tolist()
            reasons.append(f"CP Code mismatch (clearing={clear_row['cp_code_normalized']}, broker={broker_cp_codes})")
            return "; ".join(reasons)

        broker_code_matches = cp_matches[cp_matches['broker_code'] == clear_row['broker_code_normalized']]
        if len(broker_code_matches) == 0:
            broker_codes = cp_matches['broker_code'].unique().tolist()
            reasons.append(f"Broker Code mismatch (clearing={clear_row['broker_code_normalized']}, broker={broker_codes})")
            return "; ".join(reasons)

        side_matches = broker_code_matches[broker_code_matches['side'] == clear_row['side_normalized']]
        if len(side_matches) == 0:
            broker_sides = broker_code_matches['side'].unique().tolist()
            reasons.append(f"Side mismatch (clearing={clear_row['side_normalized']}, broker={broker_sides})")
            return "; ".join(reasons)

        qty_matches = side_matches[side_matches['quantity'] == clear_row['quantity']]
        if len(qty_matches) == 0:
            broker_qtys = side_matches['quantity'].unique().tolist()
            reasons.append(f"Quantity mismatch (clearing={clear_row['quantity']}, broker={broker_qtys})")
            return "; ".join(reasons)

        # Check lots if available
        if 'lots' in broker_df.columns and 'lots' in clear_row.index:
            clear_lots = abs(clear_row.get('lots', 0))
            if clear_lots > 0:  # Only check if clearing has lots data
                lots_matches = qty_matches[qty_matches['lots'].abs() == clear_lots]
                if len(lots_matches) == 0:
                    broker_lots = qty_matches['lots'].abs().unique().tolist()
                    reasons.append(f"Lots mismatch (clearing={clear_lots}, broker={broker_lots})")
                    return "; ".join(reasons)
                qty_matches = lots_matches  # Continue with lots-matched trades

        # Check price tolerance
        price_tolerance = 0.00001
        price_matches = qty_matches[
            (qty_matches['price'] - clear_row['price']).abs() / clear_row['price'] < price_tolerance
        ]
        if len(price_matches) == 0:
            broker_prices = qty_matches['price'].tolist()
            price_diffs = [(p - clear_row['price']) / clear_row['price'] * 100 for p in broker_prices]
            reasons.append(f"Price mismatch (clearing={clear_row['price']:.4f}, broker={broker_prices}, diff%={price_diffs})")

        return "; ".join(reasons) if reasons else "Unknown reason"

    def _find_broker_match_failure_reason(self, broker_row, clearing_df: pd.DataFrame) -> str:
        """Find why a broker trade didn't match with any clearing trade"""
        reasons = []

        # Check each criterion
        ticker_matches = clearing_df[clearing_df['ticker_normalized'] == broker_row['ticker_normalized']]
        if len(ticker_matches) == 0:
            reasons.append(f"No clearing trade with ticker {broker_row['ticker_normalized']}")
            return "; ".join(reasons)

        cp_matches = ticker_matches[ticker_matches['cp_code_normalized'] == broker_row['cp_code_normalized']]
        if len(cp_matches) == 0:
            clearing_cp_codes = ticker_matches['cp_code_normalized'].unique().tolist()
            reasons.append(f"CP Code mismatch (broker={broker_row['cp_code_normalized']}, clearing={clearing_cp_codes})")
            return "; ".join(reasons)

        broker_code_matches = cp_matches[cp_matches['broker_code_normalized'] == broker_row['broker_code']]
        if len(broker_code_matches) == 0:
            clearing_broker_codes = cp_matches['broker_code_normalized'].unique().tolist()
            reasons.append(f"Broker Code mismatch (broker={broker_row['broker_code']}, clearing={clearing_broker_codes})")
            return "; ".join(reasons)

        side_matches = broker_code_matches[broker_code_matches['side_normalized'] == broker_row['side']]
        if len(side_matches) == 0:
            clearing_sides = broker_code_matches['side_normalized'].unique().tolist()
            reasons.append(f"Side mismatch (broker={broker_row['side']}, clearing={clearing_sides})")
            return "; ".join(reasons)

        qty_matches = side_matches[side_matches['quantity'] == broker_row['quantity']]
        if len(qty_matches) == 0:
            clearing_qtys = side_matches['quantity'].unique().tolist()
            reasons.append(f"Quantity mismatch (broker={broker_row['quantity']}, clearing={clearing_qtys})")
            return "; ".join(reasons)

        # Check lots if available in both dataframes
        if 'lots' in clearing_df.columns and 'lots' in broker_row.index:
            broker_lots = abs(broker_row.get('lots', 0))
            if broker_lots > 0:
                lots_matches = qty_matches[qty_matches['lots'].abs() == broker_lots]
                if len(lots_matches) == 0:
                    clearing_lots = qty_matches['lots'].abs().unique().tolist()
                    reasons.append(f"Lots mismatch (broker={broker_lots}, clearing={clearing_lots})")
                    return "; ".join(reasons)
                qty_matches = lots_matches  # Continue with lots-matched trades

        # Check price tolerance
        price_tolerance = 0.00001
        price_matches = qty_matches[
            (qty_matches['price'] - broker_row['price']).abs() / broker_row['price'] < price_tolerance
        ]
        if len(price_matches) == 0:
            clearing_prices = qty_matches['price'].tolist()
            price_diffs = [(p - broker_row['price']) / broker_row['price'] * 100 for p in clearing_prices]
            reasons.append(f"Price mismatch (broker={broker_row['price']:.4f}, clearing={clearing_prices}, diff%={price_diffs})")

        return "; ".join(reasons) if reasons else "Unknown reason"

    def _generate_reconciliation_report(self, matched: List, unmatched_clearing: List,
                                      unmatched_broker: List, clearing_df: pd.DataFrame,
                                      broker_df: pd.DataFrame) -> str:
        """Generate Excel reconciliation report with 4 sheets"""
        try:
            # Use trade date if available, otherwise timestamp
            date_str = self.trade_date_str if self.trade_date_str else self.timestamp
            filename = f"{self.account_prefix}broker_recon_report_{date_str}.xlsx"
            filepath = self.output_dir / filename

            with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
                # Sheet 1: Matched Trades - Original clearing format + 3 new columns
                matched_rows = []
                if matched:
                    for match in matched:
                        clear_idx = match['clearing_idx']
                        broker_idx = match['broker_idx']

                        # Get original clearing row as dict
                        clear_row = clearing_df.iloc[clear_idx].to_dict()
                        broker_row = broker_df.iloc[broker_idx]

                        # Add broker data to clearing row with exact column names
                        clear_row['Comms'] = broker_row.get('pure_brokerage', 0)
                        clear_row['Taxes'] = broker_row.get('total_taxes', 0)
                        clear_row['TD'] = broker_row.get('trade_date', '')

                        matched_rows.append(clear_row)

                matched_df = pd.DataFrame(matched_rows) if matched_rows else pd.DataFrame()

                # Remove internal/duplicate columns from matched output
                if not matched_df.empty:
                    internal_cols = ['cp_code_normalized', 'broker_code_normalized', 'side_normalized',
                                   'quantity', 'price', 'ticker_normalized', 'Bloomberg Ticker']
                    cols_to_drop = [col for col in internal_cols if col in matched_df.columns]
                    if cols_to_drop:
                        matched_df = matched_df.drop(columns=cols_to_drop)

                    # Format Expiry Dt as date (not datetime)
                    if 'Expiry Dt' in matched_df.columns:
                        matched_df['Expiry Dt'] = pd.to_datetime(matched_df['Expiry Dt'], dayfirst=True, errors='coerce').dt.date

                    # Format TD as date only if it has valid values
                    if 'TD' in matched_df.columns:
                        def convert_td(val):
                            if pd.isna(val) or val == '' or val is None:
                                return ''
                            try:
                                # Use dayfirst=True to parse as DD/MM/YYYY
                                parsed_date = pd.to_datetime(val, dayfirst=True, errors='coerce')
                                if pd.notna(parsed_date):
                                    return parsed_date.date()
                                return val
                            except:
                                return val
                        matched_df['TD'] = matched_df['TD'].apply(convert_td)

                matched_df.to_excel(writer, sheet_name='Matched Trades', index=False)

                # Sheet 2: Unmatched Clearing - Original format + 3 empty columns + diagnostic columns
                if unmatched_clearing:
                    unmatched_clear_df = clearing_df.iloc[unmatched_clearing].copy()
                    # Add empty columns for consistency
                    unmatched_clear_df['Comms'] = ''
                    unmatched_clear_df['Taxes'] = ''
                    unmatched_clear_df['TD'] = ''

                    # Add diagnostic columns BEFORE removing internal columns
                    diagnostic_data = []
                    for idx in unmatched_clearing:
                        clear_row = clearing_df.iloc[idx]
                        reason = self._find_match_failure_reason(clear_row, broker_df)
                        diagnostic_data.append({
                            'DIAGNOSTIC_Ticker': clear_row.get('ticker_normalized', ''),
                            'DIAGNOSTIC_CP_Code': clear_row.get('cp_code_normalized', ''),
                            'DIAGNOSTIC_Broker_Code': clear_row.get('broker_code_normalized', ''),
                            'DIAGNOSTIC_Side': clear_row.get('side_normalized', ''),
                            'DIAGNOSTIC_Qty': clear_row.get('quantity', ''),
                            'DIAGNOSTIC_Price': clear_row.get('price', ''),
                            'DIAGNOSTIC_Match_Failure_Reason': reason
                        })
                    diagnostic_df = pd.DataFrame(diagnostic_data)

                    # Combine with unmatched clearing data
                    unmatched_clear_df = pd.concat([unmatched_clear_df.reset_index(drop=True), diagnostic_df], axis=1)

                    # Remove internal/duplicate columns
                    internal_cols = ['cp_code_normalized', 'broker_code_normalized', 'side_normalized',
                                   'quantity', 'price', 'ticker_normalized', 'Bloomberg Ticker']
                    cols_to_drop = [col for col in internal_cols if col in unmatched_clear_df.columns]
                    if cols_to_drop:
                        unmatched_clear_df = unmatched_clear_df.drop(columns=cols_to_drop)

                    # Format Expiry Dt as date (not datetime)
                    if 'Expiry Dt' in unmatched_clear_df.columns:
                        unmatched_clear_df['Expiry Dt'] = pd.to_datetime(unmatched_clear_df['Expiry Dt'], dayfirst=True, errors='coerce').dt.date

                    # Format TD as date (not datetime) - should be empty for unmatched
                    if 'TD' in unmatched_clear_df.columns:
                        unmatched_clear_df['TD'] = pd.to_datetime(unmatched_clear_df['TD'], dayfirst=True, errors='coerce').dt.date
                else:
                    # Create empty dataframe with same structure as clearing
                    original_cols = [col for col in clearing_df.columns
                                   if col not in ['cp_code_normalized', 'broker_code_normalized', 'side_normalized',
                                                 'quantity', 'price', 'ticker_normalized', 'Bloomberg Ticker']]
                    diagnostic_cols = ['DIAGNOSTIC_Ticker', 'DIAGNOSTIC_CP_Code', 'DIAGNOSTIC_Broker_Code',
                                     'DIAGNOSTIC_Side', 'DIAGNOSTIC_Qty', 'DIAGNOSTIC_Price', 'DIAGNOSTIC_Match_Failure_Reason']
                    unmatched_clear_df = pd.DataFrame(columns=original_cols + ['Comms', 'Taxes', 'TD'] + diagnostic_cols)

                unmatched_clear_df.to_excel(writer, sheet_name='Unmatched Clearing', index=False)

                # Sheet 3: Unmatched Broker - Broker format with all columns including the 3 needed + diagnostic columns
                if unmatched_broker:
                    unmatched_broker_df = broker_df.iloc[unmatched_broker].copy()

                    # Add diagnostic columns BEFORE renaming/removing columns
                    diagnostic_data = []
                    for idx in unmatched_broker:
                        broker_row = broker_df.iloc[idx]
                        reason = self._find_broker_match_failure_reason(broker_row, clearing_df)
                        diagnostic_data.append({
                            'DIAGNOSTIC_Ticker': broker_row.get('ticker_normalized', ''),
                            'DIAGNOSTIC_CP_Code': broker_row.get('cp_code_normalized', ''),
                            'DIAGNOSTIC_Broker_Code': broker_row.get('broker_code', ''),
                            'DIAGNOSTIC_Side': broker_row.get('side', ''),
                            'DIAGNOSTIC_Qty': broker_row.get('quantity', ''),
                            'DIAGNOSTIC_Price': broker_row.get('price', ''),
                            'DIAGNOSTIC_Match_Failure_Reason': reason
                        })
                    diagnostic_df = pd.DataFrame(diagnostic_data)

                    # Rename columns to match output format
                    column_rename = {
                        'bloomberg_ticker': 'Bloomberg Ticker',
                        'pure_brokerage': 'Comms',
                        'total_taxes': 'Taxes',
                        'trade_date': 'TD',
                        'broker_name': 'Broker Name'
                    }
                    unmatched_broker_df = unmatched_broker_df.rename(columns=column_rename)

                    # Combine with diagnostic data
                    unmatched_broker_df = pd.concat([unmatched_broker_df.reset_index(drop=True), diagnostic_df], axis=1)

                    # Remove duplicate ticker column and internal normalized columns
                    cols_to_drop = ['ticker', 'ticker_normalized', 'cp_code_normalized']
                    cols_to_drop = [col for col in cols_to_drop if col in unmatched_broker_df.columns]
                    if cols_to_drop:
                        unmatched_broker_df = unmatched_broker_df.drop(columns=cols_to_drop)

                    # Format expiry_date as date (not datetime)
                    if 'expiry_date' in unmatched_broker_df.columns:
                        unmatched_broker_df['expiry_date'] = pd.to_datetime(unmatched_broker_df['expiry_date'], dayfirst=True, errors='coerce').dt.date

                    # Format TD as date only if it has valid values
                    if 'TD' in unmatched_broker_df.columns:
                        def convert_td(val):
                            if pd.isna(val) or val == '' or val is None:
                                return ''
                            try:
                                # Use dayfirst=True to parse as DD/MM/YYYY
                                parsed_date = pd.to_datetime(val, dayfirst=True, errors='coerce')
                                if pd.notna(parsed_date):
                                    return parsed_date.date()
                                return val
                            except:
                                return val
                        unmatched_broker_df['TD'] = unmatched_broker_df['TD'].apply(convert_td)
                else:
                    # Create empty dataframe with broker structure (without duplicate ticker)
                    diagnostic_cols = ['DIAGNOSTIC_Ticker', 'DIAGNOSTIC_CP_Code', 'DIAGNOSTIC_Broker_Code',
                                     'DIAGNOSTIC_Side', 'DIAGNOSTIC_Qty', 'DIAGNOSTIC_Price', 'DIAGNOSTIC_Match_Failure_Reason']
                    unmatched_broker_df = pd.DataFrame(columns=['Bloomberg Ticker', 'symbol', 'instrument', 'expiry_date',
                                                                'strike', 'security_type', 'side', 'quantity', 'price',
                                                                'Comms', 'Taxes', 'TD', 'Broker Name'] + diagnostic_cols)

                unmatched_broker_df.to_excel(writer, sheet_name='Unmatched Broker', index=False)

                # Sheet 4: Commission Report - Average commission rate by broker
                if matched:
                    comm_report_rows = []
                    for match in matched:
                        broker_row = broker_df.iloc[match['broker_idx']]
                        clear_row = clearing_df.iloc[match['clearing_idx']]

                        # Get trade details
                        price = broker_row.get('price', 0)
                        quantity = broker_row.get('quantity', 0)
                        brokerage = broker_row.get('pure_brokerage', 0)
                        taxes = broker_row.get('total_taxes', 0)
                        security_type = broker_row.get('security_type', '')
                        lots = broker_row.get('lots', 0)

                        trade_value = price * quantity

                        # For options (non-futures), calculate per-lot commission rate
                        # For futures, calculate percentage commission rate
                        if security_type == 'Futures':
                            comm_rate = (brokerage / trade_value * 100) if trade_value > 0 else 0
                            comm_rate_display = f"{comm_rate:.4f}%"
                        else:
                            # Options: brokerage per lot
                            comm_per_lot = (brokerage / lots) if lots > 0 else 0
                            comm_rate_display = f"₹{comm_per_lot:.2f}/lot"

                        # Tax rate: taxes / trade value (irrespective of instrument type)
                        tax_rate = (taxes / trade_value * 100) if trade_value > 0 else 0

                        comm_report_rows.append({
                            'Broker Name': broker_row.get('broker_name', ''),
                            'Broker Code': broker_row.get('broker_code', ''),
                            'Bloomberg Ticker': broker_row.get('bloomberg_ticker', ''),
                            'Instrument': security_type,
                            'Side': broker_row.get('side', ''),
                            'Lots': lots if lots > 0 else '',
                            'Quantity': quantity,
                            'Price': price,
                            'Trade Value': trade_value,
                            'Brokerage': brokerage,
                            'Comm Rate': comm_rate_display,
                            'Taxes': taxes,
                            'Tax Rate (%)': tax_rate
                        })

                    comm_report_df = pd.DataFrame(comm_report_rows)

                    # Add summary by broker at the bottom
                    if not comm_report_df.empty:
                        # Separate futures and options for summary
                        summary_rows = []

                        # Group by broker and instrument type
                        for (broker_name, broker_code), group in comm_report_df.groupby(['Broker Name', 'Broker Code']):
                            # Futures summary
                            futures_trades = group[group['Instrument'] == 'Futures']
                            if not futures_trades.empty:
                                total_value = futures_trades['Trade Value'].sum()
                                total_brokerage = futures_trades['Brokerage'].sum()
                                total_taxes = futures_trades['Taxes'].sum()
                                total_quantity = futures_trades['Quantity'].sum()
                                avg_comm_rate = (total_brokerage / total_value * 100) if total_value > 0 else 0
                                avg_tax_rate = (total_taxes / total_value * 100) if total_value > 0 else 0

                                summary_rows.append({
                                    'Broker Name': broker_name,
                                    'Broker Code': broker_code,
                                    'Bloomberg Ticker': f"{len(futures_trades)} Futures trades",
                                    'Instrument': 'Futures',
                                    'Side': '',
                                    'Lots': '',
                                    'Quantity': total_quantity,
                                    'Price': '',
                                    'Trade Value': total_value,
                                    'Brokerage': total_brokerage,
                                    'Comm Rate': f"{avg_comm_rate:.4f}%",
                                    'Taxes': total_taxes,
                                    'Tax Rate (%)': avg_tax_rate
                                })

                            # Options summary
                            options_trades = group[group['Instrument'] != 'Futures']
                            if not options_trades.empty:
                                total_lots = options_trades['Lots'].replace('', 0).astype(float).sum()
                                total_value = options_trades['Trade Value'].sum()
                                total_brokerage = options_trades['Brokerage'].sum()
                                total_taxes = options_trades['Taxes'].sum()
                                total_quantity = options_trades['Quantity'].sum()
                                avg_comm_per_lot = (total_brokerage / total_lots) if total_lots > 0 else 0
                                avg_tax_rate = (total_taxes / total_value * 100) if total_value > 0 else 0

                                summary_rows.append({
                                    'Broker Name': broker_name,
                                    'Broker Code': broker_code,
                                    'Bloomberg Ticker': f"{len(options_trades)} Options trades",
                                    'Instrument': 'Options',
                                    'Side': '',
                                    'Lots': total_lots,
                                    'Quantity': total_quantity,
                                    'Price': '',
                                    'Trade Value': total_value,
                                    'Brokerage': total_brokerage,
                                    'Comm Rate': f"₹{avg_comm_per_lot:.2f}/lot",
                                    'Taxes': total_taxes,
                                    'Tax Rate (%)': avg_tax_rate
                                })

                        # Add separator and summary to report
                        if summary_rows:
                            separator_row = pd.DataFrame([{col: '' for col in comm_report_df.columns}])
                            summary_header = pd.DataFrame([{
                                'Broker Name': 'BROKER SUMMARY',
                                **{col: '' for col in comm_report_df.columns if col != 'Broker Name'}
                            }])
                            broker_summary_df = pd.DataFrame(summary_rows)

                            # Combine all
                            comm_report_df = pd.concat([
                                comm_report_df,
                                separator_row,
                                summary_header,
                                broker_summary_df
                            ], ignore_index=True)

                    comm_report_df.to_excel(writer, sheet_name='Commission Report', index=False)
                else:
                    # Empty commission report
                    empty_comm = pd.DataFrame(columns=['Broker Name', 'Broker Code', 'Bloomberg Ticker',
                                                       'Instrument', 'Side', 'Lots', 'Quantity', 'Price',
                                                       'Trade Value', 'Brokerage', 'Comm Rate', 'Taxes', 'Tax Rate (%)'])
                    empty_comm.to_excel(writer, sheet_name='Commission Report', index=False)

                # Sheet 5: Summary
                summary_data = {
                    'Metric': [
                        'Total Clearing Trades',
                        'Total Broker Trades',
                        'Matched Trades',
                        'Unmatched Clearing Trades',
                        'Unmatched Broker Trades',
                        'Match Rate (%)',
                        'Total Brokerage',
                        'Total Taxes'
                    ],
                    'Value': [
                        len(clearing_df),
                        len(broker_df),
                        len(matched),
                        len(unmatched_clearing),
                        len(unmatched_broker),
                        round(len(matched) / len(clearing_df) * 100, 2) if len(clearing_df) > 0 else 0,
                        sum([broker_df.iloc[m['broker_idx']]['pure_brokerage'] for m in matched]) if matched else 0,
                        sum([broker_df.iloc[m['broker_idx']]['total_taxes'] for m in matched]) if matched else 0
                    ]
                }
                summary_df = pd.DataFrame(summary_data)
                summary_df.to_excel(writer, sheet_name='Summary', index=False)

            logger.info(f"Generated reconciliation report: {filename}")
            return str(filepath)

        except Exception as e:
            logger.error(f"Error generating reconciliation report: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return ""
