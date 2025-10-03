"""
Broker Parser Module
Parses executing broker files and generates Bloomberg tickers
"""

import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List, Optional
import logging
import io

logger = logging.getLogger(__name__)


def decrypt_excel_file(file_obj):
    """
    Try to decrypt password-protected Excel file with known passwords.
    Returns decrypted file object or None if not encrypted/failed.

    This is a shared utility for all Excel file reading (clearing and broker files).
    """
    try:
        import msoffcrypto
        from io import BytesIO

        file_obj.seek(0)

        # Known passwords for encrypted files
        passwords = ['Aurigin2024', 'Aurigin2017']

        for password in passwords:
            try:
                file_obj.seek(0)
                ms_file = msoffcrypto.OfficeFile(file_obj)

                # Try to decrypt with this password
                decrypted = BytesIO()
                ms_file.load_key(password=password)
                ms_file.decrypt(decrypted)
                decrypted.seek(0)

                logger.info(f"Successfully decrypted Excel file with password '{password}'")
                return decrypted

            except Exception as e:
                # Password didn't work, try next one
                continue

        # No password worked, file might not be encrypted
        file_obj.seek(0)
        return None

    except ImportError:
        logger.warning("msoffcrypto-tool not installed, cannot decrypt password-protected files")
        return None
    except Exception as e:
        logger.debug(f"File is not encrypted or decryption not needed: {e}")
        return None

# Month codes for Bloomberg tickers
MONTH_CODE = {
    1: "F", 2: "G", 3: "H", 4: "J", 5: "K", 6: "M",
    7: "N", 8: "Q", 9: "U", 10: "V", 11: "X", 12: "Z"
}

# Special index ticker mappings
INDEX_TICKER_RULES = {
    'NIFTY': {'futures_ticker': 'NZ', 'options_ticker': 'NIFTY', 'is_index': True},
    'NZ': {'futures_ticker': 'NZ', 'options_ticker': 'NIFTY', 'is_index': True},
    'BANKNIFTY': {'futures_ticker': 'AF1', 'options_ticker': 'NSEBANK', 'is_index': True},
    'AF1': {'futures_ticker': 'AF1', 'options_ticker': 'NSEBANK', 'is_index': True},
    'AF': {'futures_ticker': 'AF1', 'options_ticker': 'NSEBANK', 'is_index': True},
    'NSEBANK': {'futures_ticker': 'AF1', 'options_ticker': 'NSEBANK', 'is_index': True},
    'MIDCPNIFTY': {'futures_ticker': 'RNS', 'options_ticker': 'NMIDSELP', 'is_index': True},
    'RNS': {'futures_ticker': 'RNS', 'options_ticker': 'NMIDSELP', 'is_index': True},
    'NMIDSELP': {'futures_ticker': 'RNS', 'options_ticker': 'NMIDSELP', 'is_index': True},
    'MCN': {'futures_ticker': 'RNS', 'options_ticker': 'NMIDSELP', 'is_index': True}
}


class BrokerParserBase:
    """Base class for broker parsers"""

    def __init__(self, futures_mapping_file: str = "futures mapping.csv"):
        self.futures_mapping_file = futures_mapping_file
        self.symbol_to_ticker = {}
        self.ticker_to_underlying = {}
        self._load_mappings()

    def _add_lots_if_available(self, parsed_row: dict, row, df: pd.DataFrame):
        """Add lots to parsed row if available in source data"""
        # Check for various lot column names
        lot_columns = ['Lots traded', 'Lots Traded', 'Lots', 'lots', 'Contract Lot', 'Contract Lots', 'No Of Traded Lots', 'No. of Contracts', 'No of Contracts']
        for col in lot_columns:
            if col in df.columns:
                try:
                    parsed_row['lots'] = abs(float(row[col])) if pd.notna(row[col]) else 0
                    return
                except:
                    continue

    def _get_broker_code_from_row(self, row, df: pd.DataFrame, default_code: int) -> int:
        """
        Get broker code from row, reading from Broker Code or TM Code columns if available.
        Falls back to default_code if not found.

        Args:
            row: DataFrame row
            df: Full DataFrame (to check column existence)
            default_code: Default broker code to use if not found in file

        Returns:
            Broker code as integer
        """
        # Check for broker code columns
        broker_code_columns = ['Broker Code', 'BrokerNSECode', 'Broker NSE Code', 'TM Code', 'TM_Code']

        for col in broker_code_columns:
            if col in df.columns and pd.notna(row.get(col)):
                try:
                    broker_code_str = str(row[col]).strip().lstrip('0')
                    if broker_code_str:
                        return abs(int(float(broker_code_str)))
                except:
                    continue

        # No broker code found, return default
        return default_code

    def _load_mappings(self):
        """Load futures mapping file"""
        try:
            df = pd.read_csv(self.futures_mapping_file, skiprows=3)

            # Build symbol to ticker mapping
            for idx, row in df.iterrows():
                symbol = row.get('Symbol')
                ticker = row.get('Ticker')
                cash = row.get('Cash ')

                if pd.notna(symbol) and pd.notna(ticker):
                    self.symbol_to_ticker[str(symbol).strip().upper()] = str(ticker).strip()

                if pd.notna(ticker):
                    ticker_clean = str(ticker).strip()
                    self.symbol_to_ticker[ticker_clean.upper()] = ticker_clean

                    # Store underlying
                    if pd.notna(cash):
                        self.ticker_to_underlying[ticker_clean] = str(cash).strip()

            logger.info(f"Loaded {len(self.symbol_to_ticker)} symbol mappings")
        except Exception as e:
            logger.error(f"Error loading futures mapping: {e}")

    def _get_ticker_for_symbol(self, symbol: str) -> Optional[str]:
        """Get ticker for a given symbol"""
        symbol_upper = str(symbol).strip().upper()

        # Check direct mapping
        if symbol_upper in self.symbol_to_ticker:
            return self.symbol_to_ticker[symbol_upper]

        # Check if already a ticker
        return symbol_upper

    def _generate_bloomberg_ticker(self, ticker: str, expiry: datetime,
                                  security_type: str, strike: float = 0,
                                  instrument: str = None) -> str:
        """Generate Bloomberg ticker format"""
        ticker_upper = ticker.upper()

        # Determine if index
        is_index = False
        if instrument:
            instrument_upper = instrument.upper()
            if 'IDX' in instrument_upper or 'INDEX' in instrument_upper:
                is_index = True

        if ticker_upper in ['NZ', 'NBZ', 'NIFTY', 'BANKNIFTY', 'AF1', 'NSEBANK', 'RNS', 'NMIDSELP', 'MCN', 'MIDCPNIFTY'] or 'NIFTY' in ticker_upper:
            is_index = True

        # Generate ticker based on security type
        if security_type == 'Futures':
            month_code = MONTH_CODE.get(expiry.month, "")
            year_code = str(expiry.year)[-1]

            if is_index:
                return f"{ticker}{month_code}{year_code} Index"
            else:
                return f"{ticker}={month_code}{year_code} IS Equity"
        else:
            # Options
            date_str = expiry.strftime('%m/%d/%y')
            strike_str = str(int(strike)) if strike == int(strike) else str(strike)

            if is_index:
                if security_type == 'Call':
                    return f"{ticker} {date_str} C{strike_str} Index"
                else:
                    return f"{ticker} {date_str} P{strike_str} Index"
            else:
                if security_type == 'Call':
                    return f"{ticker} IS {date_str} C{strike_str} Equity"
                else:
                    return f"{ticker} IS {date_str} P{strike_str} Equity"

    def _parse_date(self, date_str: str) -> Optional[datetime]:
        """Parse date string in various formats"""
        date_str = str(date_str).strip()

        formats = [
            '%d/%m/%Y', '%d-%m-%Y', '%d.%m.%Y',
            '%m/%d/%Y', '%m-%d-%Y', '%m.%d.%Y',
            '%Y-%m-%d', '%Y/%m/%d', '%Y.%m.%d',
            '%d/%m/%y', '%d-%m-%y', '%d.%m.%y',
            '%m/%d/%y', '%m-%d-%y', '%m.%d.%y',
            '%d-%b-%Y', '%d-%b-%y',
        ]

        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt)
            except:
                continue

        try:
            return pd.to_datetime(date_str)
        except:
            logger.warning(f"Could not parse date: {date_str}")
            return None

    def parse_file(self, file_obj) -> pd.DataFrame:
        """Parse broker file - to be implemented by subclasses"""
        raise NotImplementedError()


class IciciParser(BrokerParserBase):
    """Parser for ICICI Securities files"""

    def parse_file(self, file_obj) -> pd.DataFrame:
        """Parse ICICI broker file"""
        try:
            # Try to decrypt if password-protected
            file_obj.seek(0)
            decrypted_file = decrypt_excel_file(file_obj)
            if decrypted_file:
                file_obj = decrypted_file

            # Read Excel file
            df = pd.read_excel(file_obj)

            logger.info(f"Read ICICI file with {len(df)} rows")

            # Expected columns
            required_cols = ['CP Code', 'Broker Code', 'Scrip Code', 'Segment Type',
                           'Expiry', 'Strike Price', 'Call / Put', 'Buy / Sell',
                           'Qty', 'Mkt. Rate', 'Pure Brokerage AMT', 'Total Taxes', 'Trade Date']

            # Check if all required columns exist
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                logger.error(f"Missing columns in ICICI file: {missing_cols}")
                return pd.DataFrame()

            # Process each row
            parsed_rows = []

            for idx, row in df.iterrows():
                try:
                    # Extract fields
                    scrip_code = str(row['Scrip Code']).strip()
                    segment_type = str(row['Segment Type']).strip().upper()

                    # Map segment type to instrument
                    if 'STOCK' in segment_type and 'FUTURE' in segment_type:
                        instrument = 'FUTSTK'
                        security_type = 'Futures'
                    elif 'INDEX' in segment_type and 'FUTURE' in segment_type:
                        instrument = 'FUTIDX'
                        security_type = 'Futures'
                    elif 'STOCK' in segment_type and 'OPTION' in segment_type:
                        instrument = 'OPTSTK'
                        option_type = str(row['Call / Put']).strip()
                        security_type = 'Call' if 'CALL' in option_type.upper() else 'Put'
                    elif 'INDEX' in segment_type and 'OPTION' in segment_type:
                        instrument = 'OPTIDX'
                        option_type = str(row['Call / Put']).strip()
                        security_type = 'Call' if 'CALL' in option_type.upper() else 'Put'
                    else:
                        logger.warning(f"Unknown segment type: {segment_type}")
                        continue

                    # Get ticker for symbol
                    ticker = self._get_ticker_for_symbol(scrip_code)
                    if not ticker:
                        logger.warning(f"No ticker found for symbol: {scrip_code}")
                        continue

                    # Parse expiry
                    expiry = self._parse_date(row['Expiry'])
                    if not expiry:
                        continue

                    # Get strike price
                    strike = float(row['Strike Price']) if pd.notna(row['Strike Price']) else 0

                    # Generate Bloomberg ticker
                    bloomberg_ticker = self._generate_bloomberg_ticker(
                        ticker, expiry, security_type, strike, instrument
                    )

                    # Normalize B/S
                    side = str(row['Buy / Sell']).strip()
                    side_normalized = 'Buy' if side.upper().startswith('B') else 'Sell'

                    # Build parsed row
                    parsed_row = {
                        'bloomberg_ticker': bloomberg_ticker,
                        'cp_code': str(row['CP Code']).strip().upper(),
                        'broker_code': self._get_broker_code_from_row(row, df, 7730),  # Read from file, default ICICI
                        'side': side_normalized,
                        'quantity': int(row['Qty']),
                        'price': float(row['Mkt. Rate']),
                        'pure_brokerage': float(row['Pure Brokerage AMT']),
                        'total_taxes': round(float(row['Total Taxes']), 2),
                        'trade_date': row['Trade Date'].strftime('%d/%m/%Y') if isinstance(row['Trade Date'], (datetime, pd.Timestamp)) else str(row['Trade Date']).strip(),
                        'scrip_code': scrip_code,
                        'instrument': instrument,
                        'security_type': security_type,
                        'strike': strike,
                        'expiry_date': expiry.strftime('%d/%m/%Y'),
                        'ticker': ticker
                    }

                    # Add lots if available
                    self._add_lots_if_available(parsed_row, row, df)

                    parsed_rows.append(parsed_row)

                except Exception as e:
                    logger.warning(f"Error parsing ICICI row {idx}: {e}")
                    continue

            result_df = pd.DataFrame(parsed_rows)
            logger.info(f"Parsed {len(result_df)} ICICI trades with Bloomberg tickers")
            return result_df

        except Exception as e:
            logger.error(f"Error parsing ICICI file: {e}")
            return pd.DataFrame()


class KotakParser(BrokerParserBase):
    """Parser for Kotak Securities files"""

    def _parse_scrip(self, scrip: str):
        """Parse Kotak scrip format like 'ACC23OCT1800PE' or 'NIFTY23OCTFUT'"""
        import re

        scrip = scrip.strip().upper()

        # Pattern: SYMBOL + YYMMM + (STRIKE + CE/PE) or FUT
        # Examples: ACC23OCT1800PE, NIFTY23OCTFUT, BANKNIFTY23OCT45000CE

        # For futures: ends with FUT
        if scrip.endswith('FUT'):
            # Extract symbol and date
            match = re.match(r'([A-Z]+)(\d{2})([A-Z]{3})FUT', scrip)
            if match:
                symbol = match.group(1)
                year = match.group(2)
                month = match.group(3)
                return symbol, year, month, 0, None, 'FUT'

        # For options: ends with CE or PE
        match = re.match(r'([A-Z]+)(\d{2})([A-Z]{3})(\d+)(CE|PE)', scrip)
        if match:
            symbol = match.group(1)
            year = match.group(2)
            month = match.group(3)
            strike = int(match.group(4))
            option_type = match.group(5)
            return symbol, year, month, strike, option_type, 'OPT'

        logger.warning(f"Could not parse scrip: {scrip}")
        return None, None, None, None, None, None

    def _convert_month(self, month_str: str, year_str: str) -> Optional[datetime]:
        """Convert month string like 'OCT' and year '23' to expiry date"""
        month_map = {
            'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
            'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12
        }

        month = month_map.get(month_str.upper())
        if not month:
            return None

        # Year is 2-digit, assume 20xx
        year = 2000 + int(year_str)

        # Get last Thursday of the month (typical expiry)
        from calendar import monthrange
        last_day = monthrange(year, month)[1]

        # Find last Thursday
        for day in range(last_day, 0, -1):
            d = datetime(year, month, day)
            if d.weekday() == 3:  # Thursday
                return d

        # Fallback to last day
        return datetime(year, month, last_day)

    def parse_file(self, file_obj) -> pd.DataFrame:
        """Parse Kotak broker file (supports both old and new formats)"""
        try:
            # Try to decrypt if password-protected
            file_obj.seek(0)
            decrypted_file = decrypt_excel_file(file_obj)
            if decrypted_file:
                file_obj = decrypted_file

            # Read Excel file
            df = pd.read_excel(file_obj)

            logger.info(f"Read Kotak file with {len(df)} rows")

            # Detect format: New format has Symbol, Expiry Date, Strike Price, Option Type columns
            new_format_cols = ['Symbol', 'Expiry Date', 'Strike Price', 'Option Type']
            is_new_format = all(col in df.columns for col in new_format_cols)

            if is_new_format:
                logger.info("Detected new Kotak format with separate columns")
                return self._parse_new_format(df)
            else:
                logger.info("Detected old Kotak format with combined Scrip")
                return self._parse_old_format(df)

        except Exception as e:
            logger.error(f"Error parsing Kotak file: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return pd.DataFrame()

    def _parse_new_format(self, df: pd.DataFrame) -> pd.DataFrame:
        """Parse new Kotak format with separate Symbol, Expiry, Strike, Option Type columns"""
        try:
            # Required columns for new format
            required_cols = ['Symbol', 'Expiry Date', 'Strike Price', 'Option Type', 'Instrument',
                           'Buy/Sell', 'Quantity', 'Traded Price', 'Brokerage', 'Total Taxes']

            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                logger.error(f"Missing columns in new Kotak format: {missing_cols}")
                return pd.DataFrame()

            parsed_rows = []

            for idx, row in df.iterrows():
                try:
                    # Get symbol
                    symbol = str(row['Symbol']).strip().upper()

                    # Get instrument (already in correct format)
                    instrument = str(row['Instrument']).strip().upper()

                    # Get option type (handle NaN for futures)
                    option_type_val = row['Option Type']
                    if pd.isna(option_type_val):
                        option_type = ''
                    else:
                        option_type = str(option_type_val).strip().upper()

                    # Determine security type
                    if option_type in ['FF', 'FUT', 'FUTURE', '', 'NAN']:
                        # Empty or NaN means futures
                        security_type = 'Futures'
                        strike = 0
                    elif option_type in ['CE', 'CALL']:
                        security_type = 'Call'
                        strike = float(row['Strike Price']) if pd.notna(row['Strike Price']) else 0
                    elif option_type in ['PE', 'PUT']:
                        security_type = 'Put'
                        strike = float(row['Strike Price']) if pd.notna(row['Strike Price']) else 0
                    else:
                        logger.warning(f"Unknown option type: {option_type} at row {idx}")
                        continue

                    # Get ticker for symbol
                    ticker = self._get_ticker_for_symbol(symbol)
                    if not ticker:
                        ticker = symbol

                    # Parse expiry date
                    expiry_val = row['Expiry Date']
                    if isinstance(expiry_val, (datetime, pd.Timestamp)):
                        expiry = expiry_val
                    else:
                        # Try parsing as string - multiple formats
                        expiry = None
                        expiry_str = str(expiry_val).strip()
                        for fmt in ['%d-%m-%Y', '%d-%m-%y', '%d-%b-%y', '%d-%b-%Y']:
                            try:
                                expiry = datetime.strptime(expiry_str, fmt)
                                break
                            except:
                                continue

                        if not expiry:
                            logger.warning(f"Could not parse expiry '{expiry_val}' at row {idx}")
                            continue

                    # Generate Bloomberg ticker
                    bloomberg_ticker = self._generate_bloomberg_ticker(
                        ticker, expiry, security_type, strike, instrument
                    )

                    # Normalize B/S
                    side = str(row['Buy/Sell']).strip()
                    side_normalized = 'Buy' if side.upper().startswith('B') else 'Sell'

                    # Parse quantity
                    qty_str = str(row['Quantity']).strip().replace(',', '')
                    quantity = int(float(qty_str))

                    # Get CP code
                    cp_code = str(row['CPCode']).strip().upper() if 'CPCode' in df.columns else ''

                    # Get trade date - handle Timestamp objects
                    trade_date = ''
                    if 'Trade Date' in df.columns:
                        trade_date_val = row['Trade Date']
                        if pd.notna(trade_date_val):
                            if isinstance(trade_date_val, (datetime, pd.Timestamp)):
                                trade_date = trade_date_val.strftime('%d/%m/%Y')
                            else:
                                trade_date = str(trade_date_val).strip()

                    # Build parsed row
                    parsed_row = {
                        'bloomberg_ticker': bloomberg_ticker,
                        'cp_code': cp_code,
                        'broker_code': self._get_broker_code_from_row(row, df, 8081),  # Read from file, default Kotak
                        'side': side_normalized,
                        'quantity': quantity,
                        'price': float(row['Traded Price']),
                        'pure_brokerage': float(row['Brokerage']),
                        'total_taxes': round(float(row['Total Taxes']), 2),
                        'trade_date': trade_date,
                        'symbol': symbol,
                        'instrument': instrument,
                        'security_type': security_type,
                        'strike': strike,
                        'expiry_date': expiry.strftime('%d/%m/%Y'),
                        'ticker': ticker
                    }

                    # Add lots if available
                    self._add_lots_if_available(parsed_row, row, df)

                    parsed_rows.append(parsed_row)

                except Exception as e:
                    logger.warning(f"Error parsing Kotak row {idx}: {e}")
                    continue

            result_df = pd.DataFrame(parsed_rows)
            logger.info(f"Parsed {len(result_df)} Kotak trades (new format) with Bloomberg tickers")
            return result_df

        except Exception as e:
            logger.error(f"Error parsing new Kotak format: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return pd.DataFrame()

    def _parse_old_format(self, df: pd.DataFrame) -> pd.DataFrame:
        """Parse old Kotak format with combined Scrip column"""
        try:
            # Expected columns for old format
            required_cols = ['Scrip', 'Instrument', 'Buy/Sell', 'Quantity', 'Traded Price', 'Brokerage', 'Total Taxes']

            # Check if required columns exist
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                logger.error(f"Missing columns in old Kotak format: {missing_cols}")
                return pd.DataFrame()

            # Process each row
            parsed_rows = []

            for idx, row in df.iterrows():
                try:
                    # Parse scrip to extract symbol, expiry, strike, option type
                    scrip = str(row['Scrip']).strip()
                    symbol, year, month, strike, option_type, contract_type = self._parse_scrip(scrip)

                    if not symbol:
                        logger.warning(f"Could not parse scrip '{scrip}' at row {idx}")
                        continue

                    # Get instrument
                    instrument = str(row['Instrument']).strip().upper()

                    # Determine security type
                    if contract_type == 'FUT':
                        security_type = 'Futures'
                    elif contract_type == 'OPT':
                        security_type = 'Call' if option_type == 'CE' else 'Put'
                    else:
                        logger.warning(f"Unknown contract type for {scrip}")
                        continue

                    # Get ticker for symbol
                    ticker = self._get_ticker_for_symbol(symbol)
                    if not ticker:
                        ticker = symbol  # Use symbol as ticker if not found

                    # Convert expiry
                    expiry = self._convert_month(month, year)
                    if not expiry:
                        logger.warning(f"Could not convert expiry {month}/{year} at row {idx}")
                        continue

                    # Generate Bloomberg ticker
                    bloomberg_ticker = self._generate_bloomberg_ticker(
                        ticker, expiry, security_type, strike, instrument
                    )

                    # Normalize B/S
                    side = str(row['Buy/Sell']).strip()
                    side_normalized = 'Buy' if side.upper().startswith('B') else 'Sell'

                    # Parse quantity (may have commas)
                    qty_str = str(row['Quantity']).strip().replace(',', '')
                    quantity = int(float(qty_str))

                    # Get trade date if available - handle Timestamp objects
                    trade_date = ''
                    if 'Trade Date' in df.columns:
                        trade_date_val = row['Trade Date']
                        if pd.notna(trade_date_val):
                            if isinstance(trade_date_val, (datetime, pd.Timestamp)):
                                trade_date = trade_date_val.strftime('%d/%m/%Y')
                            else:
                                trade_date = str(trade_date_val).strip()

                    # Build parsed row (no CP code available in file)
                    parsed_row = {
                        'bloomberg_ticker': bloomberg_ticker,
                        'cp_code': '',  # Not available in this format
                        'broker_code': self._get_broker_code_from_row(row, df, 8081),  # Read from file, default Kotak
                        'side': side_normalized,
                        'quantity': quantity,
                        'price': float(row['Traded Price']),
                        'pure_brokerage': float(row['Brokerage']),
                        'total_taxes': round(float(row['Total Taxes']), 2),
                        'trade_date': trade_date,
                        'symbol': symbol,
                        'scrip': scrip,
                        'instrument': instrument,
                        'security_type': security_type,
                        'strike': strike,
                        'expiry_date': expiry.strftime('%d/%m/%Y'),
                        'ticker': ticker
                    }

                    # Add lots if available
                    self._add_lots_if_available(parsed_row, row, df)

                    parsed_rows.append(parsed_row)

                except Exception as e:
                    logger.warning(f"Error parsing Kotak row {idx}: {e}")
                    continue

            result_df = pd.DataFrame(parsed_rows)
            logger.info(f"Parsed {len(result_df)} Kotak trades (old format) with Bloomberg tickers")
            return result_df

        except Exception as e:
            logger.error(f"Error parsing old Kotak format: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return pd.DataFrame()


class IIFLParser(BrokerParserBase):
    """Parser for IIFL Securities files"""

    def _parse_expiry_date(self, expiry_str: str) -> Optional[datetime]:
        """Parse expiry date from dd-mmm-yy format (e.g., 29-Sep-23)"""
        try:
            # Parse dd-mmm-yy format
            return datetime.strptime(expiry_str.strip(), '%d-%b-%y')
        except:
            try:
                # Try alternative format with full year
                return datetime.strptime(expiry_str.strip(), '%d-%b-%Y')
            except:
                logger.warning(f"Could not parse expiry date: {expiry_str}")
                return None

    def parse_file(self, file_obj) -> pd.DataFrame:
        """Parse IIFL broker file"""
        try:
            # Try to decrypt if password-protected
            file_obj.seek(0)
            decrypted_file = decrypt_excel_file(file_obj)
            if decrypted_file:
                file_obj = decrypted_file

            # Read Excel file
            df = pd.read_excel(file_obj)

            logger.info(f"Read IIFL file with {len(df)} rows")

            # Expected columns
            required_cols = ['Symbol', 'ExpiryDate', 'OptionType', 'BuySellStatus',
                           'Quantity', 'ConfPrice', 'BrokValue', 'Total Tax', 'Trade Date']

            # Check if required columns exist
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                logger.error(f"Missing columns in IIFL file: {missing_cols}")
                return pd.DataFrame()

            # Process each row
            parsed_rows = []

            for idx, row in df.iterrows():
                try:
                    # Get symbol
                    symbol = str(row['Symbol']).strip().upper()

                    # Determine if index or stock
                    index_symbols = ['NIFTY', 'NSEBANK', 'MIDCAP', 'BANKNIFTY', 'FINNIFTY']
                    is_index = symbol in index_symbols

                    # Get option type
                    option_type = str(row['OptionType']).strip().upper()

                    # Determine instrument and security type
                    if option_type == 'FF':
                        # Futures
                        instrument = 'FUTIDX' if is_index else 'FUTSTK'
                        security_type = 'Futures'
                        strike = 0
                    elif option_type == 'CE':
                        # Call option
                        instrument = 'OPTIDX' if is_index else 'OPTSTK'
                        security_type = 'Call'
                        strike = float(row['StrikePrice']) if pd.notna(row['StrikePrice']) else 0
                    elif option_type == 'PE':
                        # Put option
                        instrument = 'OPTIDX' if is_index else 'OPTSTK'
                        security_type = 'Put'
                        strike = float(row['StrikePrice']) if pd.notna(row['StrikePrice']) else 0
                    else:
                        logger.warning(f"Unknown option type: {option_type} at row {idx}")
                        continue

                    # Get ticker for symbol
                    ticker = self._get_ticker_for_symbol(symbol)
                    if not ticker:
                        ticker = symbol  # Use symbol as ticker if not found

                    # Parse expiry date
                    expiry = self._parse_expiry_date(str(row['ExpiryDate']))
                    if not expiry:
                        logger.warning(f"Could not parse expiry at row {idx}")
                        continue

                    # Generate Bloomberg ticker
                    bloomberg_ticker = self._generate_bloomberg_ticker(
                        ticker, expiry, security_type, strike, instrument
                    )

                    # Normalize B/S (already normalized as Buy/Sell)
                    side = str(row['BuySellStatus']).strip()

                    # Parse quantity
                    quantity = int(row['Quantity'])

                    # Get custodian code (CP code)
                    cp_code = str(row['CustodianCode']).strip().upper() if 'CustodianCode' in df.columns else ''

                    # Get trade date - handle Timestamp objects
                    trade_date_val = row['Trade Date']
                    if pd.notna(trade_date_val):
                        if isinstance(trade_date_val, (datetime, pd.Timestamp)):
                            trade_date = trade_date_val.strftime('%d/%m/%Y')
                        else:
                            trade_date = str(trade_date_val).strip()
                    else:
                        trade_date = ''

                    # Build parsed row
                    parsed_row = {
                        'bloomberg_ticker': bloomberg_ticker,
                        'cp_code': cp_code,
                        'broker_code': self._get_broker_code_from_row(row, df, 10975),  # Read from file, default IIFL
                        'side': side,
                        'quantity': quantity,
                        'price': float(row['ConfPrice']),
                        'pure_brokerage': float(row['BrokValue']),
                        'total_taxes': round(float(row['Total Tax']), 2),
                        'trade_date': trade_date,
                        'symbol': symbol,
                        'instrument': instrument,
                        'security_type': security_type,
                        'strike': strike,
                        'expiry_date': expiry.strftime('%d/%m/%Y'),
                        'ticker': ticker
                    }

                    # Add lots if available
                    self._add_lots_if_available(parsed_row, row, df)

                    parsed_rows.append(parsed_row)

                except Exception as e:
                    logger.warning(f"Error parsing IIFL row {idx}: {e}")
                    continue

            result_df = pd.DataFrame(parsed_rows)
            logger.info(f"Parsed {len(result_df)} IIFL trades with Bloomberg tickers")
            return result_df

        except Exception as e:
            logger.error(f"Error parsing IIFL file: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return pd.DataFrame()


class AxisParser(BrokerParserBase):
    """Parser for Axis Securities files"""

    def _parse_expiry_date(self, expiry_value) -> Optional[datetime]:
        """Parse expiry date from dd/mm/yyyy format or datetime object"""
        # If already a datetime/Timestamp, return it
        if isinstance(expiry_value, (datetime, pd.Timestamp)):
            return expiry_value.to_pydatetime() if isinstance(expiry_value, pd.Timestamp) else expiry_value

        # If string, try to parse it
        try:
            expiry_str = str(expiry_value).strip()
            return datetime.strptime(expiry_str, '%d/%m/%Y')
        except:
            try:
                # Try alternative format
                return datetime.strptime(expiry_str, '%d-%m-%Y')
            except:
                logger.warning(f"Could not parse expiry date: {expiry_value}")
                return None

    def parse_file(self, file_obj) -> pd.DataFrame:
        """Parse Axis broker file"""
        try:
            # Try to decrypt if password-protected
            file_obj.seek(0)
            decrypted_file = decrypt_excel_file(file_obj)
            if decrypted_file:
                file_obj = decrypted_file

            # Read Excel file
            df = pd.read_excel(file_obj)

            logger.info(f"Read Axis file with {len(df)} rows")

            # Expected columns
            required_cols = ['CP Code', 'Buy/Sell', 'Qty', 'Instrument', 'Scrip', 'OptType',
                           'Expiry', 'Mkt Price', 'Brokerage', 'GST', 'STT', 'Trade Date']

            # Check if required columns exist
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                logger.error(f"Missing columns in Axis file: {missing_cols}")
                return pd.DataFrame()

            # Process each row
            parsed_rows = []

            for idx, row in df.iterrows():
                try:
                    # Get symbol
                    symbol = str(row['Scrip']).strip().upper()

                    # Get instrument (already in correct format: FUTSTK/OPTSTK/FUTIDX/OPTIDX)
                    instrument = str(row['Instrument']).strip().upper()

                    # Get option type
                    option_type = str(row['OptType']).strip().upper()

                    # Determine security type
                    if option_type == 'FF':
                        # Futures
                        security_type = 'Futures'
                        strike = 0
                    elif option_type == 'CE':
                        # Call option
                        security_type = 'Call'
                        strike = float(row['Strike']) if pd.notna(row['Strike']) else 0
                    elif option_type == 'PE':
                        # Put option
                        security_type = 'Put'
                        strike = float(row['Strike']) if pd.notna(row['Strike']) else 0
                    else:
                        logger.warning(f"Unknown option type: {option_type} at row {idx}")
                        continue

                    # Get ticker for symbol
                    ticker = self._get_ticker_for_symbol(symbol)
                    if not ticker:
                        ticker = symbol  # Use symbol as ticker if not found

                    # Parse expiry date
                    expiry = self._parse_expiry_date(row['Expiry'])
                    if not expiry:
                        logger.warning(f"Could not parse expiry at row {idx}")
                        continue

                    # Generate Bloomberg ticker
                    bloomberg_ticker = self._generate_bloomberg_ticker(
                        ticker, expiry, security_type, strike, instrument
                    )

                    # Normalize B/S
                    side = str(row['Buy/Sell']).strip()

                    # Parse quantity
                    quantity = int(row['Qty'])

                    # Get CP code
                    cp_code = str(row['CP Code']).strip().upper()

                    # Get trade date - handle Timestamp objects
                    trade_date_val = row['Trade Date']
                    if pd.notna(trade_date_val):
                        if isinstance(trade_date_val, (datetime, pd.Timestamp)):
                            trade_date = trade_date_val.strftime('%d/%m/%Y')
                        else:
                            trade_date = str(trade_date_val).strip()
                    else:
                        trade_date = ''

                    # Calculate total taxes (GST + STT)
                    gst = float(row['GST']) if pd.notna(row['GST']) else 0
                    stt = float(row['STT']) if pd.notna(row['STT']) else 0
                    total_taxes = round(gst + stt, 2)

                    # Build parsed row
                    parsed_row = {
                        'bloomberg_ticker': bloomberg_ticker,
                        'cp_code': cp_code,
                        'broker_code': self._get_broker_code_from_row(row, df, 13872),  # Read from file, default Axis
                        'side': side,
                        'quantity': quantity,
                        'price': float(row['Mkt Price']),
                        'pure_brokerage': float(row['Brokerage']),
                        'total_taxes': total_taxes,
                        'trade_date': trade_date,
                        'symbol': symbol,
                        'instrument': instrument,
                        'security_type': security_type,
                        'strike': strike,
                        'expiry_date': expiry.strftime('%d/%m/%Y'),
                        'ticker': ticker
                    }

                    # Add lots if available
                    self._add_lots_if_available(parsed_row, row, df)

                    parsed_rows.append(parsed_row)

                except Exception as e:
                    logger.warning(f"Error parsing Axis row {idx}: {e}")
                    continue

            result_df = pd.DataFrame(parsed_rows)
            logger.info(f"Parsed {len(result_df)} Axis trades with Bloomberg tickers")
            return result_df

        except Exception as e:
            logger.error(f"Error parsing Axis file: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return pd.DataFrame()


class EquirusParser(BrokerParserBase):
    """Parser for Equirus Securities files"""

    def _parse_expiry_date(self, expiry_value) -> Optional[datetime]:
        """Parse expiry date from various formats"""
        # If already a datetime/Timestamp, return it
        if isinstance(expiry_value, (datetime, pd.Timestamp)):
            return expiry_value.to_pydatetime() if isinstance(expiry_value, pd.Timestamp) else expiry_value

        # If string, try to parse it
        try:
            expiry_str = str(expiry_value).strip()
            # Try common formats including MM/DD/YYYY (American format)
            for fmt in ['%m/%d/%Y', '%m/%d/%y', '%d-%m-%Y', '%d-%m-%y', '%d-%b-%y', '%d-%b-%Y', '%d/%m/%Y', '%d/%m/%y']:
                try:
                    return datetime.strptime(expiry_str, fmt)
                except:
                    continue
            logger.warning(f"Could not parse expiry date: {expiry_value}")
            return None
        except:
            logger.warning(f"Could not parse expiry date: {expiry_value}")
            return None

    def parse_file(self, file_obj) -> pd.DataFrame:
        """Parse Equirus broker file"""
        try:
            # Try to decrypt if password-protected
            file_obj.seek(0)
            decrypted_file = decrypt_excel_file(file_obj)
            if decrypted_file:
                file_obj = decrypted_file

            # Try reading as Excel first, then CSV
            try:
                df = pd.read_excel(file_obj)
                logger.info(f"Read Equirus file as Excel with {len(df)} rows")
            except:
                file_obj.seek(0)
                # Try reading CSV with different encodings and handle BOM
                try:
                    df = pd.read_csv(file_obj, encoding='utf-8-sig')
                except:
                    file_obj.seek(0)
                    try:
                        df = pd.read_csv(file_obj, encoding='cp1252')
                    except:
                        file_obj.seek(0)
                        df = pd.read_csv(file_obj, encoding='latin-1')
                logger.info(f"Read Equirus file as CSV with {len(df)} rows")

            # Remove empty rows
            df = df.dropna(how='all')
            logger.info(f"After removing empty rows: {len(df)} rows")
            logger.info(f"Equirus file columns: {list(df.columns)}")

            # Expected columns (Strike Price is optional - blank for futures)
            required_cols = ['CP Code', 'Scrip Code', 'Expiry', 'Buy / Sell',
                           'Qty', 'Mkt. Rate', 'Pure Brokerage AMT', 'Total Taxes', 'Trade Date']

            # Check if required columns exist
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                logger.error(f"Missing columns in Equirus file: {missing_cols}")
                logger.error(f"File has these columns: {list(df.columns)}")
                return pd.DataFrame()

            # Process each row
            parsed_rows = []

            for idx, row in df.iterrows():
                try:
                    # Get scrip code (symbol)
                    scrip_code = str(row['Scrip Code']).strip().upper()

                    # Determine if index or stock
                    index_symbols = ['NIFTY', 'NSEBANK', 'MIDCAPNIFTY', 'BANKNIFTY', 'FINNIFTY']
                    is_index = scrip_code in index_symbols

                    # Get Call/Put to determine if futures or options (column may or may not exist)
                    call_put = ''
                    if 'Call / Put' in df.columns:
                        call_put = str(row['Call / Put']).strip().upper() if pd.notna(row['Call / Put']) else ''

                    # Get Strike Price if available
                    strike_price_val = 0
                    if 'Strike Price' in df.columns and pd.notna(row['Strike Price']):
                        try:
                            strike_price_val = float(row['Strike Price'])
                        except:
                            strike_price_val = 0

                    # Determine instrument and security type
                    if not call_put or call_put == 'NAN' or call_put == '':
                        # Blank = Futures
                        instrument = 'FUTIDX' if is_index else 'FUTSTK'
                        security_type = 'Futures'
                        strike = 0
                    elif call_put in ['CALL', 'C', 'CE']:
                        # Call option
                        instrument = 'OPTIDX' if is_index else 'OPTSTK'
                        security_type = 'Call'
                        strike = strike_price_val
                    elif call_put in ['PUT', 'P', 'PE']:
                        # Put option
                        instrument = 'OPTIDX' if is_index else 'OPTSTK'
                        security_type = 'Put'
                        strike = strike_price_val
                    else:
                        logger.warning(f"Unknown Call/Put value: {call_put} at row {idx}")
                        continue

                    # Get ticker for symbol
                    ticker = self._get_ticker_for_symbol(scrip_code)
                    if not ticker:
                        ticker = scrip_code  # Use scrip code as ticker if not found

                    # Parse expiry date
                    expiry = self._parse_expiry_date(row['Expiry'])
                    if not expiry:
                        logger.warning(f"Could not parse expiry at row {idx}")
                        continue

                    # Generate Bloomberg ticker
                    bloomberg_ticker = self._generate_bloomberg_ticker(
                        ticker, expiry, security_type, strike, instrument
                    )

                    # Normalize B/S
                    side = str(row['Buy / Sell']).strip()
                    side_normalized = 'Buy' if side.upper().startswith('B') else 'Sell'

                    # Parse quantity
                    quantity = int(row['Qty'])

                    # Get CP code
                    cp_code = str(row['CP Code']).strip().upper()

                    # Get trade date - handle Timestamp objects
                    trade_date_val = row['Trade Date']
                    if pd.notna(trade_date_val):
                        if isinstance(trade_date_val, (datetime, pd.Timestamp)):
                            trade_date = trade_date_val.strftime('%d/%m/%Y')
                        else:
                            trade_date = str(trade_date_val).strip()
                    else:
                        trade_date = ''

                    # Build parsed row
                    parsed_row = {
                        'bloomberg_ticker': bloomberg_ticker,
                        'cp_code': cp_code,
                        'broker_code': self._get_broker_code_from_row(row, df, 13017),  # Read from file, default Equirus
                        'side': side_normalized,
                        'quantity': quantity,
                        'price': float(row['Mkt. Rate']),
                        'pure_brokerage': float(row['Pure Brokerage AMT']),
                        'total_taxes': round(float(row['Total Taxes']), 2),
                        'trade_date': trade_date,
                        'symbol': scrip_code,
                        'instrument': instrument,
                        'security_type': security_type,
                        'strike': strike,
                        'expiry_date': expiry.strftime('%d/%m/%Y'),
                        'ticker': ticker
                    }

                    # Add lots if available
                    self._add_lots_if_available(parsed_row, row, df)

                    parsed_rows.append(parsed_row)

                except Exception as e:
                    logger.warning(f"Error parsing Equirus row {idx}: {e}")
                    continue

            result_df = pd.DataFrame(parsed_rows)
            logger.info(f"Parsed {len(result_df)} Equirus trades with Bloomberg tickers")
            return result_df

        except Exception as e:
            logger.error(f"Error parsing Equirus file: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return pd.DataFrame()


class EdelweissParser(BrokerParserBase):
    """Parser for Edelweiss Securities files"""

    def _parse_expiry_date(self, expiry_value) -> Optional[datetime]:
        """Parse expiry date from various formats"""
        # If already a datetime/Timestamp, return it
        if isinstance(expiry_value, (datetime, pd.Timestamp)):
            return expiry_value.to_pydatetime() if isinstance(expiry_value, pd.Timestamp) else expiry_value

        # If string, try to parse it
        try:
            expiry_str = str(expiry_value).strip()
            # Try common formats
            for fmt in ['%d/%m/%Y', '%d/%m/%y', '%d-%m-%Y', '%d-%m-%y', '%d-%b-%y', '%d-%b-%Y']:
                try:
                    return datetime.strptime(expiry_str, fmt)
                except:
                    continue
            logger.warning(f"Could not parse expiry date: {expiry_value}")
            return None
        except:
            logger.warning(f"Could not parse expiry date: {expiry_value}")
            return None

    def parse_file(self, file_obj) -> pd.DataFrame:
        """Parse Edelweiss broker file"""
        try:
            # Try to decrypt if password-protected
            file_obj.seek(0)
            decrypted_file = decrypt_excel_file(file_obj)
            if decrypted_file:
                file_obj = decrypted_file

            # Read Excel file
            df = pd.read_excel(file_obj)

            logger.info(f"Read Edelweiss file with {len(df)} rows")

            # Expected columns
            required_cols = ['CP Code', 'Buy/Sell', 'Qty', 'Instrument', 'Scrip', 'OptType',
                           'Expiry', 'Mkt. Price', 'Brokerage', 'GST', 'STT', 'Trade Date']

            # Check if required columns exist
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                logger.error(f"Missing columns in Edelweiss file: {missing_cols}")
                return pd.DataFrame()

            # Process each row
            parsed_rows = []

            for idx, row in df.iterrows():
                try:
                    # Get symbol
                    symbol = str(row['Scrip']).strip().upper()

                    # Get instrument (already in correct format: FUTSTK/OPTSTK/FUTIDX/OPTIDX)
                    instrument = str(row['Instrument']).strip().upper()

                    # Get option type (handle NaN for futures)
                    option_type_val = row['OptType']
                    if pd.isna(option_type_val):
                        option_type = ''
                    else:
                        option_type = str(option_type_val).strip().upper()

                    # Determine security type
                    if option_type in ['FF', '', 'NAN']:
                        # Futures (FF or empty/NaN)
                        security_type = 'Futures'
                        strike = 0
                    elif option_type == 'CE':
                        # Call option
                        security_type = 'Call'
                        strike = float(row['Strike']) if pd.notna(row['Strike']) else 0
                    elif option_type == 'PE':
                        # Put option
                        security_type = 'Put'
                        strike = float(row['Strike']) if pd.notna(row['Strike']) else 0
                    else:
                        logger.warning(f"Unknown option type: {option_type} at row {idx}")
                        continue

                    # Get ticker for symbol
                    ticker = self._get_ticker_for_symbol(symbol)
                    if not ticker:
                        ticker = symbol  # Use symbol as ticker if not found

                    # Parse expiry date
                    expiry = self._parse_expiry_date(row['Expiry'])
                    if not expiry:
                        logger.warning(f"Could not parse expiry at row {idx}")
                        continue

                    # Generate Bloomberg ticker
                    bloomberg_ticker = self._generate_bloomberg_ticker(
                        ticker, expiry, security_type, strike, instrument
                    )

                    # Normalize B/S
                    side = str(row['Buy/Sell']).strip()

                    # Parse quantity
                    quantity = int(row['Qty'])

                    # Get CP code
                    cp_code = str(row['CP Code']).strip().upper()

                    # Get trade date - handle Timestamp objects
                    trade_date_val = row['Trade Date']
                    if pd.notna(trade_date_val):
                        if isinstance(trade_date_val, (datetime, pd.Timestamp)):
                            trade_date = trade_date_val.strftime('%d/%m/%Y')
                        else:
                            trade_date = str(trade_date_val).strip()
                    else:
                        trade_date = ''

                    # Calculate GST and STT
                    gst = float(row['GST']) if pd.notna(row['GST']) else 0
                    stt = float(row['STT']) if pd.notna(row['STT']) else 0
                    total_taxes = round(gst + stt, 2)

                    # Brokerage column is already pure brokerage (not inclusive of GST)
                    pure_brokerage = float(row['Brokerage']) if pd.notna(row['Brokerage']) else 0

                    # Build parsed row
                    parsed_row = {
                        'bloomberg_ticker': bloomberg_ticker,
                        'cp_code': cp_code,
                        'broker_code': self._get_broker_code_from_row(row, df, 11933),  # Read from file, default Edelweiss
                        'side': side,
                        'quantity': quantity,
                        'price': float(row['Mkt. Price']),
                        'pure_brokerage': pure_brokerage,
                        'total_taxes': total_taxes,
                        'trade_date': trade_date,
                        'symbol': symbol,
                        'instrument': instrument,
                        'security_type': security_type,
                        'strike': strike,
                        'expiry_date': expiry.strftime('%d/%m/%Y'),
                        'ticker': ticker
                    }

                    # Add lots if available
                    self._add_lots_if_available(parsed_row, row, df)

                    parsed_rows.append(parsed_row)

                except Exception as e:
                    logger.warning(f"Error parsing Edelweiss row {idx}: {e}")
                    continue

            result_df = pd.DataFrame(parsed_rows)
            logger.info(f"Parsed {len(result_df)} Edelweiss trades with Bloomberg tickers")
            return result_df

        except Exception as e:
            logger.error(f"Error parsing Edelweiss file: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return pd.DataFrame()


class MorganStanleyParser(BrokerParserBase):
    """Parser for Morgan Stanley files"""

    def _parse_expiry_date(self, expiry_value) -> Optional[datetime]:
        """Parse expiry date from Morgan Stanley format (e.g., '28-Oct-25')"""
        if isinstance(expiry_value, (datetime, pd.Timestamp)):
            return expiry_value.to_pydatetime() if isinstance(expiry_value, pd.Timestamp) else expiry_value

        try:
            expiry_str = str(expiry_value).strip()
            # Try common formats
            for fmt in ['%d-%b-%y', '%d-%b-%Y', '%d-%B-%y', '%d-%B-%Y', '%d/%m/%Y', '%d/%m/%y']:
                try:
                    return datetime.strptime(expiry_str, fmt)
                except:
                    continue
            logger.warning(f"Could not parse expiry date: {expiry_value}")
            return None
        except:
            logger.warning(f"Could not parse expiry date: {expiry_value}")
            return None

    def parse_file(self, file_obj) -> pd.DataFrame:
        """Parse Morgan Stanley broker file (CSV or Excel with header rows)"""
        try:
            file_obj.seek(0)

            # Try reading as Excel first
            try:
                # Try to decrypt if password-protected
                decrypted_file = decrypt_excel_file(file_obj)
                if decrypted_file:
                    file_to_use = decrypted_file
                    logger.info("Morgan Stanley file was password-protected, using decrypted version")
                else:
                    file_to_use = file_obj

                # Read all sheets and find the one with Trade Date
                xls = pd.ExcelFile(file_to_use)
                df = None
                header_row_idx = None
                target_sheet = None

                for sheet_name in xls.sheet_names:
                    file_to_use.seek(0)
                    temp_df = pd.read_excel(file_to_use, sheet_name=sheet_name, header=None)

                    # Find header row
                    for idx, row in temp_df.iterrows():
                        row_str = ' '.join([str(val) for val in row if pd.notna(val)])
                        if 'Trade Date' in row_str and 'CP Code' in row_str:
                            logger.info(f"Found Morgan Stanley header at row {idx} in sheet '{sheet_name}'")
                            header_row_idx = idx
                            target_sheet = sheet_name
                            break

                    if header_row_idx is not None:
                        break

                if header_row_idx is None:
                    logger.error("Could not find header row (with 'Trade Date' and 'CP Code') in any Excel sheet")
                    return pd.DataFrame()

                # Re-read with proper header
                file_to_use.seek(0)
                df = pd.read_excel(file_to_use, sheet_name=target_sheet, header=header_row_idx)

                logger.info(f"Read Morgan Stanley Excel file with {len(df)} rows from sheet '{target_sheet}'")
                logger.info(f"Columns found: {list(df.columns)}")

            except Exception as excel_error:
                # If Excel reading fails, try CSV
                logger.info(f"Not an Excel file, trying CSV: {excel_error}")
                file_obj.seek(0)
                content = file_obj.read()

                # Decode if bytes - try multiple encodings
                if isinstance(content, bytes):
                    encodings = ['utf-8-sig', 'utf-8', 'cp1252', 'latin-1', 'iso-8859-1']
                    decoded = False
                    for encoding in encodings:
                        try:
                            content = content.decode(encoding)
                            logger.info(f"Successfully decoded Morgan Stanley CSV with {encoding} encoding")
                            decoded = True
                            break
                        except UnicodeDecodeError:
                            continue

                    if not decoded:
                        logger.error("Could not decode Morgan Stanley CSV with any known encoding")
                        return pd.DataFrame()
                else:
                    if content.startswith('\ufeff'):
                        content = content[1:]

                # Split into lines
                lines = content.split('\n')

                # Find header row
                header_row_idx = None
                for idx, line in enumerate(lines):
                    if 'Trade Date' in line and 'CP Code' in line:
                        header_row_idx = idx
                        logger.info(f"Found Morgan Stanley CSV header at line {idx + 1}")
                        break

                if header_row_idx is None:
                    logger.error("Could not find header row in Morgan Stanley CSV")
                    return pd.DataFrame()

                # Read CSV starting from header row
                from io import StringIO
                csv_content = '\n'.join(lines[header_row_idx:])
                df = pd.read_csv(StringIO(csv_content))

                logger.info(f"Read Morgan Stanley CSV with {len(df)} rows")
                logger.info(f"Columns found: {list(df.columns)}")

            # Expected columns - use the first occurrence of duplicate columns
            required_cols = ['Trade Date', 'CP Code', 'Symbol', 'Expiry Date', 'Strike Price',
                           'Option Type', 'Instrument Type', 'Buy/Sell', 'Qty', 'WAP',
                           'Commission (Taxable Value)', 'Central GST*', 'State GST**',
                           'STT', 'Stamp Duty']

            # Check if required columns exist
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                logger.error(f"Missing columns in Morgan Stanley file: {missing_cols}")
                logger.error(f"Available columns: {list(df.columns)}")
                return pd.DataFrame()

            # Process each row
            parsed_rows = []

            for idx, row in df.iterrows():
                try:
                    # Get symbol
                    symbol = str(row['Symbol']).strip().upper()

                    # Get ticker for symbol
                    ticker = self._get_ticker_for_symbol(symbol)
                    if not ticker:
                        ticker = symbol  # Use symbol as ticker if not found

                    # Get instrument type (FUTSTK/OPTSTK/FUTIDX/OPTIDX)
                    instrument = str(row['Instrument Type']).strip().upper()

                    # Get option type (blank for futures, CE/PE for options)
                    option_type_val = row['Option Type']
                    if pd.isna(option_type_val) or str(option_type_val).strip() == '':
                        option_type = ''
                    else:
                        option_type = str(option_type_val).strip().upper()

                    # Determine security type
                    if not option_type or option_type in ['', 'NAN']:
                        # Futures
                        security_type = 'Futures'
                        strike = 0
                    elif option_type in ['CE', 'CALL', 'C']:
                        # Call option
                        security_type = 'Call'
                        strike = float(row['Strike Price']) if pd.notna(row['Strike Price']) else 0
                    elif option_type in ['PE', 'PUT', 'P']:
                        # Put option
                        security_type = 'Put'
                        strike = float(row['Strike Price']) if pd.notna(row['Strike Price']) else 0
                    else:
                        logger.warning(f"Unknown option type: {option_type} at row {idx}")
                        continue

                    # Parse expiry date
                    expiry = self._parse_expiry_date(row['Expiry Date'])
                    if not expiry:
                        logger.warning(f"Could not parse expiry at row {idx}")
                        continue

                    # Generate Bloomberg ticker
                    bloomberg_ticker = self._generate_bloomberg_ticker(
                        ticker, expiry, security_type, strike, instrument
                    )

                    # Normalize B/S
                    side_val = str(row['Buy/Sell']).strip().upper()
                    if side_val == 'B':
                        side = 'Buy'
                    elif side_val == 'S':
                        side = 'Sell'
                    else:
                        side = side_val

                    # Parse quantity
                    quantity = int(row['Qty'])

                    # Get CP code
                    cp_code = str(row['CP Code']).strip().upper()

                    # Get trade date
                    trade_date_val = row['Trade Date']
                    if pd.notna(trade_date_val):
                        if isinstance(trade_date_val, (datetime, pd.Timestamp)):
                            trade_date = trade_date_val.strftime('%d/%m/%Y')
                        else:
                            # Parse date format "26-Sept-25"
                            try:
                                parsed_date = datetime.strptime(str(trade_date_val).strip(), '%d-%b-%y')
                                trade_date = parsed_date.strftime('%Y-%m-%d')
                            except:
                                trade_date = str(trade_date_val).strip()
                    else:
                        trade_date = ''

                    # Pure brokerage = Commission (Taxable Value)
                    pure_brokerage = float(row['Commission (Taxable Value)']) if pd.notna(row['Commission (Taxable Value)']) else 0

                    # Total Taxes = Central GST + State GST + STT + Stamp Duty
                    central_gst = float(row['Central GST*']) if pd.notna(row['Central GST*']) else 0
                    state_gst = float(row['State GST**']) if pd.notna(row['State GST**']) else 0
                    stt = float(row['STT']) if pd.notna(row['STT']) else 0
                    stamp_duty = float(row['Stamp Duty']) if pd.notna(row['Stamp Duty']) else 0
                    total_taxes = round(central_gst + state_gst + stt + stamp_duty, 2)

                    # Build parsed row
                    parsed_row = {
                        'bloomberg_ticker': bloomberg_ticker,
                        'cp_code': cp_code,
                        'broker_code': 10542,  # Morgan Stanley code
                        'side': side,
                        'quantity': quantity,
                        'price': float(row['WAP']),
                        'pure_brokerage': pure_brokerage,
                        'total_taxes': total_taxes,
                        'trade_date': trade_date,
                        'symbol': symbol,
                        'instrument': instrument,
                        'security_type': security_type,
                        'strike': strike,
                        'expiry_date': expiry.strftime('%d/%m/%Y'),
                        'ticker': ticker
                    }

                    # Add lots if available
                    self._add_lots_if_available(parsed_row, row, df)

                    parsed_rows.append(parsed_row)

                except Exception as e:
                    logger.warning(f"Error parsing Morgan Stanley row {idx}: {e}")
                    import traceback
                    logger.warning(f"Traceback: {traceback.format_exc()}")
                    continue

            if not parsed_rows:
                logger.error(f"No Morgan Stanley rows successfully parsed from {len(df)} data rows")
                logger.error(f"Check if data rows exist below header row {header_row_idx if 'header_row_idx' in locals() else 'unknown'}")
                logger.error(f"First few rows of dataframe:\n{df.head() if not df.empty else 'Empty DataFrame'}")

            result_df = pd.DataFrame(parsed_rows)
            logger.info(f"Parsed {len(result_df)} Morgan Stanley trades with Bloomberg tickers")
            return result_df

        except Exception as e:
            logger.error(f"Error parsing Morgan Stanley file: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return pd.DataFrame()


class AntiqueParser(BrokerParserBase):
    """Parser for Antique Securities files"""

    def _parse_expiry_date(self, expiry_value) -> Optional[datetime]:
        """Parse expiry date from various formats"""
        # If already a datetime/Timestamp, return it
        if isinstance(expiry_value, (datetime, pd.Timestamp)):
            return expiry_value.to_pydatetime() if isinstance(expiry_value, pd.Timestamp) else expiry_value

        # If string, try to parse it
        try:
            expiry_str = str(expiry_value).strip()
            # Try common formats including MM/DD/YYYY (American format)
            for fmt in ['%m/%d/%Y', '%m/%d/%y', '%d/%m/%Y', '%d/%m/%y', '%d-%m-%Y', '%d-%m-%y', '%d-%b-%y', '%d-%b-%Y']:
                try:
                    return datetime.strptime(expiry_str, fmt)
                except:
                    continue
            logger.warning(f"Could not parse expiry date: {expiry_value}")
            return None
        except:
            logger.warning(f"Could not parse expiry date: {expiry_value}")
            return None

    def parse_file(self, file_obj) -> pd.DataFrame:
        """Parse Antique broker file"""
        try:
            # Try to decrypt if password-protected
            file_obj.seek(0)
            decrypted_file = decrypt_excel_file(file_obj)
            if decrypted_file:
                file_obj = decrypted_file

            # Try reading as Excel first, then CSV
            try:
                df = pd.read_excel(file_obj)
                logger.info(f"Read Antique file as Excel with {len(df)} rows")
            except:
                file_obj.seek(0)
                # Try reading CSV with different encodings and handle BOM
                try:
                    df = pd.read_csv(file_obj, encoding='utf-8-sig')
                except:
                    file_obj.seek(0)
                    try:
                        df = pd.read_csv(file_obj, encoding='cp1252')
                    except:
                        file_obj.seek(0)
                        df = pd.read_csv(file_obj, encoding='latin-1')
                logger.info(f"Read Antique file as CSV with {len(df)} rows")

            # Remove empty rows
            df = df.dropna(how='all')
            logger.info(f"After removing empty rows: {len(df)} rows")
            logger.info(f"Antique file columns: {list(df.columns)}")

            # Expected columns
            required_cols = ['CP Code', 'Scrip Code', 'Strike Price', 'Call / Put', 'Expiry',
                           'Buy / Sell', 'Qty', 'Mkt. Rate', 'Pure Brokerage AMT', 'Total Taxes', 'Trade Date']

            # Check if required columns exist
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                logger.error(f"Missing columns in Antique file: {missing_cols}")
                logger.error(f"File has these columns: {list(df.columns)}")
                return pd.DataFrame()

            # Process each row
            parsed_rows = []

            for idx, row in df.iterrows():
                try:
                    # Get scrip code (symbol)
                    scrip_code = str(row['Scrip Code']).strip().upper()

                    # Determine if index or stock
                    index_symbols = ['NIFTY', 'NSEBANK', 'MIDCAPNIFTY', 'BANKNIFTY', 'FINNIFTY']
                    is_index = scrip_code in index_symbols

                    # Get segment type if available
                    segment_type = ''
                    if 'Segment Type' in df.columns:
                        segment_type = str(row['Segment Type']).strip().upper()

                    # Get Call/Put to determine if futures or options
                    call_put = str(row['Call / Put']).strip().upper() if pd.notna(row['Call / Put']) else ''

                    # Determine instrument and security type
                    if not call_put or call_put == 'NAN' or call_put == '' or call_put == 'XX':
                        # Blank or XX = Futures
                        instrument = 'FUTIDX' if is_index else 'FUTSTK'
                        security_type = 'Futures'
                        strike = 0
                    elif call_put in ['CALL', 'C', 'CE']:
                        # Call option
                        instrument = 'OPTIDX' if is_index else 'OPTSTK'
                        security_type = 'Call'
                        strike = float(row['Strike Price']) if pd.notna(row['Strike Price']) else 0
                    elif call_put in ['PUT', 'P', 'PE']:
                        # Put option
                        instrument = 'OPTIDX' if is_index else 'OPTSTK'
                        security_type = 'Put'
                        strike = float(row['Strike Price']) if pd.notna(row['Strike Price']) else 0
                    else:
                        logger.warning(f"Unknown Call/Put value: {call_put} at row {idx}")
                        continue

                    # Get ticker for symbol
                    ticker = self._get_ticker_for_symbol(scrip_code)
                    if not ticker:
                        ticker = scrip_code  # Use scrip code as ticker if not found

                    # Parse expiry date
                    expiry = self._parse_expiry_date(row['Expiry'])
                    if not expiry:
                        logger.warning(f"Could not parse expiry at row {idx}")
                        continue

                    # Generate Bloomberg ticker
                    bloomberg_ticker = self._generate_bloomberg_ticker(
                        ticker, expiry, security_type, strike, instrument
                    )

                    # Normalize B/S
                    side = str(row['Buy / Sell']).strip()
                    side_normalized = 'Buy' if side.upper().startswith('B') else 'Sell'

                    # Parse quantity
                    quantity = int(row['Qty'])

                    # Get CP code
                    cp_code = str(row['CP Code']).strip().upper()

                    # Get trade date - handle Timestamp objects
                    trade_date_val = row['Trade Date']
                    if pd.notna(trade_date_val):
                        if isinstance(trade_date_val, (datetime, pd.Timestamp)):
                            trade_date = trade_date_val.strftime('%d/%m/%Y')
                        else:
                            trade_date = str(trade_date_val).strip()
                    else:
                        trade_date = ''

                    # Build parsed row
                    parsed_row = {
                        'bloomberg_ticker': bloomberg_ticker,
                        'cp_code': cp_code,
                        'broker_code': self._get_broker_code_from_row(row, df, 12987),  # Read from file, default Antique
                        'side': side_normalized,
                        'quantity': quantity,
                        'price': float(row['Mkt. Rate']),
                        'pure_brokerage': float(row['Pure Brokerage AMT']),
                        'total_taxes': round(float(row['Total Taxes']), 2),
                        'trade_date': trade_date,
                        'symbol': scrip_code,
                        'instrument': instrument,
                        'security_type': security_type,
                        'strike': strike,
                        'expiry_date': expiry.strftime('%d/%m/%Y'),
                        'ticker': ticker
                    }

                    # Add lots if available
                    self._add_lots_if_available(parsed_row, row, df)

                    parsed_rows.append(parsed_row)

                except Exception as e:
                    logger.warning(f"Error parsing Antique row {idx}: {e}")
                    continue

            result_df = pd.DataFrame(parsed_rows)
            logger.info(f"Parsed {len(result_df)} Antique trades with Bloomberg tickers")
            return result_df

        except Exception as e:
            logger.error(f"Error parsing Antique file: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return pd.DataFrame()


def get_parser_for_broker(broker_id: str, futures_mapping_file: str = "futures mapping.csv"):
    """Get parser instance for a broker"""
    if broker_id == 'ICICI':
        return IciciParser(futures_mapping_file)
    elif broker_id == 'KOTAK':
        return KotakParser(futures_mapping_file)
    elif broker_id == 'IIFL':
        return IIFLParser(futures_mapping_file)
    elif broker_id == 'AXIS':
        return AxisParser(futures_mapping_file)
    elif broker_id == 'EQUIRUS':
        return EquirusParser(futures_mapping_file)
    elif broker_id == 'EDELWEISS':
        return EdelweissParser(futures_mapping_file)
    elif broker_id == 'NUVAMA':
        return EdelweissParser(futures_mapping_file)  # Same format as Edelweiss
    elif broker_id == 'MORGAN':
        return MorganStanleyParser(futures_mapping_file)
    elif broker_id == 'ANTIQUE':
        return AntiqueParser(futures_mapping_file)
    else:
        logger.error(f"Unknown broker: {broker_id}")
        return None
