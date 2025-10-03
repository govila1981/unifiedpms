"""
Simplified Price Manager
- Loads symbols, yahoo codes, and override prices from default_stocks.csv
- Uses Yahoo Code column directly (no construction/guessing)
- Supports override price fallback
- Persists updates based on environment (desktop/Railway/cloud)
"""

import pandas as pd
import yfinance as yf
from typing import Dict, Optional
import streamlit as st
import logging
import os
from pathlib import Path
import json

logger = logging.getLogger(__name__)

class SimplePriceManager:
    """Simplified price management - fetch from Yahoo using provided codes, fallback to override prices"""

    def __init__(self):
        self.master_prices = {}  # Symbol/Bloomberg/Yahoo -> Price mapping
        self.symbol_to_bloomberg = {}  # Symbol -> Bloomberg mapping
        self.bloomberg_to_symbol = {}  # Bloomberg -> Symbol mapping
        self.yahoo_to_symbol = {}  # Yahoo Code -> Symbol mapping
        self.symbol_to_yahoo = {}  # Symbol -> Yahoo Code mapping
        self.override_prices = {}  # Symbol -> Override price (from CSV)
        self.missing_symbols = set()
        self.price_source = "Not initialized"
        self.stocks_df = None  # Store original DataFrame
        self.csv_path = None  # Store CSV path for updates

    def _detect_environment(self) -> str:
        """Detect if running on desktop, Railway, or Streamlit Cloud"""
        if os.environ.get('RAILWAY_ENVIRONMENT'):
            return 'railway'
        elif os.environ.get('STREAMLIT_SHARING_MODE') or os.environ.get('STREAMLIT_RUNTIME_ENV'):
            return 'streamlit_cloud'
        else:
            return 'desktop'

    def load_default_stocks(self, file_path: str = "default_stocks.csv"):
        """Load symbols, Yahoo codes, and override prices from CSV"""
        try:
            self.csv_path = file_path
            df = pd.read_csv(file_path)
            self.stocks_df = df.copy()

            logger.info(f"Loading stocks from {file_path}")
            logger.info(f"Columns: {list(df.columns)}")

            # Expected columns: Company Name, Industry, Symbol, Series, ISIN Code, Bloomberg Code, Yahoo Code, override price
            loaded_count = 0

            for idx, row in df.iterrows():
                try:
                    symbol = str(row['Symbol']).strip()
                    bloomberg = str(row['Bloomberg Code']).strip()
                    yahoo_code = str(row['Yahoo Code']).strip() if pd.notna(row.get('Yahoo Code')) else ''
                    override_price = float(row['override price']) if pd.notna(row.get('override price')) and row.get('override price') != '' else None

                    if not symbol or symbol.upper() == 'NAN':
                        continue

                    # Build mappings
                    self.symbol_to_bloomberg[symbol] = bloomberg
                    self.bloomberg_to_symbol[bloomberg] = symbol

                    if yahoo_code and yahoo_code.upper() != 'NAN':
                        self.symbol_to_yahoo[symbol] = yahoo_code
                        self.yahoo_to_symbol[yahoo_code] = symbol

                    # Store override price
                    if override_price and override_price > 0:
                        self.override_prices[symbol] = override_price
                        # Also map by Bloomberg and Yahoo code
                        if bloomberg:
                            self.override_prices[bloomberg] = override_price
                        if yahoo_code:
                            self.override_prices[yahoo_code] = override_price

                    loaded_count += 1

                except Exception as e:
                    logger.debug(f"Error loading row {idx}: {e}")
                    continue

            # Count actual stocks with override prices (not dictionary entries)
            stocks_with_prices = sum(1 for symbol in self.symbol_to_bloomberg.keys() if symbol in self.override_prices)

            logger.info(f"Loaded {loaded_count} stocks with {len(self.symbol_to_yahoo)} Yahoo codes")
            logger.info(f"Loaded {stocks_with_prices} stocks with override prices ({len(self.override_prices)} total mappings)")

            # Initialize master_prices with override prices
            self.master_prices = self.override_prices.copy()
            self.price_source = f"Override prices ({stocks_with_prices} stocks loaded)"

            return True

        except Exception as e:
            logger.error(f"Error loading default stocks: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False

    def fetch_all_prices_yahoo(self, progress_callback=None):
        """Fetch prices using Yahoo Code column - no construction/guessing"""
        fetched_prices = {}
        failed_symbols = []

        # Get symbols that have Yahoo codes
        symbols_with_yahoo = [(symbol, yahoo_code) for symbol, yahoo_code in self.symbol_to_yahoo.items()]
        total = len(symbols_with_yahoo)

        logger.info(f"Fetching prices for {total} symbols using Yahoo Finance...")

        for i, (symbol, yahoo_code) in enumerate(symbols_with_yahoo):
            try:
                # Use Yahoo Code directly - NO modifications
                ticker = yf.Ticker(yahoo_code)
                hist = ticker.history(period="1d")

                if not hist.empty:
                    price = float(hist['Close'].iloc[-1])

                    # Store price with multiple keys for lookup
                    fetched_prices[symbol] = price

                    bloomberg = self.symbol_to_bloomberg.get(symbol)
                    if bloomberg:
                        fetched_prices[bloomberg] = price

                    fetched_prices[yahoo_code] = price

                    logger.info(f"✓ {symbol} ({yahoo_code}): {price:.2f}")

                else:
                    failed_symbols.append(symbol)
                    logger.warning(f"✗ {symbol} ({yahoo_code}): No data returned")

            except Exception as e:
                failed_symbols.append(symbol)
                logger.warning(f"✗ {symbol} ({yahoo_code}): {str(e)}")

            # Update progress
            if progress_callback:
                progress_callback(i + 1, total)

        # Update master prices with fetched data
        self.master_prices.update(fetched_prices)
        self.missing_symbols = set(failed_symbols)

        fetched_count = len(symbols_with_yahoo) - len(failed_symbols)
        self.price_source = f"Yahoo Finance ({fetched_count}/{total} fetched)"

        logger.info(f"✓ Price fetch complete: {fetched_count} success, {len(failed_symbols)} failed")

        # Persist prices
        self._persist_prices(fetched_prices)

        return self.master_prices

    def _persist_prices(self, fetched_prices: Dict[str, float]):
        """Save fetched prices based on environment"""
        env = self._detect_environment()
        logger.info(f"Persisting prices (environment: {env})")

        if env == 'desktop':
            # Update CSV file directly
            self._update_csv_file(fetched_prices)

        elif env == 'railway':
            # Save to /tmp or mounted volume
            self._save_to_json(fetched_prices)
            # Also try to update CSV if writable
            try:
                self._update_csv_file(fetched_prices)
            except:
                logger.info("CSV not writable on Railway, using JSON cache only")

        # For streamlit_cloud, prices stay in memory only (download button provided in UI)

    def _update_csv_file(self, fetched_prices: Dict[str, float]):
        """Update override price column in CSV file"""
        if not self.csv_path or self.stocks_df is None:
            return

        try:
            # Update override price column
            updated_count = 0
            for idx, row in self.stocks_df.iterrows():
                symbol = str(row['Symbol']).strip()
                if symbol in fetched_prices:
                    self.stocks_df.at[idx, 'override price'] = fetched_prices[symbol]
                    updated_count += 1

            # Save back to CSV
            self.stocks_df.to_csv(self.csv_path, index=False)
            logger.info(f"✓ Updated {updated_count} prices in {self.csv_path}")

        except Exception as e:
            logger.error(f"Error updating CSV: {e}")

    def _save_to_json(self, fetched_prices: Dict[str, float]):
        """Save prices to JSON (Railway /tmp or volume)"""
        try:
            # Check if we have a mounted volume
            volume_path = os.environ.get('RAILWAY_VOLUME_MOUNT_PATH')
            if volume_path:
                json_path = Path(volume_path) / 'price_overrides.json'
            else:
                json_path = Path('/tmp') / 'price_overrides.json'

            with open(json_path, 'w') as f:
                json.dump(fetched_prices, f, indent=2)

            logger.info(f"✓ Saved prices to {json_path}")

        except Exception as e:
            logger.error(f"Error saving JSON: {e}")

    def get_updated_csv_dataframe(self) -> Optional[pd.DataFrame]:
        """Get DataFrame with updated prices for download"""
        if self.stocks_df is None:
            return None

        df = self.stocks_df.copy()

        # Update override price column with current prices
        for idx, row in df.iterrows():
            symbol = str(row['Symbol']).strip()
            if symbol in self.master_prices:
                df.at[idx, 'override price'] = self.master_prices[symbol]

        return df

    def load_manual_prices(self, price_df: pd.DataFrame):
        """Load prices from uploaded file (Symbol/Ticker, Price format)"""
        loaded = 0

        # Find price column
        price_col = None
        ticker_col = None

        for col in price_df.columns:
            if 'price' in col.lower():
                price_col = col
            if any(x in col.lower() for x in ['symbol', 'ticker', 'bloomberg']):
                ticker_col = col

        if not price_col or not ticker_col:
            logger.error("Could not find Symbol/Ticker and Price columns")
            return False

        # Load prices
        for _, row in price_df.iterrows():
            try:
                ticker = str(row[ticker_col]).strip()
                price = float(row[price_col])

                if ticker and price > 0:
                    self.master_prices[ticker] = price

                    # Map to symbol if it's a Bloomberg code
                    if ticker in self.bloomberg_to_symbol:
                        symbol = self.bloomberg_to_symbol[ticker]
                        self.master_prices[symbol] = price

                    # Map to Bloomberg if it's a symbol
                    if ticker in self.symbol_to_bloomberg:
                        bloomberg = self.symbol_to_bloomberg[ticker]
                        self.master_prices[bloomberg] = price

                    loaded += 1

            except:
                continue

        self.price_source = f"Manual upload ({loaded} prices)"
        logger.info(f"Loaded {loaded} manual prices")

        return True

    def get_price(self, ticker: str) -> Optional[float]:
        """Get price for any ticker/symbol/bloomberg code"""
        if not ticker:
            return None

        ticker_clean = str(ticker).strip()

        # Try original case first
        if ticker_clean in self.master_prices:
            return self.master_prices[ticker_clean]

        # Try uppercase
        ticker_upper = ticker_clean.upper()
        if ticker_upper in self.master_prices:
            return self.master_prices[ticker_upper]

        # Try without Bloomberg suffixes
        for suffix in [' IS EQUITY', ' IS Equity', ' INDEX', ' Index', ' EQUITY', ' Equity']:
            if ticker_clean.endswith(suffix):
                base = ticker_clean[:-len(suffix)].strip()
                if base in self.master_prices:
                    return self.master_prices[base]
                if base.upper() in self.master_prices:
                    return self.master_prices[base.upper()]

        # Try mapping symbol -> bloomberg
        if ticker_clean in self.symbol_to_bloomberg:
            bloomberg = self.symbol_to_bloomberg[ticker_clean]
            if bloomberg in self.master_prices:
                return self.master_prices[bloomberg]

        # Try mapping bloomberg -> symbol
        if ticker_clean in self.bloomberg_to_symbol:
            symbol = self.bloomberg_to_symbol[ticker_clean]
            if symbol in self.master_prices:
                return self.master_prices[symbol]

        # Try mapping with uppercase
        if ticker_upper in self.symbol_to_bloomberg:
            bloomberg = self.symbol_to_bloomberg[ticker_upper]
            if bloomberg in self.master_prices:
                return self.master_prices[bloomberg]

        if ticker_upper in self.bloomberg_to_symbol:
            symbol = self.bloomberg_to_symbol[ticker_upper]
            if symbol in self.master_prices:
                return self.master_prices[symbol]

        return None

    def get_missing_symbols_report(self) -> pd.DataFrame:
        """Get report of symbols without prices"""
        if not self.missing_symbols:
            return pd.DataFrame()

        data = []
        for symbol in sorted(self.missing_symbols):
            bloomberg = self.symbol_to_bloomberg.get(symbol, "N/A")
            yahoo_code = self.symbol_to_yahoo.get(symbol, "N/A")
            data.append({
                'Symbol': symbol,
                'Bloomberg Code': bloomberg,
                'Yahoo Code': yahoo_code,
                'Status': 'No price found'
            })

        return pd.DataFrame(data)

    def get_price_summary(self) -> pd.DataFrame:
        """Get summary of all prices"""
        data = []

        # Get unique symbols (avoid duplicates)
        processed = set()

        for symbol in sorted(self.symbol_to_bloomberg.keys()):
            if symbol not in processed and symbol in self.master_prices:
                data.append({
                    'Symbol': symbol,
                    'Bloomberg Code': self.symbol_to_bloomberg.get(symbol, "N/A"),
                    'Yahoo Code': self.symbol_to_yahoo.get(symbol, "N/A"),
                    'Price': self.master_prices[symbol],
                    'Source': self.price_source
                })
                processed.add(symbol)

        return pd.DataFrame(data)

# Global instance
_price_manager_instance = None

def get_price_manager() -> SimplePriceManager:
    """Get or create the global price manager instance"""
    global _price_manager_instance
    if _price_manager_instance is None:
        _price_manager_instance = SimplePriceManager()
    return _price_manager_instance
