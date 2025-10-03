"""
Simplified Price Manager
- Loads symbols from default_stocks.csv
- Fetches prices ONCE at startup
- Allows manual price override
- Used everywhere in the system
"""

import pandas as pd
import yfinance as yf
from typing import Dict, Optional
import streamlit as st
import logging

logger = logging.getLogger(__name__)

class SimplePriceManager:
    """Simplified price management - fetch once, use everywhere"""

    def __init__(self):
        self.master_prices = {}  # Symbol -> Price mapping
        self.symbol_to_bloomberg = {}  # Symbol -> Bloomberg mapping
        self.bloomberg_to_symbol = {}  # Bloomberg -> Symbol mapping
        self.missing_symbols = set()
        self.price_source = "Not initialized"

    def load_default_stocks(self, file_path: str = "default_stocks.csv"):
        """Load symbols from default stocks file"""
        try:
            df = pd.read_csv(file_path)

            # Build mappings
            for _, row in df.iterrows():
                symbol = str(row['Symbol']).strip()
                bloomberg = str(row['Bloomberg Code']).strip()

                self.symbol_to_bloomberg[symbol] = bloomberg
                self.bloomberg_to_symbol[bloomberg] = symbol

            # Add index mappings manually
            self.symbol_to_bloomberg['NIFTY'] = 'NZ'
            self.bloomberg_to_symbol['NZ'] = 'NIFTY'

            self.symbol_to_bloomberg['BANKNIFTY'] = 'AF'
            self.bloomberg_to_symbol['AF'] = 'BANKNIFTY'

            self.symbol_to_bloomberg['MIDCPNIFTY'] = 'RNS'
            self.symbol_to_bloomberg['NMIDSELP'] = 'RNS'
            self.symbol_to_bloomberg['NMIDSELD'] = 'RNS'
            self.bloomberg_to_symbol['RNS'] = 'MIDCPNIFTY'

            logger.info(f"Loaded {len(self.symbol_to_bloomberg)} stock mappings")
            return True

        except Exception as e:
            logger.error(f"Error loading default stocks: {e}")
            return False

    def fetch_all_prices_yahoo(self, progress_callback=None):
        """Fetch all prices from Yahoo Finance in one go"""
        self.master_prices.clear()
        self.missing_symbols.clear()

        symbols = list(self.symbol_to_bloomberg.keys())
        total = len(symbols)
        fetched = 0
        failed = 0

        logger.info(f"Fetching prices for {total} symbols from Yahoo Finance...")

        for i, symbol in enumerate(symbols):
            try:
                # Add .NS suffix for NSE stocks
                yahoo_symbol = f"{symbol}.NS"
                ticker = yf.Ticker(yahoo_symbol)
                hist = ticker.history(period="1d")

                if not hist.empty:
                    price = float(hist['Close'].iloc[-1])
                    self.master_prices[symbol] = price

                    # Also map by Bloomberg code
                    bloomberg = self.symbol_to_bloomberg.get(symbol)
                    if bloomberg:
                        self.master_prices[bloomberg] = price

                    fetched += 1
                else:
                    self.missing_symbols.add(symbol)
                    failed += 1

            except Exception as e:
                self.missing_symbols.add(symbol)
                failed += 1
                logger.debug(f"Failed to fetch {symbol}: {e}")

            # Update progress
            if progress_callback:
                progress_callback(i + 1, total)

        # Add index prices manually
        index_prices = {
            'NIFTY': None,
            'BANKNIFTY': None,
            'FINNIFTY': None,
            'MIDCPNIFTY': None
        }

        # Try to fetch index prices
        try:
            nifty = yf.Ticker("^NSEI")
            hist = nifty.history(period="1d")
            if not hist.empty:
                index_prices['NIFTY'] = float(hist['Close'].iloc[-1])
                self.master_prices['NIFTY'] = index_prices['NIFTY']
                self.master_prices['NZ'] = index_prices['NIFTY']  # Bloomberg code
        except:
            pass

        try:
            banknifty = yf.Ticker("^NSEBANK")
            hist = banknifty.history(period="1d")
            if not hist.empty:
                index_prices['BANKNIFTY'] = float(hist['Close'].iloc[-1])
                self.master_prices['BANKNIFTY'] = index_prices['BANKNIFTY']
                self.master_prices['AF'] = index_prices['BANKNIFTY']  # Bloomberg code
        except:
            pass

        try:
            midcpnifty = yf.Ticker("NIFTY_MID_SELECT.NS")
            hist = midcpnifty.history(period="1d")
            if not hist.empty:
                index_prices['MIDCPNIFTY'] = float(hist['Close'].iloc[-1])
                self.master_prices['MIDCPNIFTY'] = index_prices['MIDCPNIFTY']
                self.master_prices['NMIDSELP'] = index_prices['MIDCPNIFTY']  # Symbol
                self.master_prices['NMIDSELD'] = index_prices['MIDCPNIFTY']  # Symbol
                self.master_prices['RNS'] = index_prices['MIDCPNIFTY']  # Bloomberg futures code
        except:
            pass

        self.price_source = f"Yahoo Finance ({fetched} fetched, {failed} failed)"
        logger.info(f"Price fetch complete: {fetched} success, {failed} failed")

        return self.master_prices

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

        # Update missing symbols
        self.missing_symbols = set()
        for symbol in self.symbol_to_bloomberg.keys():
            if symbol not in self.master_prices:
                self.missing_symbols.add(symbol)

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

        # Try without common suffixes
        for suffix in [' IS EQUITY', ' IS Equity', ' INDEX', ' Index', '.NS', '.BO']:
            if ticker_clean.endswith(suffix):
                base = ticker_clean[:-len(suffix)]
                if base in self.master_prices:
                    return self.master_prices[base]
                if base.upper() in self.master_prices:
                    return self.master_prices[base.upper()]

        # Try mapping with original case
        if ticker_clean in self.symbol_to_bloomberg:
            bloomberg = self.symbol_to_bloomberg[ticker_clean]
            if bloomberg in self.master_prices:
                return self.master_prices[bloomberg]

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
            data.append({
                'Symbol': symbol,
                'Bloomberg Code': bloomberg,
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