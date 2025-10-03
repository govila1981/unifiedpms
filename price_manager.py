"""
Centralized Price Manager Module
Manages all price fetching and caching with support for:
- Yahoo Finance API fetching
- Manual price file upload
- Symbol to Bloomberg ticker mapping
- Persistent price cache across modules
"""

import pandas as pd
import logging
from typing import Dict, Optional, Union
from datetime import datetime
import json
from pathlib import Path

try:
    import yfinance as yf
    YFINANCE_AVAILABLE = True
except ImportError:
    YFINANCE_AVAILABLE = False
    logging.warning("yfinance not installed. Install with: pip install yfinance")

logger = logging.getLogger(__name__)


class PriceManager:
    """Centralized price management with caching and multiple data sources"""

    _instance = None
    _initialized = False

    def __new__(cls):
        """Singleton pattern to ensure single instance across modules"""
        if cls._instance is None:
            cls._instance = super(PriceManager, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        """Initialize price manager with empty cache"""
        if not self._initialized:
            self.price_cache = {}
            self.manual_prices = {}  # Prices from uploaded file
            self.symbol_to_bloomberg = {}  # Symbol to Bloomberg mapping
            self.bloomberg_to_symbol = {}  # Reverse mapping
            self.use_manual_prices = False
            self.last_update = None
            self.price_sources = {}  # Track where each price came from
            self._initialized = True
            logger.info("PriceManager initialized")

    def load_symbol_mapping(self, mapping_file: str = "futures mapping.csv"):
        """Load symbol to Bloomberg ticker mapping from CSV"""
        try:
            df = pd.read_csv(mapping_file, encoding='utf-8', skiprows=3)

            # Build mappings from futures mapping file
            for idx, row in df.iterrows():
                symbol = str(row.get('Symbol', '')).strip()
                ticker = str(row.get('Ticker', '')).strip()

                if symbol and ticker and symbol != 'nan':
                    self.symbol_to_bloomberg[symbol.upper()] = ticker.upper()
                    self.bloomberg_to_symbol[ticker.upper()] = symbol.upper()

                    # Also map ticker to itself
                    self.symbol_to_bloomberg[ticker.upper()] = ticker.upper()

            logger.info(f"Loaded {len(self.symbol_to_bloomberg)} symbol mappings")

        except Exception as e:
            logger.error(f"Error loading symbol mapping: {str(e)}")

    def load_manual_prices(self, price_data: Union[pd.DataFrame, str, Path]):
        """
        Load manual prices from DataFrame or file
        Expected format: columns should include 'Ticker' or 'Symbol' and 'Price'
        """
        try:
            if isinstance(price_data, (str, Path)):
                if str(price_data).endswith('.csv'):
                    df = pd.read_csv(price_data)
                else:
                    df = pd.read_excel(price_data)
            else:
                df = price_data.copy()

            # Clear existing manual prices
            self.manual_prices.clear()

            # Find ticker/symbol column
            ticker_col = None
            for col in df.columns:
                if col.upper() in ['TICKER', 'SYMBOL', 'BLOOMBERG_TICKER', 'BLOOMBERG TICKER']:
                    ticker_col = col
                    break

            if not ticker_col:
                logger.error("No ticker/symbol column found in price file")
                return False

            # Find price column
            price_col = None
            for col in df.columns:
                if 'PRICE' in col.upper():
                    price_col = col
                    break

            if not price_col:
                logger.error("No price column found in price file")
                return False

            # Load prices
            loaded_count = 0
            for idx, row in df.iterrows():
                ticker = str(row[ticker_col]).strip().upper()
                try:
                    price = float(row[price_col])
                    if ticker and ticker != 'NAN' and price > 0:
                        self.manual_prices[ticker] = price
                        loaded_count += 1

                        # Also add mapped tickers
                        if ticker in self.symbol_to_bloomberg:
                            self.manual_prices[self.symbol_to_bloomberg[ticker]] = price
                        if ticker in self.bloomberg_to_symbol:
                            self.manual_prices[self.bloomberg_to_symbol[ticker]] = price

                except (ValueError, TypeError):
                    continue

            self.use_manual_prices = True
            self.last_update = datetime.now()
            logger.info(f"Loaded {loaded_count} manual prices")
            return True

        except Exception as e:
            logger.error(f"Error loading manual prices: {str(e)}")
            return False

    def get_price(self, ticker: str, force_refresh: bool = False) -> Optional[float]:
        """
        Get price for a ticker with priority:
        1. Manual prices (if enabled)
        2. Cache (if not forcing refresh)
        3. Yahoo Finance (if available)
        """
        ticker_clean = str(ticker).strip().upper()

        # Check manual prices first if enabled
        if self.use_manual_prices:
            # Direct lookup
            if ticker_clean in self.manual_prices:
                self.price_sources[ticker_clean] = f"Manual (direct): {ticker_clean}"
                return self.manual_prices[ticker_clean]

            # Try symbol mapping
            if ticker_clean in self.symbol_to_bloomberg:
                mapped = self.symbol_to_bloomberg[ticker_clean]
                if mapped in self.manual_prices:
                    self.price_sources[ticker_clean] = f"Manual (mapped): {ticker_clean} -> {mapped}"
                    return self.manual_prices[mapped]

            # Try reverse mapping
            if ticker_clean in self.bloomberg_to_symbol:
                mapped = self.bloomberg_to_symbol[ticker_clean]
                if mapped in self.manual_prices:
                    self.price_sources[ticker_clean] = f"Manual (reverse mapped): {ticker_clean} -> {mapped}"
                    return self.manual_prices[mapped]

            # Try partial matching for underlying symbols
            for symbol, price in self.manual_prices.items():
                if ticker_clean in symbol or symbol in ticker_clean:
                    self.price_sources[ticker_clean] = f"Manual (partial match): {ticker_clean} ~ {symbol}"
                    return price

        # Check cache if not forcing refresh
        if not force_refresh and ticker_clean in self.price_cache:
            if ticker_clean not in self.price_sources:
                self.price_sources[ticker_clean] = f"Cached"
            return self.price_cache[ticker_clean]

        # Try Yahoo Finance if available and not using manual prices exclusively
        if YFINANCE_AVAILABLE and not self.use_manual_prices:
            price = self._fetch_from_yahoo(ticker_clean)
            if price:
                self.price_cache[ticker_clean] = price
                self.price_sources[ticker_clean] = f"Yahoo Finance: {ticker_clean}"
                # Also cache common variations
                if ticker_clean.endswith('.NS'):
                    base_ticker = ticker_clean[:-3]
                    self.price_cache[base_ticker] = price
                    self.price_sources[base_ticker] = f"Yahoo Finance: {ticker_clean} (base: {base_ticker})"
                elif not ticker_clean.endswith('.NS') and not ticker_clean.endswith('.BO'):
                    # Try with .NS suffix
                    ns_ticker = ticker_clean + '.NS'
                    if ns_ticker in self.price_cache:
                        self.price_cache[ticker_clean] = self.price_cache[ns_ticker]
                        self.price_sources[ticker_clean] = f"Yahoo Finance (cached): {ns_ticker}"
                        return self.price_cache[ns_ticker]
                return price

        self.price_sources[ticker_clean] = "Not found"
        return None

    def _fetch_from_yahoo(self, ticker: str) -> Optional[float]:
        """Fetch price from Yahoo Finance"""
        try:
            # Clean the ticker
            ticker_clean = ticker.strip().upper()

            # Remove Bloomberg suffixes
            for suffix in [' EQUITY', ' INDEX', '-EQ', '_EQ', '.EQ', '-BE', '.BE', '-BZ', '.BZ']:
                if ticker_clean.endswith(suffix):
                    ticker_clean = ticker_clean[:-len(suffix)]
                    break

            # Special handling for indices
            index_mapping = {
                'NIFTY': '^NSEI',
                'NIFTY50': '^NSEI',
                'NZ': '^NSEI',
                'BANKNIFTY': '^NSEBANK',
                'AF': '^NSEBANK',
                'NSEBANK': '^NSEBANK',
                'FINNIFTY': '^CNXFIN',
                'MIDCPNIFTY': '^NSEMDCP50',
                'SENSEX': '^BSESN',
            }

            yahoo_ticker = index_mapping.get(ticker_clean)

            if not yahoo_ticker:
                # Try with .NS suffix for NSE stocks
                yahoo_ticker = f"{ticker_clean}.NS"

            # Fetch from Yahoo
            stock = yf.Ticker(yahoo_ticker)
            data = stock.history(period='1d')

            if not data.empty:
                price = data['Close'].iloc[-1]
                logger.info(f"Fetched {ticker} -> {yahoo_ticker}: {price}")
                return float(price)

            # Try without .NS suffix
            if yahoo_ticker.endswith('.NS'):
                stock = yf.Ticker(ticker_clean)
                data = stock.history(period='1d')
                if not data.empty:
                    price = data['Close'].iloc[-1]
                    return float(price)

        except Exception as e:
            logger.debug(f"Yahoo fetch failed for {ticker}: {str(e)}")

        return None

    def get_all_prices(self) -> Dict[str, float]:
        """Get all cached and manual prices"""
        all_prices = {}
        all_prices.update(self.price_cache)
        if self.use_manual_prices:
            all_prices.update(self.manual_prices)
        return all_prices

    def save_cache(self, filename: str = "price_cache.json"):
        """Save price cache to file"""
        try:
            cache_data = {
                'price_cache': self.price_cache,
                'manual_prices': self.manual_prices,
                'use_manual_prices': self.use_manual_prices,
                'last_update': self.last_update.isoformat() if self.last_update else None
            }
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, indent=2, ensure_ascii=False)
            logger.info(f"Saved price cache to {filename}")
        except Exception as e:
            logger.error(f"Error saving cache: {str(e)}")

    def load_cache(self, filename: str = "price_cache.json"):
        """Load price cache from file"""
        try:
            if Path(filename).exists():
                with open(filename, 'r', encoding='utf-8') as f:
                    cache_data = json.load(f)
                self.price_cache = cache_data.get('price_cache', {})
                self.manual_prices = cache_data.get('manual_prices', {})
                self.use_manual_prices = cache_data.get('use_manual_prices', False)
                if cache_data.get('last_update'):
                    self.last_update = datetime.fromisoformat(cache_data['last_update'])
                logger.info(f"Loaded price cache from {filename}")
        except Exception as e:
            logger.error(f"Error loading cache: {str(e)}")

    def clear_cache(self):
        """Clear all price caches"""
        self.price_cache.clear()
        self.manual_prices.clear()
        self.use_manual_prices = False
        self.last_update = None
        logger.info("Price cache cleared")

    def set_manual_mode(self, enabled: bool):
        """Enable/disable manual price mode"""
        self.use_manual_prices = enabled
        logger.info(f"Manual price mode: {'enabled' if enabled else 'disabled'}")

    def get_price_report(self) -> Dict[str, Dict[str, any]]:
        """Get a report of all fetched prices and their sources"""
        report = {}

        # Get all unique underlyings
        all_tickers = set(self.price_cache.keys()) | set(self.manual_prices.keys())

        for ticker in sorted(all_tickers):
            price = self.get_price(ticker)
            source = self.price_sources.get(ticker, "Unknown")

            report[ticker] = {
                'price': price,
                'source': source,
                'is_manual': ticker in self.manual_prices,
                'is_cached': ticker in self.price_cache
            }

        return report

    def get_underlying_price_summary(self) -> pd.DataFrame:
        """Get a summary DataFrame of underlying prices"""
        data = []
        for ticker, info in self.get_price_report().items():
            data.append({
                'Underlying': ticker,
                'Price': info['price'],
                'Source': info['source'],
                'Type': 'Manual' if info['is_manual'] else 'Market Data'
            })

        if data:
            df = pd.DataFrame(data)
            return df.sort_values('Underlying')
        else:
            return pd.DataFrame(columns=['Underlying', 'Price', 'Source', 'Type'])