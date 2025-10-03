"""
Position Manager Module - WITH CENTRALIZED PRICE MANAGEMENT
Uses centralized PriceManager for all price fetching
Ensures all expiry dates are formatted as simple dates (YYYY-MM-DD)
"""

from dataclasses import dataclass, field
from typing import Dict, Optional, List
import pandas as pd
import logging
from datetime import datetime
from copy import deepcopy

# Import simple price manager (centralized source)
try:
    from simple_price_manager import get_price_manager
    SimplePriceManager = True
except ImportError:
    logging.warning("SimplePriceManager not found - using fallback")
    SimplePriceManager = None

# Keep yfinance import for backward compatibility
try:
    import yfinance as yf
    YFINANCE_AVAILABLE = True
except ImportError:
    YFINANCE_AVAILABLE = False
    logging.warning("yfinance not installed. Install with: pip install yfinance")

logger = logging.getLogger(__name__)


class PriceFetcher:
    """Fetch prices using centralized SimplePriceManager or fallback to Yahoo Finance"""

    def __init__(self):
        # Use centralized SimplePriceManager if available
        if SimplePriceManager:
            self.price_manager = get_price_manager()
            # Load default stocks mapping
            try:
                self.price_manager.load_default_stocks()
            except:
                pass
        else:
            self.price_manager = None
        self.price_cache = {}

    def fetch_price_for_symbol(self, symbol: str) -> Optional[float]:
        """
        Fetch current price for a single symbol
        Returns None if price cannot be fetched
        """
        # Use centralized PriceManager if available
        if self.price_manager:
            return self.price_manager.get_price(symbol)

        # Fallback to local cache
        if symbol in self.price_cache:
            return self.price_cache[symbol]

        if not YFINANCE_AVAILABLE:
            logger.warning(f"yfinance not available for {symbol}")
            self.price_cache[symbol] = None
            return None
        
        # Clean the symbol - remove any extra spaces or special characters
        symbol_clean = str(symbol).strip().upper()
        
        # Remove common suffixes that might be in the symbol
        for suffix in ['-EQ', '_EQ', '.EQ', '-BE', '.BE', '-BZ', '.BZ']:
            if symbol_clean.endswith(suffix):
                symbol_clean = symbol_clean[:-len(suffix)]
                break
        
        price = None
        
        # Special handling for indices
        index_mapping = {
            'NIFTY': '^NSEI',
            'NIFTY50': '^NSEI',
            'NF': '^NSEI',
            'NZ': '^NSEI',  # Bloomberg ticker for NIFTY
            'BANKNIFTY': '^NSEBANK',
            'BNF': '^NSEBANK',
            'BANKN': '^NSEBANK',
            'NSEBANK': '^NSEBANK',
            'AF1': '^NSEBANK',  # Bloomberg ticker for BANKNIFTY
            'AF': '^NSEBANK',
            'FINNIFTY': '^CNXFIN',
            'FNF': '^CNXFIN',
            'FINNNIFTY': '^CNXFIN',
            'MIDCPNIFTY': '^NSEMDCP50',
            'MIDCAP': '^NSEMDCP50',
            'MCN': '^NSEMDCP50',
            'NMIDSELP': '^NSEMDCP50',  # Bloomberg ticker
            'RNS': '^NSEMDCP50',  # Bloomberg ticker for MIDCPNIFTY
        }
        
        # Check if it's an index
        yahoo_symbol = None
        if symbol_clean in index_mapping:
            yahoo_symbol = index_mapping[symbol_clean]
            logger.info(f"Mapped {symbol} to index {yahoo_symbol}")
            fetched_price = self._fetch_from_yahoo(yahoo_symbol)
            if fetched_price and fetched_price > 0:
                price = fetched_price
            
        if price is None:
            # Try as regular stock - NSE first, then BSE
            for suffix in ['.NS', '.BO', '']:
                yahoo_symbol = f"{symbol_clean}{suffix}"
                fetched_price = self._fetch_from_yahoo(yahoo_symbol)
                if fetched_price and fetched_price > 0:
                    logger.info(f"Found price for {symbol} using {yahoo_symbol}: {fetched_price}")
                    price = fetched_price
                    break
        
        # If still no price, log and return None
        if price is None:
            logger.warning(f"Could not fetch price for {symbol} - will show N/A")
        
        # Cache and return
        self.price_cache[symbol] = price
        return price
    
    def _fetch_from_yahoo(self, yahoo_symbol: str) -> Optional[float]:
        """Internal method to fetch from Yahoo Finance"""
        try:
            ticker = yf.Ticker(yahoo_symbol)
            
            # Try history first (more reliable)
            try:
                hist = ticker.history(period="5d")  # 5 days in case market was closed
                if not hist.empty and 'Close' in hist:
                    price = float(hist['Close'].iloc[-1])
                    if price > 0:
                        return round(price, 2)
            except:
                pass
            
            # Fallback to info
            try:
                info = ticker.info
                # Try multiple price fields
                for field in ['currentPrice', 'regularMarketPrice', 'previousClose', 'open']:
                    if field in info and info[field]:
                        price = float(info[field])
                        if price > 0:
                            return round(price, 2)
            except:
                pass
                
        except Exception as e:
            logger.debug(f"Error fetching {yahoo_symbol}: {str(e)[:100]}")
        
        return None


@dataclass
class PositionDetails:
    """Complete position information with all attributes"""
    ticker: str
    symbol: str
    security_type: str
    expiry: datetime
    strike: float
    lots: float  # Signed quantity in lots
    lot_size: int
    qty: float  # lots * lot_size
    strategy: str
    direction: str  # Long/Short
    underlying_ticker: str = ""
    
    def update_qty(self):
        """Recalculate QTY from lots and lot_size"""
        self.qty = self.lots * self.lot_size
        self.direction = "Long" if self.lots > 0 else "Short" if self.lots < 0 else "Flat"
    
    def get_expiry_date_str(self) -> str:
        """Get expiry as date string (DD/MM/YYYY)"""
        return self.expiry.strftime('%d/%m/%Y')
    
    def __repr__(self):
        return f"Position({self.ticker}, {self.lots} lots @ {self.lot_size}, {self.strategy})"


class PositionManager:
    """Manages positions with complete tracking and Yahoo price fetching"""
    
    def __init__(self):
        self.positions: Dict[str, PositionDetails] = {}
        self.initial_positions_df = None
        self.ticker_details_map = {}
        self.trade_details_cache = {}
        self.price_fetcher = PriceFetcher()
    
    def initialize_from_positions(self, initial_positions: List) -> pd.DataFrame:
        """
        Initialize position manager with existing positions
        Returns DataFrame with Yahoo prices and formatted dates
        """
        self.positions.clear()
        self.ticker_details_map.clear()
        positions_data = []
        
        for pos in initial_positions:
            # Determine initial strategy
            if pos.security_type == 'Put':
                strategy = 'FUSH' if pos.position_lots > 0 else 'FULO'
            else:
                strategy = 'FULO' if pos.position_lots > 0 else 'FUSH'
            
            # Calculate QTY
            qty = pos.position_lots * pos.lot_size
            
            # Create Position object
            position_details = PositionDetails(
                ticker=pos.bloomberg_ticker,
                symbol=pos.symbol,
                security_type=pos.security_type,
                expiry=pos.expiry_date,
                strike=pos.strike_price if pos.security_type != 'Futures' else 0,
                lots=pos.position_lots,
                lot_size=pos.lot_size,
                qty=qty,
                strategy=strategy,
                direction='Long' if pos.position_lots > 0 else 'Short',
                underlying_ticker=pos.underlying_ticker
            )
            
            # Store in positions dict
            self.positions[pos.bloomberg_ticker] = position_details
            
            # Store ticker details
            self.ticker_details_map[pos.bloomberg_ticker] = {
                'symbol': pos.symbol,
                'security_type': pos.security_type,
                'expiry': pos.expiry_date,
                'strike': pos.strike_price,
                'lot_size': pos.lot_size,
                'underlying': pos.underlying_ticker
            }
            
            # Format expiry as simple date string
            expiry_date_str = pos.expiry_date.strftime('%d/%m/%Y')
            
            # Add to DataFrame data with formatted date
            positions_data.append({
                'Ticker': pos.bloomberg_ticker,
                'Symbol': pos.symbol,
                'Security_Type': pos.security_type,
                'Expiry': expiry_date_str,  # Simple date format
                'Strike': pos.strike_price if pos.security_type != 'Futures' else 0,
                'Lots': pos.position_lots,
                'Lot_Size': pos.lot_size,
                'QTY': qty,
                'Strategy': strategy,
                'Direction': 'Long' if pos.position_lots > 0 else 'Short',
                'Underlying': pos.underlying_ticker
            })
            
            logger.info(f"Initialized: {pos.bloomberg_ticker} with {pos.position_lots} lots @ {pos.lot_size}/lot")
        
        # Create DataFrame
        self.initial_positions_df = pd.DataFrame(positions_data)
        
        # Add Yahoo prices
        self.initial_positions_df = self.add_yahoo_prices(self.initial_positions_df)
        
        return self.initial_positions_df
    
    def update_position(self, ticker: str, quantity_change: float, 
                       security_type: str, strategy: str,
                       trade_object=None):
        """Update position with a trade"""
        if ticker not in self.positions:
            # NEW POSITION
            if trade_object:
                # Cache trade details
                self.trade_details_cache[ticker] = {
                    'symbol': trade_object.symbol,
                    'security_type': trade_object.security_type,
                    'expiry': trade_object.expiry_date,
                    'strike': trade_object.strike_price,
                    'lot_size': trade_object.lot_size,
                    'underlying': trade_object.underlying_ticker
                }
                
                position_details = PositionDetails(
                    ticker=ticker,
                    symbol=trade_object.symbol,
                    security_type=security_type,
                    expiry=trade_object.expiry_date,
                    strike=trade_object.strike_price if security_type != 'Futures' else 0,
                    lots=quantity_change,
                    lot_size=trade_object.lot_size,
                    qty=quantity_change * trade_object.lot_size,
                    strategy=strategy,
                    direction='Long' if quantity_change > 0 else 'Short',
                    underlying_ticker=trade_object.underlying_ticker
                )
            else:
                # Fallback
                logger.warning(f"Creating position {ticker} without full trade details")
                position_details = PositionDetails(
                    ticker=ticker,
                    symbol=ticker.split(' ')[0],
                    security_type=security_type,
                    expiry=datetime.now(),
                    strike=0,
                    lots=quantity_change,
                    lot_size=100,
                    qty=quantity_change * 100,
                    strategy=strategy,
                    direction='Long' if quantity_change > 0 else 'Short'
                )
            
            self.positions[ticker] = position_details
            logger.info(f"Created new position: {position_details}")
        else:
            # UPDATE EXISTING
            old_position = self.positions[ticker]
            old_lots = old_position.lots
            new_lots = old_lots + quantity_change
            
            if abs(new_lots) < 0.0001:
                # Position closed
                del self.positions[ticker]
                logger.info(f"Closed position for {ticker}")
            else:
                # Update position
                old_position.lots = new_lots
                old_position.strategy = strategy
                old_position.update_qty()
                logger.info(f"Updated {ticker}: {old_lots} -> {new_lots} lots")
    
    def get_position(self, ticker: str) -> Optional[PositionDetails]:
        """Get current position for a ticker"""
        return self.positions.get(ticker)
    
    def is_trade_opposing(self, ticker: str, trade_quantity: float, security_type: str) -> bool:
        """Check if trade opposes current position"""
        position = self.get_position(ticker)
        if position is None:
            return False
        return (position.lots > 0 and trade_quantity < 0) or \
               (position.lots < 0 and trade_quantity > 0)
    
    def get_final_positions(self) -> pd.DataFrame:
        """Get final positions with Yahoo prices and formatted dates"""
        if not self.positions:
            # Return empty DataFrame with correct structure
            return pd.DataFrame(columns=[
                'Ticker', 'Symbol', 'Security_Type', 'Expiry', 'Strike',
                'Lots', 'Lot_Size', 'QTY', 'Strategy', 'Direction',
                'Underlying', 'Yahoo_Price', 'Moneyness'
            ])
        
        positions_data = []
        
        for ticker, position in self.positions.items():
            # Format expiry as simple date string
            expiry_date_str = position.expiry.strftime('%d/%m/%Y')
            
            positions_data.append({
                'Ticker': ticker,
                'Symbol': position.symbol,
                'Security_Type': position.security_type,
                'Expiry': expiry_date_str,  # Simple date format
                'Strike': position.strike,
                'Lots': position.lots,
                'Lot_Size': position.lot_size,
                'QTY': position.qty,
                'Strategy': position.strategy,
                'Direction': position.direction,
                'Underlying': position.underlying_ticker
            })
        
        final_df = pd.DataFrame(positions_data)
        
        # Sort by ticker
        final_df = final_df.sort_values('Ticker').reset_index(drop=True)
        
        # Add Yahoo prices
        final_df = self.add_yahoo_prices(final_df)
        
        logger.info(f"Final positions: {len(final_df)} positions")
        
        return final_df
    
    def add_yahoo_prices(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add Yahoo prices and calculate moneyness for options"""
        if df.empty:
            return df
        
        df = df.copy()
        
        # Add columns if they don't exist
        if 'Yahoo_Price' not in df.columns:
            df['Yahoo_Price'] = 'N/A'
        if 'Moneyness' not in df.columns:
            df['Moneyness'] = ''
        
        logger.info(f"Fetching Yahoo prices for {len(df)} positions...")
        
        # Get unique symbols
        unique_symbols = df['Symbol'].unique()
        
        # Fetch price for each unique symbol
        for symbol in unique_symbols:
            price = self.price_fetcher.fetch_price_for_symbol(symbol)
            
            # Update all rows with this symbol
            symbol_mask = df['Symbol'] == symbol
            
            if price is not None and price > 0:
                # We have a valid price
                df.loc[symbol_mask, 'Yahoo_Price'] = price
                
                # Calculate moneyness for options
                for idx in df[symbol_mask].index:
                    row = df.loc[idx]
                    
                    if row['Security_Type'] == 'Call':
                        strike = row['Strike']
                        if price > strike * 1.01:  # 1% buffer
                            df.at[idx, 'Moneyness'] = 'ITM'
                        elif price < strike * 0.99:
                            df.at[idx, 'Moneyness'] = 'OTM'
                        else:
                            df.at[idx, 'Moneyness'] = 'ATM'
                    elif row['Security_Type'] == 'Put':
                        strike = row['Strike']
                        if price < strike * 0.99:  # 1% buffer
                            df.at[idx, 'Moneyness'] = 'ITM'
                        elif price > strike * 1.01:
                            df.at[idx, 'Moneyness'] = 'OTM'
                        else:
                            df.at[idx, 'Moneyness'] = 'ATM'
                    elif row['Security_Type'] == 'Futures':
                        df.at[idx, 'Moneyness'] = 'N/A'
            else:
                # No price available - use N/A
                df.loc[symbol_mask, 'Yahoo_Price'] = 'N/A'
                df.loc[symbol_mask, 'Moneyness'] = 'N/A'
        
        # Convert Yahoo_Price to proper format (numeric where possible, string for N/A)
        for idx in df.index:
            val = df.at[idx, 'Yahoo_Price']
            if val != 'N/A' and pd.notna(val):
                try:
                    df.at[idx, 'Yahoo_Price'] = round(float(val), 2)
                except:
                    df.at[idx, 'Yahoo_Price'] = 'N/A'
        
        return df
    
    def get_position_summary(self) -> Dict:
        """Get summary statistics"""
        if not self.positions:
            return {
                'total_positions': 0,
                'long_positions': 0,
                'short_positions': 0,
                'by_security_type': {},
                'by_strategy': {}
            }
        
        long_count = sum(1 for p in self.positions.values() if p.lots > 0)
        short_count = sum(1 for p in self.positions.values() if p.lots < 0)
        
        by_type = {}
        for p in self.positions.values():
            by_type[p.security_type] = by_type.get(p.security_type, 0) + 1
        
        by_strategy = {}
        for p in self.positions.values():
            by_strategy[p.strategy] = by_strategy.get(p.strategy, 0) + 1
        
        return {
            'total_positions': len(self.positions),
            'long_positions': long_count,
            'short_positions': short_count,
            'by_security_type': by_type,
            'by_strategy': by_strategy
        }
    
    def clear_all_positions(self):
        """Clear all positions"""
        self.positions.clear()
        self.ticker_details_map.clear()
        self.trade_details_cache.clear()
        self.price_fetcher.price_cache.clear()
        logger.info("Cleared all positions and price cache")
