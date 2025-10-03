"""
Position Grouping Module
Groups positions by underlying for hierarchical display
"""

import pandas as pd
from typing import Dict, List, Any, Optional, Tuple
import logging
from dataclasses import dataclass
from datetime import datetime

logger = logging.getLogger(__name__)


@dataclass
class GroupedPosition:
    """Represents a grouped position by underlying"""
    underlying: str
    bloomberg_ticker: str
    symbol: str
    expiry: datetime
    security_type: str
    strike: float
    position_lots: float
    lot_size: int

    @property
    def position_quantity(self) -> float:
        """Get position quantity (lots * lot_size)"""
        return self.position_lots * self.lot_size


class PositionGrouper:
    """Groups and organizes positions by underlying"""

    def __init__(self):
        self.grouped_positions = {}

    def group_positions_from_dataframe(self, df: pd.DataFrame,
                                     position_col: str = 'Lots',
                                     price_manager=None) -> Dict[str, List[Dict]]:
        """
        Group positions from a dataframe by underlying
        Calculates net deliverable positions based on ITM/OTM status

        Returns: Dict with underlying as key and list of positions as value
        """
        grouped = {}

        # Identify relevant columns
        cols_mapping = {
            'underlying': self._find_column(df, ['Underlying', 'Underlying Ticker', 'Underlying Asset']),
            'bloomberg': self._find_column(df, ['Ticker', 'Bloomberg Ticker', 'BBG Ticker']),
            'symbol': self._find_column(df, ['Symbol', 'Trading Symbol']),
            'expiry': self._find_column(df, ['Expiry', 'Expiry Date', 'Maturity']),
            'type': self._find_column(df, ['Security_Type', 'Security Type', 'Type', 'Instrument Type']),
            'strike': self._find_column(df, ['Strike', 'Strike Price']),
            'position': self._find_column(df, ['Lots', 'Final Position (Lots)', 'Position']),
            'lot_size': self._find_column(df, ['Lot_Size', 'Lot Size', 'Contract Size'])
        }

        # Group by underlying
        for idx, row in df.iterrows():
            underlying = self._get_value(row, cols_mapping['underlying'], 'Unknown')

            if underlying not in grouped:
                grouped[underlying] = {
                    'underlying': underlying,
                    'positions': [],
                    'total_futures': 0,
                    'total_calls': 0,
                    'total_puts': 0,
                    'net_position': 0,
                    'net_deliverable': 0,  # Net deliverable position
                    'unique_expiries': set(),
                    'spot_price': None
                }

            # Extract position details
            position = {
                'bloomberg_ticker': self._get_value(row, cols_mapping['bloomberg'], ''),
                'symbol': self._get_value(row, cols_mapping['symbol'], ''),
                'expiry': self._parse_date(self._get_value(row, cols_mapping['expiry'], '')),
                'security_type': self._get_value(row, cols_mapping['type'], ''),
                'strike': self._get_float(row, cols_mapping['strike']),
                'position_lots': self._get_float(row, cols_mapping['position']),
                'lot_size': self._get_int(row, cols_mapping['lot_size'], 1)
            }

            # Skip if no position
            if position['position_lots'] == 0:
                continue

            grouped[underlying]['positions'].append(position)

            # Update totals based on security type
            if position['security_type'] == 'Futures':
                grouped[underlying]['total_futures'] += position['position_lots']
            elif position['security_type'] == 'Call':
                grouped[underlying]['total_calls'] += position['position_lots']
            elif position['security_type'] == 'Put':
                grouped[underlying]['total_puts'] += position['position_lots']

            # For net_position, we still show the raw sum
            grouped[underlying]['net_position'] += position['position_lots']

            if position['expiry']:
                grouped[underlying]['unique_expiries'].add(position['expiry'])

        # Calculate net deliverable positions
        for underlying in grouped:
            # Try to get spot price with multiple fallbacks
            spot_price = None
            if price_manager:
                # Try underlying first
                spot_price = price_manager.get_price(underlying)

                # If not found, try the first position's symbol
                if not spot_price and grouped[underlying]['positions']:
                    first_pos = grouped[underlying]['positions'][0]
                    spot_price = price_manager.get_price(first_pos['symbol'])

                # Try variations of the underlying name
                if not spot_price:
                    # Try base ticker (before space)
                    if ' ' in underlying:
                        base = underlying.split(' ')[0]
                        spot_price = price_manager.get_price(base)

                    # Try with common suffixes
                    if not spot_price:
                        for suffix in ['.NS', '.BO', ' IS Equity', ' Index']:
                            test_key = underlying.replace(' IS Equity', '').replace(' Index', '') + suffix
                            spot_price = price_manager.get_price(test_key)
                            if spot_price:
                                break

                if spot_price:
                    grouped[underlying]['spot_price'] = spot_price

            # Calculate net deliverable
            net_deliverable = 0
            for pos in grouped[underlying]['positions']:
                if pos['security_type'] == 'Futures':
                    # Futures always deliver (sign of position)
                    net_deliverable += pos['position_lots']
                elif spot_price:  # Only calculate for options if we have price
                    if pos['security_type'] == 'Call' and spot_price > pos['strike']:
                        # ITM calls deliver long underlying (same sign as call position)
                        net_deliverable += pos['position_lots']
                    elif pos['security_type'] == 'Put' and spot_price < pos['strike']:
                        # ITM puts deliver short underlying (opposite sign of put position)
                        net_deliverable -= pos['position_lots']
                    # OTM options don't deliver

            grouped[underlying]['net_deliverable'] = net_deliverable

        # Convert sets to lists for JSON serialization
        for underlying in grouped:
            grouped[underlying]['unique_expiries'] = sorted(list(grouped[underlying]['unique_expiries']))
            # Sort positions by expiry and strike
            grouped[underlying]['positions'].sort(
                key=lambda x: (x['expiry'] or datetime.max, x['strike'] or 0)
            )

        return grouped

    def create_summary_dataframe(self, grouped_data: Dict[str, Dict]) -> pd.DataFrame:
        """Create a summary dataframe from grouped positions"""
        summary_data = []

        for underlying, data in grouped_data.items():
            row_data = {
                'Underlying': underlying,
                'Total Positions': len(data['positions']),
                'Net Position (Lots)': data['net_position'],
                'Net Deliverable (Lots)': data.get('net_deliverable', 0),
                'Futures': data['total_futures'],
                'Calls': data['total_calls'],
                'Puts': data['total_puts'],
                'Unique Expiries': len(data['unique_expiries'])
            }
            summary_data.append(row_data)

        return pd.DataFrame(summary_data)

    def group_by_expiry(self, grouped_data: Dict[str, Dict]) -> Dict[str, Dict]:
        """
        Reorganize grouped data by expiry date
        Returns: Dict with expiry date as key
        """
        expiry_groups = {}

        for underlying, data in grouped_data.items():
            for pos in data['positions']:
                expiry = pos['expiry']
                if not expiry:
                    continue

                expiry_key = expiry.strftime('%d/%m/%Y')

                if expiry_key not in expiry_groups:
                    expiry_groups[expiry_key] = {
                        'expiry_date': expiry,
                        'underlyings': {},
                        'total_deliverable': 0,
                        'total_futures': 0,
                        'total_calls': 0,
                        'total_puts': 0
                    }

                if underlying not in expiry_groups[expiry_key]['underlyings']:
                    expiry_groups[expiry_key]['underlyings'][underlying] = {
                        'underlying': underlying,
                        'positions': [],
                        'net_deliverable': 0,
                        'spot_price': data.get('spot_price')
                    }

                expiry_groups[expiry_key]['underlyings'][underlying]['positions'].append(pos)

                # Update totals
                if pos['security_type'] == 'Futures':
                    expiry_groups[expiry_key]['total_futures'] += pos['position_lots']
                elif pos['security_type'] == 'Call':
                    expiry_groups[expiry_key]['total_calls'] += pos['position_lots']
                elif pos['security_type'] == 'Put':
                    expiry_groups[expiry_key]['total_puts'] += pos['position_lots']

        # Calculate deliverables for each underlying in each expiry
        for expiry_key, expiry_data in expiry_groups.items():
            for underlying, und_data in expiry_data['underlyings'].items():
                spot_price = und_data.get('spot_price')
                net_deliv = 0

                for pos in und_data['positions']:
                    if pos['security_type'] == 'Futures':
                        net_deliv += pos['position_lots']
                    elif spot_price:
                        if pos['security_type'] == 'Call' and spot_price > pos['strike']:
                            net_deliv += pos['position_lots']
                        elif pos['security_type'] == 'Put' and spot_price < pos['strike']:
                            net_deliv -= pos['position_lots']  # Negative for puts

                und_data['net_deliverable'] = net_deliv
                expiry_data['total_deliverable'] += net_deliv

        return expiry_groups

    def create_detailed_dataframe(self, underlying: str, data: Dict) -> pd.DataFrame:
        """Create detailed dataframe for a specific underlying"""
        if not data['positions']:
            return pd.DataFrame()

        detailed_data = []
        spot_price = data.get('spot_price')

        for pos in data['positions']:
            # Calculate deliverable for this position
            deliverable = 0
            moneyness = ''

            if pos['security_type'] == 'Futures':
                deliverable = pos['position_lots']
                moneyness = 'N/A'
            elif spot_price and pos['strike']:
                if pos['security_type'] == 'Call':
                    if spot_price > pos['strike']:
                        moneyness = 'ITM'
                        deliverable = pos['position_lots']  # Long call = long underlying
                    elif spot_price < pos['strike']:
                        moneyness = 'OTM'
                        deliverable = 0
                    else:
                        moneyness = 'ATM'
                        deliverable = 0
                elif pos['security_type'] == 'Put':
                    if spot_price < pos['strike']:
                        moneyness = 'ITM'
                        deliverable = -pos['position_lots']  # Long put = short underlying
                    elif spot_price > pos['strike']:
                        moneyness = 'OTM'
                        deliverable = 0
                    else:
                        moneyness = 'ATM'
                        deliverable = 0

            detailed_data.append({
                'Symbol': pos['symbol'],
                'Bloomberg Ticker': pos['bloomberg_ticker'],
                'Expiry': pos['expiry'].strftime('%d/%m/%Y') if pos['expiry'] else '',
                'Type': pos['security_type'],
                'Strike': pos['strike'] if pos['strike'] else '',
                'Position (Lots)': pos['position_lots'],
                'Moneyness': moneyness,
                'Deliverable (Lots)': deliverable,
                'Lot Size': pos['lot_size'],
                'Position (Qty)': pos['position_lots'] * pos['lot_size']
            })

        return pd.DataFrame(detailed_data)

    def group_trades_from_parser(self, trades: List[Any]) -> Dict[str, Dict]:
        """Group trades from Trade_Parser output"""
        grouped = {}

        for trade in trades:
            # Get underlying from trade object
            underlying = getattr(trade, 'underlying_ticker', 'Unknown')

            if underlying not in grouped:
                grouped[underlying] = {
                    'underlying': underlying,
                    'positions': [],
                    'total_futures': 0,
                    'total_calls': 0,
                    'total_puts': 0,
                    'net_position': 0,
                    'unique_expiries': set(),
                    'spot_price': None
                }

            # Create position dict
            position = {
                'bloomberg_ticker': getattr(trade, 'bloomberg_ticker', ''),
                'symbol': getattr(trade, 'symbol', ''),
                'expiry': getattr(trade, 'expiry_date', None),
                'security_type': getattr(trade, 'security_type', ''),
                'strike': getattr(trade, 'strike_price', 0),
                'position_lots': getattr(trade, 'position_lots', 0),
                'lot_size': getattr(trade, 'lot_size', 1)
            }

            grouped[underlying]['positions'].append(position)

            # Update totals
            if position['security_type'] == 'Futures':
                grouped[underlying]['total_futures'] += position['position_lots']
            elif position['security_type'] == 'Call':
                grouped[underlying]['total_calls'] += position['position_lots']
            elif position['security_type'] == 'Put':
                grouped[underlying]['total_puts'] += position['position_lots']

            grouped[underlying]['net_position'] += position['position_lots']

            if position['expiry']:
                grouped[underlying]['unique_expiries'].add(position['expiry'])

        # Convert sets to lists
        for underlying in grouped:
            grouped[underlying]['unique_expiries'] = sorted(list(grouped[underlying]['unique_expiries']))
            grouped[underlying]['positions'].sort(
                key=lambda x: (x['expiry'] or datetime.max, x['strike'] or 0)
            )

        return grouped

    def _find_column(self, df: pd.DataFrame, possible_names: List[str]) -> Optional[str]:
        """Find column by possible names"""
        for name in possible_names:
            if name in df.columns:
                return name
            # Case insensitive search
            for col in df.columns:
                if col.lower() == name.lower():
                    return col
        return None

    def _get_value(self, row: pd.Series, col: Optional[str], default: Any = '') -> Any:
        """Safely get value from row"""
        if col and col in row.index:
            val = row[col]
            if pd.notna(val):
                return val
        return default

    def _get_float(self, row: pd.Series, col: Optional[str], default: float = 0) -> float:
        """Safely get float value from row"""
        val = self._get_value(row, col, default)
        try:
            return float(val)
        except (ValueError, TypeError):
            return default

    def _get_int(self, row: pd.Series, col: Optional[str], default: int = 0) -> int:
        """Safely get int value from row"""
        val = self._get_value(row, col, default)
        try:
            return int(float(val))
        except (ValueError, TypeError):
            return default

    def _parse_date(self, date_val: Any) -> Optional[datetime]:
        """Parse date from various formats"""
        if not date_val:
            return None

        if isinstance(date_val, datetime):
            return date_val

        if isinstance(date_val, pd.Timestamp):
            return date_val.to_pydatetime()

        # Try string parsing
        try:
            return pd.to_datetime(date_val).to_pydatetime()
        except:
            return None