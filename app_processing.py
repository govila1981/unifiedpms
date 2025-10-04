"""
Processing functions for the Streamlit Trade Processing App
Handles Stage 1, Stage 2, deliverables, reconciliation, and broker reconciliation
"""

import streamlit as st
import pandas as pd
from pathlib import Path
import tempfile
import logging
import traceback
import os
from datetime import datetime

# Import utility functions
from app_utils import is_streamlit_cloud, get_temp_dir, get_output_path

# Import existing modules
from input_parser import InputParser
from Trade_Parser import TradeParser
from position_manager import PositionManager
from trade_processor import TradeProcessor
from output_generator import OutputGenerator
from acm_mapper import ACMMapper

# Optional feature imports
try:
    from deliverables_calculator import DeliverableCalculator
    from enhanced_recon_module import EnhancedReconciliation
    NEW_FEATURES_AVAILABLE = True
except ImportError:
    NEW_FEATURES_AVAILABLE = False

try:
    from expiry_delivery_module import ExpiryDeliveryGenerator
    EXPIRY_DELIVERY_AVAILABLE = True
except ImportError:
    EXPIRY_DELIVERY_AVAILABLE = False

try:
    from trade_reconciliation import TradeReconciler
    BROKER_RECON_AVAILABLE = True
except ImportError:
    BROKER_RECON_AVAILABLE = False

try:
    from encrypted_file_handler import read_csv_or_excel_with_password
    ENCRYPTED_FILE_SUPPORT = True
except ImportError:
    ENCRYPTED_FILE_SUPPORT = False

logger = logging.getLogger(__name__)


def process_stage1(position_file, trade_file, mapping_file, use_default, default_path,
                  position_password=None, trade_password=None, account_prefix=""):
    """Process Stage 1: Strategy Assignment with encrypted file support"""
    try:
        with st.spinner("Processing Stage 1: Strategy Assignment..."):
            temp_dir = get_temp_dir()

            # Handle position file (with potential encryption)
            if ENCRYPTED_FILE_SUPPORT and position_password:
                # Try to decrypt the file
                success, df, error = read_csv_or_excel_with_password(position_file, position_password)
                if not success:
                    st.error(f"Failed to decrypt position file: {error}")
                    return False
                # Save decrypted data to temporary file
                suffix = Path(position_file.name).suffix
                # Force .xlsx for Excel files (pandas doesn't support writing .xls)
                if suffix.lower() in ['.xls', '.xlsx']:
                    suffix = '.xlsx'
                with tempfile.NamedTemporaryFile(delete=False, suffix=suffix, dir=temp_dir) as tmp:
                    if suffix == '.csv':
                        df.to_csv(tmp.name, index=False)
                    else:
                        df.to_excel(tmp.name, index=False)
                    pos_path = tmp.name
            else:
                # Save uploaded file normally
                with tempfile.NamedTemporaryFile(delete=False, suffix=Path(position_file.name).suffix, dir=temp_dir) as tmp:
                    tmp.write(position_file.getbuffer())
                    pos_path = tmp.name

            # Handle trade file (with potential encryption)
            if ENCRYPTED_FILE_SUPPORT and trade_password:
                # Try to decrypt the file
                success, df, error = read_csv_or_excel_with_password(trade_file, trade_password)
                if not success:
                    st.error(f"Failed to decrypt trade file: {error}")
                    return False
                # Save decrypted data to temporary file
                suffix = Path(trade_file.name).suffix
                # Force .xlsx for Excel files (pandas doesn't support writing .xls)
                if suffix.lower() in ['.xls', '.xlsx']:
                    suffix = '.xlsx'
                with tempfile.NamedTemporaryFile(delete=False, suffix=suffix, dir=temp_dir) as tmp:
                    if suffix == '.csv':
                        df.to_csv(tmp.name, index=False)
                    else:
                        df.to_excel(tmp.name, index=False)
                    trade_path = tmp.name
            else:
                # Save uploaded file normally
                with tempfile.NamedTemporaryFile(delete=False, suffix=Path(trade_file.name).suffix, dir=temp_dir) as tmp:
                    tmp.write(trade_file.getbuffer())
                    trade_path = tmp.name

            if mapping_file:
                with tempfile.NamedTemporaryFile(delete=False, suffix='.csv', dir=temp_dir) as tmp:
                    tmp.write(mapping_file.getbuffer())
                    map_path = tmp.name
            else:
                map_path = default_path

            # Parse positions
            input_parser = InputParser(map_path)
            positions = input_parser.parse_file(pos_path)

            if not positions:
                st.error("❌ No positions found")
                return False

            st.success(f"✅ Parsed {len(positions)} positions ({input_parser.format_type} format)")

            # Parse trades
            trade_parser = TradeParser(map_path)

            if trade_path.endswith('.csv'):
                trade_df = pd.read_csv(trade_path, header=None)
            else:
                trade_df = pd.read_excel(trade_path, header=None)

            trades = trade_parser.parse_trade_file(trade_path)

            if not trades:
                st.error("❌ No trades found")
                return False

            st.success(f"✅ Parsed {len(trades)} trades ({trade_parser.format_type} format)")

            # Check for missing mappings
            missing_positions = len(input_parser.unmapped_symbols) if hasattr(input_parser, 'unmapped_symbols') else 0
            missing_trades = len(trade_parser.unmapped_symbols) if hasattr(trade_parser, 'unmapped_symbols') else 0

            if missing_positions > 0 or missing_trades > 0:
                st.warning(f"⚠️ Unmapped symbols: {missing_positions} from positions, {missing_trades} from trades")

            # Process trades
            position_manager = PositionManager()
            starting_positions_df = position_manager.initialize_from_positions(positions)

            trade_processor = TradeProcessor(position_manager)
            # Create OutputGenerator with appropriate path
            if is_streamlit_cloud():
                output_gen = OutputGenerator(str(get_temp_dir() / "stage1"), account_prefix=account_prefix)
            else:
                output_gen = OutputGenerator("output/stage1", account_prefix=account_prefix)

            parsed_trades_df = output_gen.create_trade_dataframe_from_positions(trades)
            processed_trades_df = trade_processor.process_trades(trades, trade_df)
            final_positions_df = position_manager.get_final_positions()

            # Generate final enhanced clearing file (original format with splits applied)
            has_headers = trade_processor._check_for_headers(trade_df) if hasattr(trade_processor, '_check_for_headers') else False
            header_row = trade_df.iloc[0].to_list() if has_headers else None
            final_enhanced_clearing_df = trade_processor.create_final_enhanced_clearing_file(
                trade_processor.processed_trades,
                trade_df,
                has_headers,
                header_row
            )

            # Save final enhanced clearing file
            # Extract trade date for file naming from processed_trades_df (has TD column)
            trade_date_str = output_gen._extract_trade_date(processed_trades_df)

            if is_streamlit_cloud():
                output_dir = get_temp_dir() / "stage1"
                output_dir.mkdir(parents=True, exist_ok=True)
            else:
                output_dir = Path("output/stage1")
                output_dir.mkdir(parents=True, exist_ok=True)

            final_enhanced_file = output_dir / f"{account_prefix}final_enhanced_clearing_{trade_date_str}.csv"

            # Format date columns as DD/MM/YYYY
            if 'Expiry Dt' in final_enhanced_clearing_df.columns:
                final_enhanced_clearing_df['Expiry Dt'] = pd.to_datetime(final_enhanced_clearing_df['Expiry Dt'], dayfirst=True, errors='coerce').dt.strftime('%d/%m/%Y')
            if 'TD' in final_enhanced_clearing_df.columns:
                final_enhanced_clearing_df['TD'] = final_enhanced_clearing_df['TD'].apply(
                    lambda x: pd.to_datetime(x, dayfirst=True, errors='coerce').strftime('%d/%m/%Y') if pd.notna(x) and x != '' else ''
                )

            final_enhanced_clearing_df.to_csv(final_enhanced_file, index=False)
            logger.info(f"Saved final enhanced clearing file: {final_enhanced_file}")

            # Store in session state for download
            st.session_state.final_enhanced_clearing_file = str(final_enhanced_file)

            # Generate output files
            output_files = output_gen.save_all_outputs(
                parsed_trades_df,
                starting_positions_df,
                processed_trades_df,
                final_positions_df,
                file_prefix="stage1",
                input_parser=input_parser,
                trade_parser=trade_parser,
                send_email=False,  # Don't send automatically
                email_recipients=[],
                email_file_filter=None
            )

            # Store in session state
            st.session_state.stage1_outputs = output_files
            st.session_state.dataframes['stage1'] = {
                'parsed_trades': parsed_trades_df,
                'starting_positions': starting_positions_df,
                'processed_trades': processed_trades_df,
                'final_positions': final_positions_df
            }
            st.session_state.stage1_complete = True
            st.session_state.processed_trades_for_acm = processed_trades_df

            # Debug: Log columns available for Stage 2
            logger.info(f"Stored {len(processed_trades_df)} trades for ACM with columns: {list(processed_trades_df.columns)}")

            # Metrics
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Starting Positions", len(starting_positions_df))
            with col2:
                st.metric("Trades Processed", len(trades))
            with col3:
                if 'Split?' in processed_trades_df.columns:
                    splits = len(processed_trades_df[processed_trades_df['Split?'] == 'Yes'])
                    st.metric("Split Trades", splits)
            with col4:
                st.metric("Final Positions", len(final_positions_df))

            # Show email status if enabled
            if st.session_state.get('send_email', False) and st.session_state.get('email_recipients'):
                recipients = st.session_state.get('email_recipients', [])
                st.success(f"✅ Stage 1 Complete! 📧 Email sent to {len(recipients)} recipient(s)")
            else:
                st.success("✅ Stage 1 Complete!")

            return True

    except Exception as e:
        st.error(f"❌ Error in Stage 1: {str(e)}")
        st.code(traceback.format_exc())
        return False


def process_stage2(schema_option, custom_schema_file, account_prefix=""):
    """Process Stage 2: ACM Mapping"""
    try:
        with st.spinner("Processing Stage 2: ACM Mapping..."):
            if 'processed_trades_for_acm' not in st.session_state:
                st.error("❌ Stage 1 must be completed first")
                return False

            processed_trades_df = st.session_state.processed_trades_for_acm

            # Validate we have trades to process
            if processed_trades_df is None or len(processed_trades_df) == 0:
                st.error("❌ No trades found in Stage 1 output. Please re-run Stage 1.")
                return False

            st.info(f"Processing {len(processed_trades_df)} trades to ACM format...")

            # Initialize ACM Mapper
            if schema_option == "Use built-in schema (default)":
                acm_mapper = ACMMapper()
                st.info("Using built-in ACM schema")
            else:
                if not custom_schema_file:
                    st.error("❌ Please upload a custom schema file")
                    return False

                temp_dir = get_temp_dir()
                with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx', dir=temp_dir) as tmp:
                    tmp.write(custom_schema_file.getbuffer())
                    schema_path = tmp.name

                acm_mapper = ACMMapper(schema_path)
                st.info(f"Using custom schema: {custom_schema_file.name}")

            st.session_state.acm_mapper = acm_mapper

            # Process to ACM format
            mapped_df, errors_df = acm_mapper.process_trades_to_acm(processed_trades_df)

            # Save outputs
            # Use appropriate output directory
            if is_streamlit_cloud():
                output_dir = get_temp_dir() / "stage2"
                output_dir.mkdir(parents=True, exist_ok=True)
            else:
                output_dir = Path("output/stage2")
                output_dir.mkdir(parents=True, exist_ok=True)

            # Extract trade date from processed trades
            trade_date_str = None
            if processed_trades_df is not None and not processed_trades_df.empty:
                from output_generator import OutputGenerator
                temp_gen = OutputGenerator()
                trade_date_str = temp_gen._extract_trade_date(processed_trades_df)

            # Use trade date if available, otherwise timestamp
            date_str = trade_date_str if trade_date_str else datetime.now().strftime("%Y%m%d_%H%M%S")

            acm_file = output_dir / f"{account_prefix}acm_listedtrades_{date_str}.csv"
            mapped_df.to_csv(acm_file, index=False)

            errors_file = output_dir / f"{account_prefix}acm_listedtrades_{date_str}_errors.csv"
            errors_df.to_csv(errors_file, index=False)

            schema_file = output_dir / f"{account_prefix}acm_schema_used_{date_str}.xlsx"
            schema_bytes = acm_mapper.generate_schema_excel()
            with open(schema_file, 'wb') as f:
                f.write(schema_bytes)

            # Store in session state
            st.session_state.stage2_outputs = {
                'acm_mapped': acm_file,
                'errors': errors_file,
                'schema_used': schema_file
            }
            st.session_state.dataframes['stage2'] = {
                'mapped': mapped_df,
                'errors': errors_df
            }
            st.session_state.stage2_complete = True

            # Show results
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Records Mapped", len(mapped_df))
            with col2:
                st.metric("Validation Errors", len(errors_df))

            if len(errors_df) == 0:
                st.success("✅ Stage 2 Complete! No validation errors.")
            else:
                st.warning(f"⚠️ Stage 2 Complete with {len(errors_df)} validation errors.")

            return True

    except Exception as e:
        st.error(f"❌ Error in Stage 2: {str(e)}")
        st.code(traceback.format_exc())
        return False


def run_deliverables_calculation(usdinr_rate: float, account_prefix=""):
    """Run deliverables and IV calculations using centralized price manager"""
    if not NEW_FEATURES_AVAILABLE:
        st.error("Deliverables module not available")
        return

    try:
        if 'dataframes' not in st.session_state or 'stage1' not in st.session_state.dataframes:
            st.error("Please complete Stage 1 first")
            return

        with st.spinner("Calculating deliverables and intrinsic values..."):
            stage1_data = st.session_state.dataframes['stage1']
            starting_positions = stage1_data.get('starting_positions', pd.DataFrame())
            final_positions = stage1_data.get('final_positions', pd.DataFrame())

            # Use centralized price manager
            prices = {}
            if 'price_manager' in st.session_state:
                pm = st.session_state.price_manager

                # Collect all unique underlying symbols
                underlying_symbols = set()
                for df in [starting_positions, final_positions]:
                    if not df.empty and 'Underlying' in df.columns:
                        underlying_symbols.update(df['Underlying'].dropna().unique())

                # Get prices from price manager
                for symbol in underlying_symbols:
                    price = pm.get_price(symbol)
                    if price:
                        prices[symbol] = price

                # Also check for symbol column
                for df in [starting_positions, final_positions]:
                    if not df.empty and 'Symbol' in df.columns:
                        for symbol in df['Symbol'].unique():
                            if symbol not in prices:
                                price = pm.get_price(symbol)
                                if price:
                                    prices[symbol] = price
            else:
                st.info("ℹ️ No prices available. Deliverables will be calculated with 0 prices (options assumed 0).")
                prices = {}  # Empty dict, will default to 0 in calculator

            calc = DeliverableCalculator(usdinr_rate)

            # Extract trade date from processed trades
            trade_date_str = None
            processed_trades = stage1_data.get('processed_trades')
            if processed_trades is not None and not processed_trades.empty:
                from output_generator import OutputGenerator
                temp_gen = OutputGenerator()
                trade_date_str = temp_gen._extract_trade_date(processed_trades)

            # Use trade date if available, otherwise timestamp
            date_str = trade_date_str if trade_date_str else datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = get_output_path(f"{account_prefix}DELIVERABLES_REPORT_{date_str}.xlsx")

            calc.generate_deliverables_report(
                starting_positions,
                final_positions,
                prices,
                output_file,
                report_type="TRADE_PROCESSING"
            )

            st.session_state.deliverables_file = output_file
            st.session_state.deliverables_complete = True

            pre_deliv = calc.calculate_deliverables_from_dataframe(starting_positions, prices)
            post_deliv = calc.calculate_deliverables_from_dataframe(final_positions, prices)

            # Get price report from simple price manager
            price_report_df = None
            if 'price_manager' in st.session_state:
                try:
                    price_report_df = st.session_state.price_manager.get_price_summary()
                except:
                    pass

            st.session_state.deliverables_data = {
                'pre_trade': pre_deliv,
                'post_trade': post_deliv,
                'prices': prices,
                'price_report': price_report_df
            }

            # Check for missing prices and warn user
            missing_prices_count = 0
            options_count = 0

            for df in [pre_deliv, post_deliv]:
                if not df.empty:
                    # Count options that need prices
                    options_mask = df['Security_Type'].isin(['Call', 'Put'])
                    options_count += options_mask.sum()

                    # Count missing prices for options
                    if 'Spot_Price' in df.columns:
                        missing_mask = options_mask & ((df['Spot_Price'] == 'N/A') | (df['Spot_Price'] == 0))
                        missing_prices_count += missing_mask.sum()

            st.success(f"✅ Deliverables calculated and saved!")

            if missing_prices_count > 0:
                st.warning(f"⚠️ {missing_prices_count} out of {options_count} options positions have no price data. "
                          "ITM/OTM determination and deliverable quantities cannot be calculated for these positions. "
                          "Futures positions are unaffected.")
                st.info("💡 To resolve: Use the Price Management section above to fetch Yahoo prices or upload a price file.")

    except Exception as e:
        st.error(f"❌ Error calculating deliverables: {str(e)}")
        logger.error(traceback.format_exc())


def run_expiry_delivery_generation(account_prefix=""):
    """Generate physical delivery outputs per expiry date"""
    if not EXPIRY_DELIVERY_AVAILABLE:
        st.error("Expiry Delivery Generator module not available. Please ensure expiry_delivery_module.py is in the directory.")
        return

    try:
        if 'dataframes' not in st.session_state or 'stage1' not in st.session_state.dataframes:
            st.error("Please complete Stage 1 first")
            return

        with st.spinner("Generating expiry delivery reports..."):
            # Get positions from Stage 1
            stage1_data = st.session_state.dataframes['stage1']
            starting_positions = stage1_data.get('starting_positions', pd.DataFrame())
            final_positions = stage1_data.get('final_positions', pd.DataFrame())

            # Check if we have positions
            if starting_positions.empty and final_positions.empty:
                st.warning("No positions found to process for expiry deliveries")
                return

            # Show position counts
            st.info(f"Processing {len(starting_positions)} starting positions and {len(final_positions)} final positions")

            # Get prices from centralized price manager
            prices = {}
            if 'price_manager' in st.session_state:
                pm = st.session_state.price_manager

                # Collect all unique underlying symbols
                underlying_symbols = set()
                for df in [starting_positions, final_positions]:
                    if not df.empty and 'Underlying' in df.columns:
                        underlying_symbols.update(df['Underlying'].dropna().unique())
                    if not df.empty and 'Symbol' in df.columns:
                        # Also add symbols (for futures and other instruments)
                        for symbol in df['Symbol'].unique():
                            underlying_symbols.add(symbol)

                # Get prices from price manager
                for symbol in underlying_symbols:
                    price = pm.get_price(symbol)
                    if price:
                        prices[symbol] = price

                st.info(f"Found {len(prices)} prices for ITM calculations")
            else:
                st.warning("Price manager not initialized. ITM calculations will not be available.")

            # Initialize generator
            generator = ExpiryDeliveryGenerator(usdinr_rate=st.session_state.get('usdinr_rate', 88.0))

            # Process positions by expiry
            pre_trade_results = generator.process_positions_by_expiry(
                starting_positions, prices, "Pre-Trade"
            )

            post_trade_results = generator.process_positions_by_expiry(
                final_positions, prices, "Post-Trade"
            )

            # Check if we got any results
            if not pre_trade_results and not post_trade_results:
                st.warning("No expiry positions found to process")
                return

            all_expiries = set(list(pre_trade_results.keys()) + list(post_trade_results.keys()))
            st.info(f"Found {len(all_expiries)} unique expiry dates")

            # Generate reports
            # Use appropriate output directory
            if is_streamlit_cloud():
                output_dir = str(get_temp_dir() / "expiry_deliveries")
            else:
                output_dir = "output/expiry_deliveries"
            Path(output_dir).mkdir(parents=True, exist_ok=True)
            output_files = generator.generate_expiry_reports(
                pre_trade_results, post_trade_results, output_dir
            )

            # Store in session state
            st.session_state.expiry_delivery_files = output_files
            st.session_state.expiry_delivery_results = {
                'pre_trade': pre_trade_results,
                'post_trade': post_trade_results
            }
            st.session_state.expiry_deliveries_complete = True

            # Show success with details
            if output_files:
                st.success(f"✅ Successfully generated {len(output_files)} expiry delivery reports!")

                # Show the expiry dates processed
                expiry_list = ", ".join([d.strftime('%Y-%m-%d') for d in sorted(output_files.keys())])
                st.info(f"Expiry dates processed: {expiry_list}")
            else:
                st.warning("No expiry delivery files were generated. Check if positions have valid expiry dates.")

    except Exception as e:
        st.error(f"❌ Error generating expiry deliveries: {str(e)}")
        st.code(traceback.format_exc())
        logger.error(traceback.format_exc())


def run_pms_reconciliation(pms_file, position_file=None, position_password=None, mapping_file=None, use_default_mapping=None, default_mapping=None):
    """
    Run PMS reconciliation

    Two modes:
    1. Position + PMS only: Simple reconciliation (current positions vs PMS)
    2. Position + Clearing + PMS: Complex reconciliation (pre-trade & post-trade vs PMS)
    """
    if not NEW_FEATURES_AVAILABLE:
        st.error("Reconciliation module not available")
        return

    try:
        temp_dir = get_temp_dir()

        # Check if we have Stage 1 data (Position + Clearing scenario)
        has_stage1_data = ('dataframes' in st.session_state and
                          'stage1' in st.session_state.dataframes and
                          st.session_state.dataframes['stage1'].get('starting_positions') is not None)

        with st.spinner("Running PMS reconciliation..."):
            # Read PMS file
            with tempfile.NamedTemporaryFile(delete=False, suffix=Path(pms_file.name).suffix, dir=temp_dir) as tmp:
                tmp.write(pms_file.getbuffer())
                pms_path = tmp.name

            recon = EnhancedReconciliation()
            pms_df = recon.read_pms_file(pms_path)

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

            if has_stage1_data:
                # COMPLEX MODE: Pre-trade and Post-trade reconciliation
                st.info("📊 Running pre-trade and post-trade reconciliation...")

                stage1_data = st.session_state.dataframes['stage1']
                starting_positions = stage1_data.get('starting_positions', pd.DataFrame())
                final_positions = stage1_data.get('final_positions', pd.DataFrame())

                # Get account prefix if available
                account_prefix = ""
                if st.session_state.get('account_validator'):
                    account_prefix = st.session_state.account_validator.get_account_prefix()

                output_file = get_output_path(f"{account_prefix}PMS_RECONCILIATION_{timestamp}.xlsx")

                recon.create_comprehensive_recon_report(
                    starting_positions,
                    final_positions,
                    pms_df,
                    output_file
                )

                pre_recon = recon.reconcile_positions(starting_positions, pms_df, "Pre-Trade")
                post_recon = recon.reconcile_positions(final_positions, pms_df, "Post-Trade")

                st.session_state.recon_data = {
                    'pre_trade': pre_recon,
                    'post_trade': post_recon,
                    'pms_df': pms_df
                }

                st.success(f"✅ Pre-trade and Post-trade reconciliation complete!")

            else:
                # SIMPLE MODE: Just reconcile current position file vs PMS
                st.info("📊 Running position reconciliation against PMS...")

                # Parse the position file
                if not position_file:
                    st.error("Position file required for PMS reconciliation")
                    return

                # Save position file to temp
                with tempfile.NamedTemporaryFile(delete=False, suffix=Path(position_file.name).suffix, dir=temp_dir) as tmp:
                    tmp.write(position_file.getbuffer())
                    pos_path = tmp.name

                # Get mapping file path
                if mapping_file:
                    with tempfile.NamedTemporaryFile(delete=False, suffix='.csv', dir=temp_dir) as tmp:
                        tmp.write(mapping_file.getbuffer())
                        map_path = tmp.name
                else:
                    map_path = default_mapping if default_mapping else "futures mapping.csv"

                # Parse position file
                from input_parser import InputParser
                input_parser = InputParser(map_path)

                # Handle encrypted position file if needed
                if position_password:
                    from encrypted_file_handler import read_csv_or_excel_with_password
                    success, pos_df, error = read_csv_or_excel_with_password(position_file, position_password)
                    if not success:
                        st.error(f"Failed to decrypt position file: {error}")
                        return
                    # Save decrypted data to temp file
                    suffix = Path(position_file.name).suffix
                    if suffix.lower() in ['.xls', '.xlsx']:
                        suffix = '.xlsx'
                    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix, dir=temp_dir) as tmp:
                        if suffix == '.csv':
                            pos_df.to_csv(tmp.name, index=False)
                        else:
                            pos_df.to_excel(tmp.name, index=False)
                        pos_path = tmp.name

                positions = input_parser.parse_file(pos_path)

                if not positions:
                    st.error("❌ No positions found in position file")
                    return

                st.success(f"✅ Parsed {len(positions)} positions")

                # Convert to DataFrame
                from position_manager import PositionManager
                position_manager = PositionManager()
                current_positions_df = position_manager.initialize_from_positions(positions)

                # Get account prefix if available
                account_prefix = ""
                if st.session_state.get('account_validator'):
                    account_prefix = st.session_state.account_validator.get_account_prefix()

                # Run simple reconciliation
                output_file = get_output_path(f"{account_prefix}PMS_POSITION_RECONCILIATION_{timestamp}.xlsx")

                # Single reconciliation (treat as pre-trade)
                position_recon = recon.reconcile_positions(current_positions_df, pms_df, "Current")

                # Create simple report (just one reconciliation)
                # Use the same report generator but with same data for both pre and post
                recon.create_comprehensive_recon_report(
                    current_positions_df,
                    current_positions_df,  # Same as starting for simple mode
                    pms_df,
                    output_file
                )

                st.session_state.recon_data = {
                    'pre_trade': position_recon,
                    'post_trade': position_recon,  # Same data for display
                    'pms_df': pms_df
                }

                # IMPORTANT: Also populate stage1 data so deliverables can be calculated
                # Even though we didn't run full Stage 1, we have positions that can be used
                if 'dataframes' not in st.session_state:
                    st.session_state.dataframes = {}

                st.session_state.dataframes['stage1'] = {
                    'starting_positions': current_positions_df,
                    'final_positions': current_positions_df,  # Same for simple mode
                    'processed_trades': None  # No trades in simple mode
                }

                # Mark stage1 as complete so deliverables can run
                st.session_state.stage1_complete = True

                st.success(f"✅ Position reconciliation complete!")

            st.session_state.recon_file = output_file
            st.session_state.recon_complete = True

            # Cleanup
            try:
                os.unlink(pms_path)
            except:
                pass

    except Exception as e:
        st.error(f"❌ Error in reconciliation: {str(e)}")
        st.code(traceback.format_exc())
        logger.error(traceback.format_exc())


def run_broker_reconciliation(trade_file, broker_files, mapping_file, account_prefix=""):
    """Run broker reconciliation to match clearing with executing broker trades"""
    if not BROKER_RECON_AVAILABLE:
        st.error("Broker reconciliation module not available")
        return False

    try:
        if not broker_files or len(broker_files) == 0:
            st.warning("No broker files uploaded")
            return False

        with st.spinner("🏦 Running broker reconciliation..."):
            # Get futures mapping path
            if mapping_file:
                temp_dir = get_temp_dir()
                with tempfile.NamedTemporaryFile(delete=False, suffix='.csv', dir=temp_dir) as tmp:
                    tmp.write(mapping_file.getbuffer())
                    mapping_path = tmp.name
            else:
                mapping_path = "futures mapping.csv"

            # Create reconciler with proper output directory
            # Use ./output for desktop, temp for cloud
            output_base = "./output" if not is_streamlit_cloud() else str(get_temp_dir())

            # Ensure output directory exists
            Path(output_base).mkdir(exist_ok=True)

            st.info(f"🗂️ Output directory: {Path(output_base).absolute()}")
            reconciler = TradeReconciler(output_dir=output_base, account_prefix=account_prefix)

            # Set trade date from Stage 1 if available, otherwise extract from clearing file
            trade_date_str = None
            if hasattr(st.session_state, 'stage1_outputs') and st.session_state.get('dataframes', {}).get('stage1'):
                processed_trades_df = st.session_state['dataframes']['stage1'].get('processed_trades')
                if processed_trades_df is not None and not processed_trades_df.empty:
                    from output_generator import OutputGenerator
                    temp_gen = OutputGenerator()
                    trade_date_str = temp_gen._extract_trade_date(processed_trades_df)

            # If no Stage 1 data, extract trade date directly from clearing file
            if not trade_date_str:
                try:
                    # Parse clearing file to get trade date
                    from Trade_Parser import Trade_Parser
                    temp_parser = Trade_Parser()

                    # Reset file pointer to beginning before reading
                    trade_file.seek(0)

                    # Save clearing file temporarily
                    temp_dir = get_temp_dir()
                    suffix = '.csv' if trade_file.name.endswith('.csv') else '.xlsx'
                    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix, dir=temp_dir) as tmp:
                        tmp.write(trade_file.read())
                        temp_clearing_path = tmp.name

                    # Reset file pointer again for later use
                    trade_file.seek(0)

                    # Parse to get trades
                    logger.info(f"Parsing clearing file to extract trade date: {temp_clearing_path}")
                    trades = temp_parser.parse_trade_file(temp_clearing_path)
                    logger.info(f"Parsed {len(trades) if trades else 0} trades from clearing file")

                    if trades and len(trades) > 0:
                        # Extract date from first trade
                        first_trade = trades[0]
                        if hasattr(first_trade, 'trade_date') and first_trade.trade_date:
                            trade_date_str = first_trade.trade_date.strftime("%d-%b-%Y")
                            logger.info(f"✓ Extracted trade date from clearing file: {trade_date_str}")
                        else:
                            logger.warning(f"First trade has no trade_date attribute")
                    else:
                        logger.warning(f"No trades parsed from clearing file")

                    # Cleanup temp file
                    try:
                        os.unlink(temp_clearing_path)
                    except:
                        pass
                except Exception as e:
                    logger.error(f"Could not extract trade date from clearing file: {e}")
                    import traceback
                    logger.error(traceback.format_exc())

            # Set the trade date if we found it
            if trade_date_str:
                reconciler.set_trade_date(trade_date_str)

            # Run reconciliation
            result = reconciler.reconcile(
                clearing_file=trade_file,
                broker_files=broker_files,
                futures_mapping_file=mapping_path
            )

            if result['success']:
                st.session_state.broker_recon_complete = True
                st.session_state.broker_recon_result = result

                # Display summary
                st.success(f"✅ Broker reconciliation complete!")
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Matched Trades", result['matched_count'])
                with col2:
                    st.metric("Match Rate", f"{result['match_rate']:.1f}%")
                with col3:
                    st.metric("Unmatched", result['unmatched_clearing_count'])

                # Store file paths for download (use same pattern as other features)
                enhanced_file = result.get('enhanced_clearing_file')
                recon_report = result.get('reconciliation_report')

                if enhanced_file:
                    st.session_state.enhanced_clearing_file = enhanced_file
                    if Path(enhanced_file).exists():
                        st.info(f"✓ Enhanced clearing file: {Path(enhanced_file).name}")
                    else:
                        st.warning(f"⚠️ Enhanced clearing file path exists but file not found: {enhanced_file}")

                if recon_report:
                    st.session_state.broker_recon_report = recon_report
                    if Path(recon_report).exists():
                        st.info(f"✓ Reconciliation report: {Path(recon_report).name}")
                    else:
                        st.warning(f"⚠️ Recon report path exists but file not found: {recon_report}")

                if not enhanced_file and not recon_report:
                    st.warning("⚠️ No output files generated. Check logs for errors.")

                # Show where to download
                st.info("📥 Download files from the **Downloads** tab → **Enhanced Reports** section")

                return True
            else:
                st.error(f"❌ Reconciliation failed: {result.get('error', 'Unknown error')}")
                return False

    except Exception as e:
        st.error(f"❌ Error in broker reconciliation: {str(e)}")
        logger.error(traceback.format_exc())
        return False
