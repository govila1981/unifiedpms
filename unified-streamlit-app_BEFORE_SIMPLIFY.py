"""
Enhanced Unified Trade Processing Pipeline - REFACTORED & MODULAR
Complete with proper viewing and downloading of expiry delivery files
"""

import streamlit as st
import pandas as pd
from pathlib import Path
import logging
from datetime import datetime
import io

# Import our new modular components
from app_utils import (
    is_streamlit_cloud, ensure_directories, get_temp_dir, 
    initialize_session_state, apply_custom_css
)
from app_processing import (
    process_stage1, process_stage2,
    run_deliverables_calculation, run_expiry_delivery_generation,
    run_pms_reconciliation, run_broker_reconciliation
)
from app_display import (
    display_pipeline_overview, display_stage1_results, display_stage2_results,
    display_positions_grouped, display_deliverables_tab, display_expiry_deliveries_tab,
    display_reconciliation_tab, display_downloads, display_broker_reconciliation_tab,
    display_email_reports_tab, display_schema_info
)

# Import existing modules from root directory
try:
    from account_validator import AccountValidator
    ACCOUNT_VALIDATION_AVAILABLE = True
except ImportError:
    ACCOUNT_VALIDATION_AVAILABLE = False

try:
    from simple_price_manager import get_price_manager
    SIMPLE_PRICE_MANAGER_AVAILABLE = True
except ImportError:
    SIMPLE_PRICE_MANAGER_AVAILABLE = False

try:
    from encrypted_file_handler import is_encrypted_excel, read_csv_or_excel_with_password, try_known_passwords
    ENCRYPTED_FILE_SUPPORT = True
except ImportError:
    ENCRYPTED_FILE_SUPPORT = False

try:
    from positions_grouper import PositionGrouper
    POSITION_GROUPER_AVAILABLE = True
except ImportError:
    POSITION_GROUPER_AVAILABLE = False

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
    from email_config import EmailConfig
    from email_sender import EmailSender
    EMAIL_AVAILABLE = True
except ImportError:
    EMAIL_AVAILABLE = False

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Page config
st.set_page_config(
    page_title="Trade Processing Pipeline - Complete Edition",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Apply custom CSS
apply_custom_css()

# Ensure directories exist
ensure_directories()

def main():
    st.title("🎯 Enhanced Trade Processing Pipeline - Complete Edition")
    st.markdown("### Comprehensive pipeline with strategy processing, deliverables, reconciliation, and expiry physical delivery")

    # Initialize price manager on first load
    if SIMPLE_PRICE_MANAGER_AVAILABLE and 'prices_initialized' not in st.session_state:
        with st.spinner("Initializing price data..."):
            pm = get_price_manager()

            # Load default stocks
            if pm.load_default_stocks():
                st.session_state.prices_initialized = True
                st.session_state.price_manager = pm
            else:
                st.error("Failed to load default stocks file")
                st.session_state.prices_initialized = False
    
    # Initialize session state
    if 'stage1_complete' not in st.session_state:
        st.session_state.stage1_complete = False
    if 'stage2_complete' not in st.session_state:
        st.session_state.stage2_complete = False
    if 'stage1_outputs' not in st.session_state:
        st.session_state.stage1_outputs = {}
    if 'stage2_outputs' not in st.session_state:
        st.session_state.stage2_outputs = {}
    if 'dataframes' not in st.session_state:
        st.session_state.dataframes = {}
    if 'acm_mapper' not in st.session_state:
        st.session_state.acm_mapper = None
    if 'deliverables_complete' not in st.session_state:
        st.session_state.deliverables_complete = False
    if 'recon_complete' not in st.session_state:
        st.session_state.recon_complete = False
    if 'deliverables_data' not in st.session_state:
        st.session_state.deliverables_data = {}
    if 'recon_data' not in st.session_state:
        st.session_state.recon_data = {}
    if 'expiry_deliveries_complete' not in st.session_state:
        st.session_state.expiry_deliveries_complete = False
    if 'expiry_delivery_files' not in st.session_state:
        st.session_state.expiry_delivery_files = {}
    if 'expiry_delivery_results' not in st.session_state:
        st.session_state.expiry_delivery_results = {}

    # Cache for sticky file behavior
    if 'cached_position_file' not in st.session_state:
        st.session_state.cached_position_file = None
    if 'cached_mapping_file' not in st.session_state:
        st.session_state.cached_mapping_file = None
    if 'cached_position_password' not in st.session_state:
        st.session_state.cached_position_password = None

    # Account validation state
    if 'account_validator' not in st.session_state:
        st.session_state.account_validator = AccountValidator() if ACCOUNT_VALIDATION_AVAILABLE else None
    if 'detected_account' not in st.session_state:
        st.session_state.detected_account = None
    if 'account_validated' not in st.session_state:
        st.session_state.account_validated = False

    # Processing mode state
    if 'processing_mode' not in st.session_state:
        st.session_state.processing_mode = 'EOD'
    if 'broker_recon_complete' not in st.session_state:
        st.session_state.broker_recon_complete = False
    if 'enhanced_clearing_file' not in st.session_state:
        st.session_state.enhanced_clearing_file = None
    if 'final_enhanced_clearing_file' not in st.session_state:
        st.session_state.final_enhanced_clearing_file = None
    if 'broker_recon_result' not in st.session_state:
        st.session_state.broker_recon_result = None

    # Sidebar
    with st.sidebar:
        st.header("⚙️ Processing Mode")

        # Mode selector
        processing_mode = st.radio(
            "Select mode:",
            ["EOD (with broker reconciliation)", "Intraday (direct trades)"],
            index=0 if st.session_state.processing_mode == 'EOD' else 1,
            help="**EOD**: Full reconciliation with executing broker files (includes Comms/Taxes)\n\n**Intraday**: Quick processing with clearing file only (no Comms/Taxes)"
        )

        # Update session state
        st.session_state.processing_mode = 'EOD' if 'EOD' in processing_mode else 'Intraday'

        # Show mode description
        if st.session_state.processing_mode == 'EOD':
            st.info("📊 **EOD Mode**: Reconciles clearing with executing broker files. Outputs include commission and tax data.")
        else:
            st.info("⚡ **Intraday Mode**: Fast processing without broker reconciliation. No commission/tax data.")

        st.divider()
        st.header("📂 Input Files")

        # Display detected account (if any) at top of sidebar
        if ACCOUNT_VALIDATION_AVAILABLE and st.session_state.detected_account:
            account = st.session_state.detected_account
            st.markdown(
                f"""
                <div style="background-color: {account['display_color']}15;
                            border: 2px solid {account['display_color']};
                            border-radius: 8px;
                            padding: 12px;
                            margin-bottom: 20px;">
                    <div style="font-size: 24px; text-align: center;">{account['icon']}</div>
                    <div style="font-weight: bold; font-size: 18px; text-align: center; color: {account['display_color']};">
                        {account['name']}
                    </div>
                    <div style="font-size: 12px; text-align: center; color: #666; margin-top: 4px;">
                        CP: {account['cp_code']}
                    </div>
                </div>
                """,
                unsafe_allow_html=True
            )

        st.divider()
        
        # Stage 1 Section
        st.markdown("### Stage 1: Strategy Processing")
        
        position_file = st.file_uploader(
            "1. Position File",
            type=['xlsx', 'xls', 'csv'],
            key='position_file',
            help="BOD, Contract, or MS format"
        )

        # Cache position file when uploaded
        if position_file is not None:
            st.session_state.cached_position_file = position_file

        # Show cached file info if no new upload
        if position_file is None and st.session_state.cached_position_file is not None:
            st.info(f"✓ Using cached: {st.session_state.cached_position_file.name}")
            position_file = st.session_state.cached_position_file

        # Password field for position file (collect BEFORE account detection)
        position_password = None
        position_file_encrypted = False
        if position_file and ENCRYPTED_FILE_SUPPORT:
            # For Excel files, always try known passwords first
            if position_file.name.endswith(('.xlsx', '.xls')):
                # Pass UploadedFile DIRECTLY - same as Stage 2 processing
                position_file.seek(0)
                auto_password = try_known_passwords(position_file)
                position_file.seek(0)

                if auto_password:
                    # Known password worked! File was encrypted
                    position_file_encrypted = True
                    position_password = auto_password
                    st.session_state.cached_position_password = auto_password
                    st.success(f"✓ Decrypted position file automatically")
                elif st.session_state.get('cached_position_password'):
                    # Use cached password from previous attempt
                    position_file_encrypted = True
                    position_password = st.session_state.cached_position_password
                elif is_encrypted_excel(position_file):
                    # File is encrypted but known passwords failed
                    position_file_encrypted = True
                    position_file.seek(0)
                    # Prompt user for password
                    position_password = st.text_input(
                        "Position file password:",
                        type="password",
                        key="position_password",
                        help="This file is encrypted. Known passwords failed, please enter password."
                    )
                    if position_password:
                        st.session_state.cached_position_password = position_password

        # Detect account in position file (validator tries known passwords automatically)
        if position_file is not None and ACCOUNT_VALIDATION_AVAILABLE and st.session_state.account_validator:
            pos_account = st.session_state.account_validator.detect_account_in_position_file(position_file)
            if pos_account:
                st.session_state.detected_account = pos_account

        # Conditional file uploaders based on mode
        if st.session_state.processing_mode == 'Intraday':
            # Intraday mode: Just clearing file
            trade_file = st.file_uploader(
                "2. Clearing Trade File",
                type=['xlsx', 'xls', 'csv'],
                key='trade_file',
                help="Clearing broker trade file (MS format)"
            )
            broker_files = None
        else:
            # EOD mode: Clearing file + Broker files
            clearing_file = st.file_uploader(
                "2. Clearing Trade File",
                type=['xlsx', 'xls', 'csv'],
                key='clearing_file_eod',
                help="Clearing broker trade file"
            )

            broker_files = st.file_uploader(
                "3. Executing Broker Files",
                type=['xlsx', 'xls', 'csv'],
                accept_multiple_files=True,
                key='broker_files',
                help="One or more broker files (ICICI, Kotak, Morgan Stanley, etc.)"
            )

            # For consistency in later code, set trade_file to clearing_file in EOD mode
            # (will be replaced with enhanced file after reconciliation)
            trade_file = clearing_file

        # Check if trade file is encrypted and get password if needed
        trade_password = None
        trade_file_encrypted = False

        if trade_file and ENCRYPTED_FILE_SUPPORT:
            # For Excel files, always try known passwords first
            if trade_file.name.endswith(('.xlsx', '.xls')):
                # Pass UploadedFile DIRECTLY - same as Stage 2 processing
                trade_file.seek(0)
                auto_password = try_known_passwords(trade_file)
                trade_file.seek(0)

                if auto_password:
                    # Known password worked! File was encrypted
                    trade_file_encrypted = True
                    trade_password = auto_password
                    st.session_state.cached_trade_password = auto_password
                    st.success(f"✓ Decrypted trade file automatically")
                elif st.session_state.get('cached_trade_password'):
                    # Use cached password from previous attempt
                    trade_file_encrypted = True
                    trade_password = st.session_state.cached_trade_password
                elif is_encrypted_excel(trade_file):
                    # File is encrypted but known passwords failed
                    trade_file_encrypted = True
                    trade_file.seek(0)
                    # Prompt user for password
                    trade_password = st.text_input(
                        "Trade file password:",
                        type="password",
                        key="trade_password",
                        help="This file is encrypted. Known passwords failed, please enter password."
                    )
                    # Cache password for reuse during processing
                    if trade_password:
                        st.session_state.cached_trade_password = trade_password

        # Detect account in trade file and validate (validator tries known passwords automatically)
        if trade_file is not None and ACCOUNT_VALIDATION_AVAILABLE and st.session_state.account_validator:
            trade_account = st.session_state.account_validator.detect_account_in_trade_file(trade_file)

            # If position file account was detected, validate match
            if st.session_state.account_validator.position_account or trade_account:
                is_valid, status_type, message = st.session_state.account_validator.validate_account_match()

                # Update detected account
                if st.session_state.account_validator.get_account_info():
                    st.session_state.detected_account = st.session_state.account_validator.get_account_info()

                st.session_state.account_validated = is_valid

                # Display validation message
                if status_type == "success":
                    st.success(message)
                elif status_type == "warning":
                    st.warning(message)
                elif status_type == "error":
                    st.error(message)
        
        # Mapping file numbering depends on mode
        mapping_number = "3" if st.session_state.processing_mode == 'Intraday' else "4"
        st.subheader(f"{mapping_number}. Mapping File")
        default_mapping = None
        
        mapping_locations = [
            "futures_mapping.csv",
            "futures mapping.csv",
            "data/futures_mapping.csv",
            "data/futures mapping.csv",
            "./futures_mapping.csv",
            "./data/futures_mapping.csv"
        ]
        
        for location in mapping_locations:
            if Path(location).exists():
                default_mapping = location
                break
        
        if default_mapping:
            use_default_mapping = st.radio(
                "Mapping source:",
                ["Use default from repository", "Upload custom"],
                index=0,
                key="mapping_radio"
            )
            
            if use_default_mapping == "Upload custom":
                mapping_file = st.file_uploader(
                    "Upload Mapping File",
                    type=['csv'],
                    key='mapping_file'
                )
                if mapping_file is not None:
                    st.session_state.cached_mapping_file = mapping_file
                elif st.session_state.cached_mapping_file is not None:
                    st.info(f"✓ Using cached: {st.session_state.cached_mapping_file.name}")
                    mapping_file = st.session_state.cached_mapping_file
            else:
                mapping_file = None
                st.success(f"✔ Using {Path(default_mapping).name}")
        else:
            st.warning("Upload mapping file (required)")
            mapping_file = st.file_uploader(
                "Upload Mapping File",
                type=['csv'],
                key='mapping_file',
                help="CSV file with symbol-to-ticker mappings"
            )
            use_default_mapping = None
        
        st.divider()

        # USD/INR Rate
        st.markdown("### 💱 Exchange Rate")
        usdinr_rate = st.number_input(
            "USD/INR Rate",
            min_value=50.0,
            max_value=150.0,
            value=88.0,
            step=0.1,
            key="usdinr_rate"
        )

        st.divider()

        # Centralized Price Management (Single source of truth)
        if SIMPLE_PRICE_MANAGER_AVAILABLE:
            st.markdown("### 💰 Price Management")

            # Initialize price manager if not exists
            if 'price_manager' not in st.session_state:
                pm = get_price_manager()
                pm.load_default_stocks()  # Load symbol mappings
                st.session_state.price_manager = pm

            # Price status
            pm = st.session_state.price_manager
            if pm.price_source != "Not initialized":
                st.info(f"Price Source: {pm.price_source}")

                # Show missing symbols if any
                if pm.missing_symbols:
                    with st.expander(f"⚠️ {len(pm.missing_symbols)} Missing Symbols", expanded=False):
                        missing_df = pm.get_missing_symbols_report()
                        if not missing_df.empty:
                            st.dataframe(missing_df, use_container_width=True)

            col1, col2 = st.columns(2)

            with col1:
                if st.button("📊 Fetch Yahoo Prices", use_container_width=True):
                    with st.spinner("Fetching prices from Yahoo Finance..."):
                        progress = st.progress(0)

                        def update_progress(current, total):
                            progress.progress(current / total)

                        pm.fetch_all_prices_yahoo(update_progress)
                        st.session_state.price_manager = pm
                        st.success(f"✓ {pm.price_source}")
                        st.rerun()

            # Show download button if prices have been fetched
            if pm.price_source and "Yahoo Finance" in pm.price_source:
                updated_csv = pm.get_updated_csv_dataframe()
                if updated_csv is not None:
                    st.info("💾 Prices updated! Download CSV to save changes:")
                    csv_data = updated_csv.to_csv(index=False)
                    st.download_button(
                        label="📥 Download Updated default_stocks.csv",
                        data=csv_data,
                        file_name="default_stocks_updated.csv",
                        mime="text/csv",
                        use_container_width=True,
                        help="Download this file and replace your default_stocks.csv to persist the updated prices"
                    )

            with col2:
                price_file = st.file_uploader(
                    "Or Upload Prices",
                    type=['csv', 'xlsx'],
                    key="price_upload",
                    help="CSV/Excel with Symbol/Ticker and Price columns"
                )

                if price_file:
                    # Check if file is encrypted
                    price_password = None
                    if ENCRYPTED_FILE_SUPPORT and price_file.name.endswith(('.xlsx', '.xls')):
                        file_bytes = price_file.read()
                        price_file.seek(0)
                        if is_encrypted_excel(io.BytesIO(file_bytes)):
                            price_password = st.text_input(
                                "Price file password:",
                                type="password",
                                key="price_password",
                                help="This file is encrypted. Please enter the password."
                            )

                    # Read the file (with decryption if needed)
                    if ENCRYPTED_FILE_SUPPORT and price_password:
                        success, price_df, error = read_csv_or_excel_with_password(price_file, price_password)
                        if not success:
                            st.error(f"Failed to read price file: {error}")
                        else:
                            if pm.load_manual_prices(price_df):
                                st.session_state.price_manager = pm
                                st.success(f"✓ {pm.price_source}")
                                st.rerun()
                            else:
                                st.error("Failed to load price data")
                    else:
                        # Read normally
                        try:
                            if price_file.name.endswith('.csv'):
                                price_df = pd.read_csv(price_file)
                            else:
                                price_df = pd.read_excel(price_file)

                            if pm.load_manual_prices(price_df):
                                st.session_state.price_manager = pm
                                st.success(f"✓ {pm.price_source}")
                                st.rerun()
                            else:
                                st.error("Failed to load price file")
                        except Exception as e:
                            st.error(f"Error reading price file: {str(e)}")
        
        # PMS Reconciliation option (only optional feature)
        st.markdown("### 🔄 PMS Reconciliation (Optional)")
        enable_recon = st.checkbox(
            "Enable PMS Reconciliation",
            value=False,
            key="enable_recon",
            help="Compare positions with PMS file"
        )
        
        pms_file = None
        if enable_recon:
            pms_file = st.file_uploader(
                "Upload PMS Position File",
                type=['xlsx', 'xls', 'csv'],
                key='pms_file'
            )

        st.divider()
        
        # Process buttons
        # Check if we can process (files + account validation + broker recon for EOD)
        base_files_ready = (
            position_file is not None and
            trade_file is not None and
            (mapping_file is not None or (use_default_mapping == "Use default from repository" and default_mapping)) and
            (not ACCOUNT_VALIDATION_AVAILABLE or st.session_state.get('account_validated', True))
        )

        # EOD mode requires broker reconciliation with 100% match
        if st.session_state.processing_mode == 'EOD':
            if broker_files and trade_file:
                # Broker reconciliation button
                st.markdown("### 🏦 Step 1: Broker Reconciliation (Required)")
                if st.button("▶️ Run Broker Reconciliation", type="primary", use_container_width=True):
                    account_prefix = ""
                    if ACCOUNT_VALIDATION_AVAILABLE and st.session_state.account_validator:
                        account_prefix = st.session_state.account_validator.get_account_prefix()

                    if BROKER_RECON_AVAILABLE:
                        run_broker_reconciliation(trade_file, broker_files, mapping_file, account_prefix)
                    else:
                        st.error("Broker reconciliation module not available")

                # Check if reconciliation is complete and 100%
                if st.session_state.broker_recon_complete:
                    recon_result = st.session_state.broker_recon_result
                    if recon_result and recon_result.get('match_rate') == 100:
                        st.success(f"✅ Broker reconciliation complete: {recon_result['match_rate']:.1f}% match")
                        can_process_stage1 = base_files_ready
                    else:
                        match_rate = recon_result.get('match_rate', 0) if recon_result else 0
                        st.error(f"⚠️ Reconciliation incomplete: {match_rate:.1f}% matched")
                        st.warning("""
                        **Options:**
                        1. Fix your files and re-run reconciliation
                        2. Switch to Intraday mode (won't have Comms/Taxes data)
                        """)
                        can_process_stage1 = False
                else:
                    st.info("👆 Run broker reconciliation first to proceed")
                    can_process_stage1 = False
            else:
                st.warning("Upload clearing file and broker files to proceed")
                can_process_stage1 = False
        else:
            # Intraday mode - no broker recon needed
            can_process_stage1 = base_files_ready

        can_process_stage2 = st.session_state.stage1_complete

        st.markdown("### 📊 Stage Processing")
        col1, col2 = st.columns(2)

        with col1:
            if st.button("🚀 Run Stage 1", type="primary", use_container_width=True, disabled=not can_process_stage1):
                # Get account prefix
                account_prefix = ""
                if ACCOUNT_VALIDATION_AVAILABLE and st.session_state.account_validator:
                    account_prefix = st.session_state.account_validator.get_account_prefix()

                # In EOD mode, use enhanced clearing file
                trade_file_to_use = trade_file
                if st.session_state.processing_mode == 'EOD' and st.session_state.broker_recon_complete:
                    enhanced_file_path = st.session_state.enhanced_clearing_file
                    if enhanced_file_path and Path(enhanced_file_path).exists():
                        with open(enhanced_file_path, 'rb') as f:
                            trade_file_to_use = io.BytesIO(f.read())
                            trade_file_to_use.name = Path(enhanced_file_path).name
                        st.info("Using enhanced clearing file (with Comms, Taxes, TD)")

                process_stage1(position_file, trade_file_to_use, mapping_file, use_default_mapping, default_mapping,
                             position_password, trade_password, account_prefix)
        
        with col2:
            if st.button("🎯 Run Stage 2", type="secondary", use_container_width=True, disabled=not can_process_stage2):
                process_stage2("Use built-in schema (default)", None)
        
        # Complete pipeline button
        if can_process_stage1:
            st.divider()
            if st.button("⚡ Run Complete Enhanced Pipeline", type="primary", use_container_width=True):
                # Get account prefix
                account_prefix = ""
                if ACCOUNT_VALIDATION_AVAILABLE and st.session_state.account_validator:
                    account_prefix = st.session_state.account_validator.get_account_prefix()

                # Step 1: Use enhanced file if in EOD mode and broker recon is complete
                trade_file_to_use = trade_file
                if st.session_state.processing_mode == 'EOD' and st.session_state.broker_recon_complete:
                    recon_result = st.session_state.broker_recon_result
                    if recon_result and recon_result.get('match_rate') == 100:
                        enhanced_file_path = st.session_state.get('enhanced_clearing_file')
                        if enhanced_file_path and Path(enhanced_file_path).exists():
                            st.info("🎯 Using enhanced clearing file (with Comms, Taxes, TD) for all processing...")

                            # Load enhanced clearing file
                            with open(enhanced_file_path, 'rb') as f:
                                trade_file_to_use = io.BytesIO(f.read())
                                trade_file_to_use.name = Path(enhanced_file_path).name

                # Step 2: Run Stage 1 (with enhanced file if available)
                if process_stage1(position_file, trade_file_to_use, mapping_file, use_default_mapping, default_mapping,
                                position_password, trade_password, account_prefix):
                    # Run Stage 2
                    process_stage2("Use built-in schema (default)", None, account_prefix)

                    # Show success if using enhanced file
                    if trade_file_to_use != trade_file:
                        st.success("✅ ACM output now includes Trade Date, Brokerage & Taxes from broker reconciliation!")

                    # Run deliverables (always enabled)
                    run_deliverables_calculation(st.session_state.get('usdinr_rate', 88.0), account_prefix)

                    # Run expiry deliveries (always enabled)
                    if EXPIRY_DELIVERY_AVAILABLE:
                        run_expiry_delivery_generation(account_prefix)

                    # Run PMS recon if enabled
                    if enable_recon and pms_file:
                        run_pms_reconciliation(pms_file)

                    st.success("✅ Complete enhanced pipeline finished!")
                    st.balloons()
        
        # Separate button for expiry deliveries if Stage 1 is complete
        if EXPIRY_DELIVERY_AVAILABLE and st.session_state.get('stage1_complete', False):
            st.divider()
            if st.button("📅 Regenerate Expiry Deliveries", type="secondary", use_container_width=True):
                run_expiry_delivery_generation()

        st.divider()
        if st.button("🔄 Reset All", type="secondary", use_container_width=True):
            for key in list(st.session_state.keys()):
                del st.session_state[key]
            st.rerun()

        # Email Settings
        st.divider()
        st.header("📧 Email Notifications")

        # Check if email module is available
        if not EMAIL_AVAILABLE:
            st.warning("Email not available")
            with st.expander("ℹ️ Setup Instructions"):
                st.markdown("""
                To enable email notifications:

                ```bash
                pip install sendgrid python-dotenv
                ```

                Then restart the Streamlit app.
                """)
        else:
            # Check if email is configured from Streamlit secrets
            try:
                email_config = EmailConfig.from_streamlit_secrets()
                email_configured = email_config.is_configured()
            except:
                email_configured = False

        if EMAIL_AVAILABLE and email_configured:
            st.success("✓ Email configured")
            st.info("📧 Configure email reports in the **Email Reports** tab")
        elif EMAIL_AVAILABLE and not email_configured:
            st.warning("⚠️ Email not configured")
            with st.expander("ℹ️ Setup Instructions"):
                st.markdown("""
                To enable email notifications, configure Streamlit secrets:

                1. Create a `.streamlit/secrets.toml` file in your project directory
                2. Add the following:

                ```toml
                SENDGRID_API_KEY = "your_api_key_here"
                SENDGRID_FROM_EMAIL = "agovil@aurigincm.com"
                SENDGRID_FROM_NAME = "Aurigin Trade Processing"
                ```

                3. Restart the Streamlit app

                **For Streamlit Cloud:**
                - Go to your app settings
                - Navigate to "Secrets" section
                - Add the same configuration in TOML format
                """)

    # Main content tabs
    tab_list = ["📊 Pipeline Overview", "🔄 Stage 1: Strategy", "📋 Stage 2: ACM"]

    # Add Positions by Underlying tab if we have positions
    if POSITION_GROUPER_AVAILABLE and 'stage1' in st.session_state.dataframes:
        tab_list.append("📂 Positions by Underlying")

    # Always show deliverables and expiry deliveries tabs (always enabled)
    if NEW_FEATURES_AVAILABLE and st.session_state.stage1_complete:
        tab_list.append("💰 Deliverables & IV")

    if EXPIRY_DELIVERY_AVAILABLE and st.session_state.stage1_complete:
        tab_list.append("📅 Expiry Deliveries")

    # Only show PMS recon if enabled
    if NEW_FEATURES_AVAILABLE and st.session_state.get('enable_recon', False):
        tab_list.append("🔄 PMS Reconciliation")

    # Show Broker Recon tab if reconciliation completed
    if BROKER_RECON_AVAILABLE and st.session_state.get('broker_recon_complete', False):
        tab_list.append("🏦 Broker Reconciliation")

    # Show Email Reports tab if email is configured and stage1 is complete
    if EMAIL_AVAILABLE and st.session_state.get('stage1_complete', False):
        tab_list.append("📧 Email Reports")

    tab_list.extend(["📥 Downloads", "📘 Schema Info"])
    
    tabs = st.tabs(tab_list)
    
    tab_index = 0
    with tabs[tab_index]:
        display_pipeline_overview()
    tab_index += 1
    
    with tabs[tab_index]:
        display_stage1_results()
    tab_index += 1
    
    with tabs[tab_index]:
        display_stage2_results()
    tab_index += 1

    # Positions by Underlying tab (if available)
    if POSITION_GROUPER_AVAILABLE and 'stage1' in st.session_state.dataframes:
        with tabs[tab_index]:
            display_positions_grouped()
        tab_index += 1

    # Deliverables tab (always enabled)
    if NEW_FEATURES_AVAILABLE and st.session_state.stage1_complete:
        with tabs[tab_index]:
            display_deliverables_tab()
        tab_index += 1

    # Expiry Deliveries tab (always enabled)
    if EXPIRY_DELIVERY_AVAILABLE and st.session_state.stage1_complete:
        with tabs[tab_index]:
            display_expiry_deliveries_tab()
        tab_index += 1

    # Reconciliation tab (only if enabled)
    if NEW_FEATURES_AVAILABLE and st.session_state.get('enable_recon', False):
        with tabs[tab_index]:
            display_reconciliation_tab()
        tab_index += 1

    # Broker Reconciliation tab (only if completed)
    if BROKER_RECON_AVAILABLE and st.session_state.get('broker_recon_complete', False):
        with tabs[tab_index]:
            display_broker_reconciliation_tab()
        tab_index += 1

    # Email Reports tab (only if email available and stage1 complete)
    if EMAIL_AVAILABLE and st.session_state.get('stage1_complete', False):
        with tabs[tab_index]:
            display_email_reports_tab()
        tab_index += 1

    with tabs[tab_index]:
        display_downloads()
    tab_index += 1
    
    with tabs[tab_index]:
        display_schema_info()

# Processing Functions

# Footer
st.divider()
st.markdown("""
<div style='text-align: center; color: #666;'>
    Enhanced Trade Processing Pipeline v4.0 | Refactored & Modular
</div>
""", unsafe_allow_html=True)

if __name__ == "__main__":
    main()
