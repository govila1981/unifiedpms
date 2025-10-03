"""
Enhanced Unified Trade Processing Pipeline - FIXED VERSION WITH WORKING EXPIRY DELIVERIES
Complete with proper viewing and downloading of expiry delivery files
"""

import streamlit as st
import pandas as pd
from pathlib import Path
import tempfile
import logging
from datetime import datetime
import traceback
import os
import sys
import io

# Import existing modules from root directory
try:
    from input_parser import InputParser
    from Trade_Parser import TradeParser
    from position_manager import PositionManager
    from trade_processor import TradeProcessor
    from output_generator import OutputGenerator
    from acm_mapper import ACMMapper

    # Import enhanced modules
    try:
        from deliverables_calculator import DeliverableCalculator
        from enhanced_recon_module import EnhancedReconciliation
        NEW_FEATURES_AVAILABLE = True
    except ImportError:
        NEW_FEATURES_AVAILABLE = False

    # Import Expiry Delivery Generator
    try:
        from expiry_delivery_module import ExpiryDeliveryGenerator
        EXPIRY_DELIVERY_AVAILABLE = True
    except ImportError:
        EXPIRY_DELIVERY_AVAILABLE = False

    # Import account validation
    try:
        from account_validator import AccountValidator
        from account_config import ACCOUNT_REGISTRY
        ACCOUNT_VALIDATION_AVAILABLE = True
    except ImportError:
        ACCOUNT_VALIDATION_AVAILABLE = False
        logging.warning("Account validation not available")

    # Import broker reconciliation
    try:
        from trade_reconciliation import TradeReconciler
        from broker_config import detect_broker_from_filename, BROKER_REGISTRY
        BROKER_RECON_AVAILABLE = True
    except ImportError:
        BROKER_RECON_AVAILABLE = False
        logging.warning("Broker reconciliation not available")

    # Import Simple Price Manager (single source of truth)
    try:
        from simple_price_manager import get_price_manager, SimplePriceManager
        SIMPLE_PRICE_MANAGER_AVAILABLE = True
    except ImportError:
        SIMPLE_PRICE_MANAGER_AVAILABLE = False
        st.error("SimplePriceManager not available. Price fetching will be limited.")

    # Import Encrypted File Handler
    try:
        from encrypted_file_handler import is_encrypted_excel, read_csv_or_excel_with_password
        ENCRYPTED_FILE_SUPPORT = True
    except ImportError:
        ENCRYPTED_FILE_SUPPORT = False
        logger.warning("Encrypted file handler not available")

    # Import Position Grouper
    try:
        from positions_grouper import PositionGrouper
        POSITION_GROUPER_AVAILABLE = True
    except ImportError:
        POSITION_GROUPER_AVAILABLE = False

    # Import Email functionality
    try:
        from email_config import EmailConfig, get_default_recipients
        from email_sender import EmailSender
        EMAIL_AVAILABLE = True
    except ImportError:
        EMAIL_AVAILABLE = False
        logging.warning("Email functionality not available - install sendgrid to enable")

except ModuleNotFoundError as e:
    st.error(f"Failed to import modules: {e}")
    st.error("Please ensure all module files are in the same directory as this app")
    st.stop()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def is_streamlit_cloud():
    """Check if running on Streamlit Cloud"""
    return os.environ.get("STREAMLIT_RUNTIME_ENV") == "cloud" or \
           os.environ.get("IS_STREAMLIT_CLOUD") == "true" or \
           not os.path.exists(os.path.expanduser("~/.streamlit"))

# Page config
st.set_page_config(
    page_title="Trade Processing Pipeline - Complete Edition",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
    <style>
    .main { padding: 0rem 1rem; }
    h1 { color: #1f77b4; }
    .stDownloadButton button { 
        width: 100%; 
        background-color: #4CAF50; 
        color: white; 
    }
    .stage-header {
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 10px;
        border-radius: 5px;
        margin: 10px 0;
    }
    .warning-box {
        background-color: #fff3cd;
        border: 1px solid #ffc107;
        border-radius: 4px;
        padding: 10px;
        margin: 10px 0;
    }
    .success-box {
        background-color: #d4edda;
        border: 1px solid #c3e6cb;
        border-radius: 4px;
        padding: 10px;
        margin: 10px 0;
    }
    .info-box {
        background-color: #d1ecf1;
        border: 1px solid #bee5eb;
        border-radius: 4px;
        padding: 10px;
        margin: 10px 0;
    }
    .expiry-card {
        background-color: #f8f9fa;
        border: 2px solid #007bff;
        border-radius: 8px;
        padding: 15px;
        margin: 10px 0;
    }
    .deliverable-header {
        background-color: #007bff;
        color: white;
        padding: 8px;
        border-radius: 4px;
        margin: 5px 0;
    }
    </style>
    """, unsafe_allow_html=True)

def ensure_directories():
    """Ensure required directories exist - skip on Streamlit Cloud"""
    if is_streamlit_cloud():
        return  # Skip directory creation on cloud

    dirs = ["output", "output/stage1", "output/stage2", "output/expiry_deliveries", "temp"]
    for dir_path in dirs:
        try:
            Path(dir_path).mkdir(parents=True, exist_ok=True)
        except Exception as e:
            logger.warning(f"Could not create directory {dir_path}: {e}")

ensure_directories()

def get_temp_dir():
    """Get temporary directory that works on both desktop and Streamlit Cloud"""
    # Try system temp directory first
    import tempfile
    temp_system = tempfile.gettempdir()
    if os.access(temp_system, os.W_OK):
        return Path(temp_system)

    # Fallback to /tmp on Unix-like systems
    if Path("/tmp").exists() and os.access("/tmp", os.W_OK):
        return Path("/tmp")

    # Last resort - create local temp directory
    try:
        temp_dir = Path("temp")
        temp_dir.mkdir(exist_ok=True)
        return temp_dir
    except:
        # If all else fails, use current directory
        return Path(".")

def get_output_path(filename: str, subfolder: str = "") -> str:
    """
    Get output file path that works on both desktop and Streamlit Cloud
    On cloud, use temporary directory; on desktop, use output folder
    """
    if is_streamlit_cloud():
        # On cloud, use temp directory
        temp_dir = get_temp_dir()
        if subfolder:
            output_dir = temp_dir / subfolder.replace("output/", "")
        else:
            output_dir = temp_dir
        output_dir.mkdir(parents=True, exist_ok=True)
        return str(output_dir / filename)
    else:
        # On desktop, use regular output folder
        if subfolder:
            output_dir = Path(subfolder)
        else:
            output_dir = Path("output")
        output_dir.mkdir(parents=True, exist_ok=True)
        return str(output_dir / filename)

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

            # Detect account in position file
            if ACCOUNT_VALIDATION_AVAILABLE and st.session_state.account_validator:
                pos_account = st.session_state.account_validator.detect_account_in_position_file(position_file)
                if pos_account:
                    st.session_state.detected_account = pos_account

        # Show cached file info if no new upload
        if position_file is None and st.session_state.cached_position_file is not None:
            st.info(f"✓ Using cached: {st.session_state.cached_position_file.name}")
            position_file = st.session_state.cached_position_file

        # Password field for position file
        position_password = None
        if position_file and ENCRYPTED_FILE_SUPPORT:
            # Check if file is encrypted
            if position_file.name.endswith(('.xlsx', '.xls')):
                file_bytes = position_file.read()
                position_file.seek(0)
                if is_encrypted_excel(io.BytesIO(file_bytes)):
                    position_password = st.text_input(
                        "Position file password:",
                        type="password",
                        key="position_password",
                        help="This file is encrypted. Please enter the password."
                    )
                    if position_password:
                        st.session_state.cached_position_password = position_password
            elif st.session_state.cached_position_password:
                position_password = st.session_state.cached_position_password

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

        # Detect account in trade file and validate
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

        # Password field for trade file
        trade_password = None
        if trade_file and ENCRYPTED_FILE_SUPPORT:
            # Check if file is encrypted
            if trade_file.name.endswith(('.xlsx', '.xls')):
                file_bytes = trade_file.read()
                trade_file.seek(0)
                if is_encrypted_excel(io.BytesIO(file_bytes)):
                    trade_password = st.text_input(
                        "Trade file password:",
                        type="password",
                        key="trade_password",
                        help="This file is encrypted. Please enter the password."
                    )
        
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
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            if is_streamlit_cloud():
                output_dir = get_temp_dir() / "stage1"
                output_dir.mkdir(parents=True, exist_ok=True)
            else:
                output_dir = Path("output/stage1")
                output_dir.mkdir(parents=True, exist_ok=True)

            final_enhanced_file = output_dir / f"{account_prefix}final_enhanced_clearing_{timestamp}.csv"

            # Format date columns as DD/MM/YYYY
            if 'Expiry Dt' in final_enhanced_clearing_df.columns:
                final_enhanced_clearing_df['Expiry Dt'] = pd.to_datetime(final_enhanced_clearing_df['Expiry Dt'], errors='coerce').dt.strftime('%d/%m/%Y')
            if 'TD' in final_enhanced_clearing_df.columns:
                final_enhanced_clearing_df['TD'] = final_enhanced_clearing_df['TD'].apply(
                    lambda x: pd.to_datetime(x, errors='coerce').strftime('%d/%m/%Y') if pd.notna(x) and x != '' else ''
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
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            acm_file = output_dir / f"{account_prefix}acm_listedtrades_{timestamp}.csv"
            mapped_df.to_csv(acm_file, index=False)

            errors_file = output_dir / f"{account_prefix}acm_listedtrades_{timestamp}_errors.csv"
            errors_df.to_csv(errors_file, index=False)

            schema_file = output_dir / f"{account_prefix}acm_schema_used_{timestamp}.xlsx"
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

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = get_output_path(f"{account_prefix}DELIVERABLES_REPORT_{timestamp}.xlsx")
            
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

def run_pms_reconciliation(pms_file):
    """Run PMS reconciliation"""
    if not NEW_FEATURES_AVAILABLE:
        st.error("Reconciliation module not available")
        return
        
    try:
        if 'dataframes' not in st.session_state or 'stage1' not in st.session_state.dataframes:
            st.error("Please complete Stage 1 first")
            return
        
        temp_dir = get_temp_dir()
        
        with st.spinner("Running PMS reconciliation..."):
            with tempfile.NamedTemporaryFile(delete=False, suffix=Path(pms_file.name).suffix, dir=temp_dir) as tmp:
                tmp.write(pms_file.getbuffer())
                pms_path = tmp.name
            
            stage1_data = st.session_state.dataframes['stage1']
            starting_positions = stage1_data.get('starting_positions', pd.DataFrame())
            final_positions = stage1_data.get('final_positions', pd.DataFrame())
            
            recon = EnhancedReconciliation()
            pms_df = recon.read_pms_file(pms_path)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = get_output_path(f"PMS_RECONCILIATION_{timestamp}.xlsx")
            
            recon.create_comprehensive_recon_report(
                starting_positions,
                final_positions,
                pms_df,
                output_file
            )
            
            st.session_state.recon_file = output_file
            st.session_state.recon_complete = True
            
            pre_recon = recon.reconcile_positions(starting_positions, pms_df, "Pre-Trade")
            post_recon = recon.reconcile_positions(final_positions, pms_df, "Post-Trade")
            
            st.session_state.recon_data = {
                'pre_trade': pre_recon,
                'post_trade': post_recon,
                'pms_df': pms_df
            }
            
            st.success(f"✅ Reconciliation complete!")
            
            try:
                os.unlink(pms_path)
            except:
                pass
                
    except Exception as e:
        st.error(f"❌ Error in reconciliation: {str(e)}")
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


# Display Functions

def display_pipeline_overview():
    """Display pipeline overview"""
    st.header("Pipeline Overview")
    
    col1, col2, col3 = st.columns([1, 0.1, 1])
    
    with col1:
        st.markdown('<div class="stage-header">Stage 1: Strategy Processing</div>', unsafe_allow_html=True)
        st.info("""
        **Input:**
        - Position File
        - Trade File
        - Symbol Mapping
        
        **Processing:**
        - Bloomberg ticker generation
        - FULO/FUSH strategy assignment
        - Trade splitting
        - Position tracking
        
        **Output:**
        - Processed trades with strategies
        - Position summaries
        """)
        
        if st.session_state.stage1_complete:
            st.success("✅ Stage 1 Complete")
    
    with col2:
        st.markdown("<br><br><br>", unsafe_allow_html=True)
        st.markdown("## →", unsafe_allow_html=True)
    
    with col3:
        st.markdown('<div class="stage-header">Stage 2: ACM Mapping</div>', unsafe_allow_html=True)
        st.info("""
        **Input:**
        - Processed trades from Stage 1
        - ACM schema
        
        **Processing:**
        - Field mapping
        - Transaction type logic
        - Validation
        
        **Output:**
        - ACM ListedTrades CSV
        - Error report
        """)
        
        if st.session_state.stage2_complete:
            st.success("✅ Stage 2 Complete")
    
    # Additional features overview
    if any([st.session_state.get('enable_deliverables'), 
            st.session_state.get('enable_expiry_delivery'),
            st.session_state.get('enable_recon')]):
        st.markdown("### Enhanced Features Enabled")
        
        cols = st.columns(4)
        feature_idx = 0
        
        if st.session_state.get('enable_deliverables'):
            with cols[feature_idx % 4]:
                st.markdown("**💰 Deliverables/IV**")
                if st.session_state.get('deliverables_complete'):
                    st.success("✅ Complete")
                else:
                    st.info("⏳ Pending")
            feature_idx += 1
        
        if st.session_state.get('enable_expiry_delivery'):
            with cols[feature_idx % 4]:
                st.markdown("**📅 Expiry Deliveries**")
                if st.session_state.get('expiry_deliveries_complete'):
                    files = st.session_state.get('expiry_delivery_files', {})
                    st.success(f"✅ {len(files)} files")
                else:
                    st.info("⏳ Pending")
            feature_idx += 1
        
        if st.session_state.get('enable_recon'):
            with cols[feature_idx % 4]:
                st.markdown("**🔄 PMS Reconciliation**")
                if st.session_state.get('recon_complete'):
                    st.success("✅ Complete")
                else:
                    st.info("⏳ Pending")

def display_stage1_results():
    """Display Stage 1 results"""
    st.header("Stage 1: Strategy Processing Results")
    
    if not st.session_state.stage1_complete:
        st.info("Stage 1 has not been run yet.")
        return
    
    if 'stage1' not in st.session_state.dataframes:
        return
    
    data = st.session_state.dataframes['stage1']
    
    sub_tabs = st.tabs(["Processed Trades", "Starting Positions", "Final Positions", "Parsed Trades"])
    
    with sub_tabs[0]:
        df = data['processed_trades']
        st.subheader("Processed Trades")
        st.dataframe(df, use_container_width=True, height=400)
    
    with sub_tabs[1]:
        df = data['starting_positions']
        st.subheader("Starting Positions")
        st.dataframe(df, use_container_width=True, height=400)
    
    with sub_tabs[2]:
        df = data['final_positions']
        st.subheader("Final Positions")
        st.dataframe(df, use_container_width=True, height=400)
    
    with sub_tabs[3]:
        df = data['parsed_trades']
        st.subheader("Parsed Trades")
        st.dataframe(df, use_container_width=True, height=400)

def display_stage2_results():
    """Display Stage 2 results"""
    st.header("Stage 2: ACM Mapping Results")

    if not st.session_state.stage2_complete:
        st.info("Stage 2 has not been run yet.")
        return
    
    if 'stage2' not in st.session_state.dataframes:
        return
    
    data = st.session_state.dataframes['stage2']
    
    sub_tabs = st.tabs(["ACM Mapped Data", "Validation Errors"])
    
    with sub_tabs[0]:
        df = data['mapped']
        st.subheader("ACM ListedTrades Format")
        st.dataframe(df, use_container_width=True, height=400)
    
    with sub_tabs[1]:
        errors_df = data['errors']
        if len(errors_df) == 0:
            st.success("✅ No validation errors!")
        else:
            st.error(f"⚠️ {len(errors_df)} validation errors")
            st.dataframe(errors_df, use_container_width=True)

def display_positions_grouped():
    """Display positions with sub-tabs for different groupings"""
    st.header("📂 Position Analysis")

    if 'stage1' not in st.session_state.dataframes:
        st.info("No positions data available. Please process files first.")
        return

    # Create sub-tabs
    view_tabs = st.tabs(["By Underlying", "By Expiry", "Pre vs Post"])

    with view_tabs[0]:
        display_positions_by_underlying()

    with view_tabs[1]:
        display_positions_by_expiry()

    with view_tabs[2]:
        display_pre_post_comparison()

def display_positions_by_underlying():
    """Display positions grouped by underlying"""

    if 'stage1' not in st.session_state.dataframes:
        st.info("No positions data available. Please process files first.")
        return

    # Get final positions
    final_positions_df = st.session_state.dataframes['stage1']['final_positions']

    if final_positions_df.empty:
        st.warning("No positions to display.")
        return

    # Debug: Show columns
    with st.expander("Debug: DataFrame Columns"):
        st.write("Columns in final_positions_df:")
        st.write(list(final_positions_df.columns))
        st.write("First row sample:")
        if not final_positions_df.empty:
            st.write(final_positions_df.iloc[0].to_dict())

    # Initialize grouper
    grouper = PositionGrouper()

    # Get price manager for spot prices if available
    price_manager = None
    if SIMPLE_PRICE_MANAGER_AVAILABLE:
        price_manager = get_price_manager()

    # Group positions with price manager for deliverable calculations
    grouped_data = grouper.group_positions_from_dataframe(final_positions_df, price_manager=price_manager)

    # Display options
    col1, col2, col3 = st.columns([2, 1, 1])

    with col1:
        # Sort options
        sort_by = st.selectbox(
            "Sort by",
            ["Underlying (A-Z)", "Net Deliverable", "Net Position", "Total Positions", "Unique Expiries"],
            index=1
        )

    with col2:
        # View options
        view_mode = st.radio(
            "View Mode",
            ["Summary", "Detailed", "Both"],
            index=2,
            horizontal=True
        )

    with col3:
        # Expansion state
        expand_all = st.checkbox("Expand All", value=False)

    # Sort the data based on selection
    sorted_underlyings = sorted(grouped_data.keys())
    if sort_by == "Net Deliverable":
        sorted_underlyings = sorted(grouped_data.keys(),
                                  key=lambda x: abs(grouped_data[x].get('net_deliverable', 0)),
                                  reverse=True)
    elif sort_by == "Net Position":
        sorted_underlyings = sorted(grouped_data.keys(),
                                  key=lambda x: abs(grouped_data[x]['net_position']),
                                  reverse=True)
    elif sort_by == "Total Positions":
        sorted_underlyings = sorted(grouped_data.keys(),
                                  key=lambda x: len(grouped_data[x]['positions']),
                                  reverse=True)
    elif sort_by == "Unique Expiries":
        sorted_underlyings = sorted(grouped_data.keys(),
                                  key=lambda x: len(grouped_data[x]['unique_expiries']),
                                  reverse=True)

    # Display summary if requested
    if view_mode in ["Summary", "Both"]:
        st.subheader("Summary")
        summary_df = grouper.create_summary_dataframe(grouped_data)

        # Add spot prices column if available
        if price_manager:
            spot_prices = []
            for underlying in summary_df['Underlying']:
                price = grouped_data.get(underlying, {}).get('spot_price', '')
                spot_prices.append(f"{price:,.2f}" if price else "N/A")
            summary_df['Spot Price'] = spot_prices

        st.dataframe(
            summary_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                'Net Position (Lots)': st.column_config.NumberColumn(format="%.2f"),
                'Net Deliverable (Lots)': st.column_config.NumberColumn(format="%.2f"),
                'Futures': st.column_config.NumberColumn(format="%.2f"),
                'Calls': st.column_config.NumberColumn(format="%.2f"),
                'Puts': st.column_config.NumberColumn(format="%.2f")
            }
        )

    # Display detailed view if requested
    if view_mode in ["Detailed", "Both"]:
        st.subheader("Detailed Positions")

        # Create expander for each underlying
        for underlying in sorted_underlyings:
            data = grouped_data[underlying]

            # Create expander label
            net_pos = data['net_position']
            net_deliverable = data.get('net_deliverable', 0)
            pos_count = len(data['positions'])
            spot_price = data.get('spot_price', None)

            label = f"{underlying} | Net Deliverable: {net_deliverable:+.0f} lots | {pos_count} positions"
            if spot_price:
                label += f" | Spot: {spot_price:,.2f}"

            with st.expander(label, expanded=expand_all):
                # Show metrics
                col1, col2, col3, col4, col5 = st.columns(5)

                with col1:
                    st.metric("Net Position", f"{net_pos:+.0f} lots")
                with col2:
                    st.metric("Net Deliverable", f"{net_deliverable:+.0f} lots")
                with col3:
                    st.metric("Futures", f"{data['total_futures']:+.0f}")
                with col4:
                    st.metric("Calls", f"{data['total_calls']:+.0f}")
                with col5:
                    st.metric("Puts", f"{data['total_puts']:+.0f}")

                # Show detailed positions
                detailed_df = grouper.create_detailed_dataframe(underlying, data)

                if not detailed_df.empty:
                    st.dataframe(
                        detailed_df,
                        use_container_width=True,
                        hide_index=True,
                        column_config={
                            'Strike': st.column_config.NumberColumn(format="%.2f"),
                            'Position (Lots)': st.column_config.NumberColumn(format="%.2f"),
                            'Deliverable (Lots)': st.column_config.NumberColumn(format="%.2f"),
                            'Position (Qty)': st.column_config.NumberColumn(format="%.0f")
                        }
                    )

                # Show expiry summary
                if data['unique_expiries']:
                    st.write(f"**Expiries**: {', '.join([exp.strftime('%Y-%m-%d') for exp in data['unique_expiries']])}")

def display_deliverables_tab():
    """Display deliverables and IV analysis"""
    st.header("💰 Deliverables & Intrinsic Value Analysis")

    if not st.session_state.get('deliverables_complete'):
        st.info("Run the pipeline with deliverables enabled to see this analysis")
        return

    data = st.session_state.deliverables_data

    # Display underlying prices report first
    if 'price_report' in data and data['price_report'] is not None and not data['price_report'].empty:
        with st.expander("📊 Underlying Prices Used", expanded=True):
            st.subheader("Underlying Asset Prices")
            st.markdown("These are the underlying prices used for all derivative calculations:")

            # Display the price report
            price_df = data['price_report']

            # Format the price column
            price_df_display = price_df.copy()
            price_df_display['Price'] = price_df_display['Price'].apply(lambda x: f"{x:,.2f}" if pd.notna(x) and x > 0 else "N/A")

            # Use columns for better layout
            col1, col2 = st.columns([1, 2])

            with col1:
                st.metric("Total Underlyings", len(price_df))
                # Count manual vs yahoo prices based on Source column
                manual_count = len(price_df[price_df['Source'].str.contains('Manual', na=False)])
                yahoo_count = len(price_df[price_df['Source'].str.contains('Yahoo', na=False)])
                st.metric("Manual Prices", manual_count)
                st.metric("Yahoo Prices", yahoo_count)

            with col2:
                st.dataframe(price_df_display, use_container_width=True, height=300)

    col1, col2, col3, col4 = st.columns(4)
    
    pre_deliv = data['pre_trade']
    post_deliv = data['post_trade']
    
    with col1:
        # Convert to numeric to handle mixed types from enhanced clearing file
        pre_total = pd.to_numeric(pre_deliv['Deliverable_Lots'], errors='coerce').sum() if not pre_deliv.empty else 0
        st.metric("Pre-Trade Deliverable (Lots)", f"{pre_total:,.0f}")

    with col2:
        # Convert to numeric to handle mixed types from enhanced clearing file
        post_total = pd.to_numeric(post_deliv['Deliverable_Lots'], errors='coerce').sum() if not post_deliv.empty else 0
        st.metric("Post-Trade Deliverable (Lots)", f"{post_total:,.0f}")
    
    with col3:
        change = post_total - pre_total
        st.metric("Deliverable Change", f"{change:,.0f}", delta=f"{change:+,.0f}")
    
    with col4:
        # Convert to numeric to handle mixed types from enhanced clearing file
        pre_iv = pd.to_numeric(pre_deliv['Intrinsic_Value_INR'], errors='coerce').sum() if not pre_deliv.empty else 0
        post_iv = pd.to_numeric(post_deliv['Intrinsic_Value_INR'], errors='coerce').sum() if not post_deliv.empty else 0
        iv_change = post_iv - pre_iv
        st.metric("IV Change (INR)", f"{iv_change:,.0f}", delta=f"{iv_change:+,.0f}")
    
    tab1, tab2, tab3 = st.tabs(["Pre-Trade Deliverables", "Post-Trade Deliverables", "Comparison"])
    
    with tab1:
        if not pre_deliv.empty:
            st.dataframe(pre_deliv, use_container_width=True, hide_index=True)
    
    with tab2:
        if not post_deliv.empty:
            st.dataframe(post_deliv, use_container_width=True, hide_index=True)
    
    with tab3:
        if not pre_deliv.empty and not post_deliv.empty:
            # Create copies with numeric conversions to handle enhanced clearing file
            pre_clean = pre_deliv[['Ticker', 'Deliverable_Lots', 'Intrinsic_Value_INR']].copy()
            post_clean = post_deliv[['Ticker', 'Deliverable_Lots', 'Intrinsic_Value_INR']].copy()

            # Convert to numeric
            pre_clean['Deliverable_Lots'] = pd.to_numeric(pre_clean['Deliverable_Lots'], errors='coerce')
            pre_clean['Intrinsic_Value_INR'] = pd.to_numeric(pre_clean['Intrinsic_Value_INR'], errors='coerce')
            post_clean['Deliverable_Lots'] = pd.to_numeric(post_clean['Deliverable_Lots'], errors='coerce')
            post_clean['Intrinsic_Value_INR'] = pd.to_numeric(post_clean['Intrinsic_Value_INR'], errors='coerce')

            comparison = pd.merge(
                pre_clean,
                post_clean,
                on='Ticker',
                how='outer',
                suffixes=('_Pre', '_Post')
            ).fillna(0)

            comparison['Deliv_Change'] = comparison['Deliverable_Lots_Post'] - comparison['Deliverable_Lots_Pre']
            comparison['IV_Change'] = comparison['Intrinsic_Value_INR_Post'] - comparison['Intrinsic_Value_INR_Pre']

            st.dataframe(comparison, use_container_width=True, hide_index=True)

def display_expiry_deliveries_tab():
    """Display expiry delivery results with both viewing and downloading"""
    st.header("📅 Expiry Physical Deliveries")
    
    # Check if generation has been run
    if not st.session_state.get('expiry_deliveries_complete'):
        st.warning("⚠️ Expiry deliveries have not been generated yet")
        
        # Add button to generate if Stage 1 is complete
        if st.session_state.get('stage1_complete'):
            col1, col2, col3 = st.columns([1, 2, 1])
            with col2:
                if st.button("🚀 Generate Expiry Deliveries Now", type="primary", use_container_width=True):
                    run_expiry_delivery_generation()
                    st.rerun()
        else:
            st.info("Complete Stage 1 first, then generate expiry deliveries")
        return
    
    # Get results from session state
    results = st.session_state.get('expiry_delivery_results', {})
    files = st.session_state.get('expiry_delivery_files', {})
    
    if not results and not files:
        st.error("No expiry delivery data available. Please regenerate.")
        if st.button("🔄 Regenerate Expiry Deliveries", type="secondary"):
            run_expiry_delivery_generation()
            st.rerun()
        return
    
    # Display summary metrics
    st.markdown("### 📊 Summary")
    col1, col2, col3, col4 = st.columns(4)
    
    pre_results = results.get('pre_trade', {})
    post_results = results.get('post_trade', {})
    
    with col1:
        st.metric("Expiry Dates", len(set(list(pre_results.keys()) + list(post_results.keys()))))
    
    with col2:
        pre_count = sum(len(data.get('derivatives', pd.DataFrame())) for data in pre_results.values())
        st.metric("Pre-Trade Deliveries", pre_count)
    
    with col3:
        post_count = sum(len(data.get('derivatives', pd.DataFrame())) for data in post_results.values())
        st.metric("Post-Trade Deliveries", post_count)
    
    with col4:
        st.metric("Files Generated", len(files))
    
    st.markdown("---")
    
    # Section 1: Download all files
    st.markdown("### 📥 Download Expiry Reports")
    
    if files:
        # Show all available files
        st.success(f"✅ {len(files)} expiry report(s) ready for download")
        
        # Create download buttons in a grid
        n_files = len(files)
        n_cols = min(3, n_files)
        
        if n_cols > 0:
            cols = st.columns(n_cols)
            for idx, (expiry_date, file_path) in enumerate(sorted(files.items())):
                col_idx = idx % n_cols
                with cols[col_idx]:
                    try:
                        # Check if file exists
                        if Path(file_path).exists():
                            with open(file_path, 'rb') as f:
                                file_data = f.read()
                            
                            # Create expiry card
                            with st.container():
                                st.markdown(f'<div class="expiry-card">', unsafe_allow_html=True)
                                st.markdown(f"**📅 {expiry_date.strftime('%B %d, %Y')}**")
                                st.download_button(
                                    f"Download Report",
                                    data=file_data,
                                    file_name=f"EXPIRY_{expiry_date.strftime('%Y%m%d')}.xlsx",
                                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                    use_container_width=True,
                                    key=f"dl_exp_{expiry_date.strftime('%Y%m%d')}"
                                )
                                st.markdown('</div>', unsafe_allow_html=True)
                        else:
                            st.error(f"File not found: {Path(file_path).name}")
                    except Exception as e:
                        st.error(f"Error loading {expiry_date}: {str(e)}")
    else:
        st.warning("No files available for download")
    
    st.markdown("---")
    
    # Section 2: View detailed data
    st.markdown("### 📋 View Expiry Details")
    
    if pre_results or post_results:
        all_expiries = sorted(set(list(pre_results.keys()) + list(post_results.keys())))
        
        col1, col2 = st.columns([2, 3])
        
        with col1:
            selected_expiry = st.selectbox(
                "Select Expiry Date to View",
                options=all_expiries,
                format_func=lambda x: x.strftime('%B %d, %Y (%a)')
            )
        
        with col2:
            if selected_expiry and selected_expiry in files:
                file_path = files[selected_expiry]
                if Path(file_path).exists():
                    with open(file_path, 'rb') as f:
                        st.download_button(
                            f"📥 Download {selected_expiry.strftime('%Y-%m-%d')} Report",
                            data=f.read(),
                            file_name=f"EXPIRY_{selected_expiry.strftime('%Y%m%d')}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            use_container_width=True,
                            key=f"dl_selected_{selected_expiry.strftime('%Y%m%d')}"
                        )
        
        if selected_expiry:
            st.markdown(f"#### Expiry Date: {selected_expiry.strftime('%B %d, %Y')}")
            
            tabs = st.tabs(["📈 Pre-Trade", "📉 Post-Trade", "🔄 Comparison"])
            
            with tabs[0]:
                display_expiry_data(pre_results.get(selected_expiry, {}), "Pre-Trade")
            
            with tabs[1]:
                display_expiry_data(post_results.get(selected_expiry, {}), "Post-Trade")
            
            with tabs[2]:
                display_expiry_comparison(
                    pre_results.get(selected_expiry, {}),
                    post_results.get(selected_expiry, {})
                )
    else:
        st.info("No expiry data available to view")

def display_expiry_data(expiry_data: dict, stage: str):
    """Helper function to display expiry data"""
    if not expiry_data:
        st.info(f"No {stage.lower()} positions for this expiry")
        return
    
    st.markdown(f'<div class="deliverable-header">{stage} Positions: {expiry_data.get("position_count", 0)}</div>', 
                unsafe_allow_html=True)
    
    # Derivatives section
    deriv_df = expiry_data.get('derivatives', pd.DataFrame())
    if not deriv_df.empty:
        with st.expander(f"📊 Derivative Trades ({len(deriv_df)} positions)", expanded=True):
            # Add color coding for Buy/Sell
            def color_buysell(val):
                if val == 'Buy':
                    return 'background-color: #90EE90'
                elif val == 'Sell':
                    return 'background-color: #FFB6C1'
                return ''
            
            styled_df = deriv_df.style.applymap(color_buysell, subset=['Buy/Sell'])
            st.dataframe(styled_df, use_container_width=True, hide_index=True)
    
    # Cash trades section
    cash_df = expiry_data.get('cash_trades', pd.DataFrame())
    if not cash_df.empty:
        with st.expander(f"💵 Cash Trades ({len(cash_df)} trades)", expanded=True):
            st.info("📌 Trade Notes: **E** = Exercise (long options), **A** = Assignment (short options)")
            
            # Highlight trade notes
            def highlight_tradenotes(val):
                if val == 'E':
                    return 'background-color: #90EE90; font-weight: bold'
                elif val == 'A':
                    return 'background-color: #FFB6C1; font-weight: bold'
                return ''
            
            styled_cash = cash_df.style.applymap(highlight_tradenotes, subset=['tradenotes'])
            st.dataframe(styled_cash, use_container_width=True, hide_index=True)
    
    # Cash summary section
    summary_df = expiry_data.get('cash_summary', pd.DataFrame())
    if not summary_df.empty:
        with st.expander("💰 Cash Summary & Net Deliverables", expanded=True):
            # Highlight NET and GRAND TOTAL rows
            def highlight_summary(row):
                if 'NET DELIVERABLE' in str(row.get('Type', '')):
                    return ['background-color: #ADD8E6; font-weight: bold'] * len(row)
                elif 'GRAND TOTAL' in str(row.get('Underlying', '')):
                    return ['background-color: #FFD700; font-weight: bold; font-size: 110%'] * len(row)
                elif row.get('Type') == 'Trade':
                    return [''] * len(row)
                else:
                    return ['background-color: #F5F5F5'] * len(row)
            
            styled_summary = summary_df.style.apply(highlight_summary, axis=1)
            st.dataframe(styled_summary, use_container_width=True, hide_index=True)
            
            # Show key metrics
            if 'GRAND TOTAL' in summary_df['Underlying'].values:
                grand_total_row = summary_df[summary_df['Underlying'] == 'GRAND TOTAL'].iloc[0]
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Total Consideration", f"₹{grand_total_row.get('Consideration', 0):,.2f}")
                with col2:
                    st.metric("Total STT", f"₹{grand_total_row.get('STT', 0):,.2f}")
                with col3:
                    st.metric("Total Taxes", f"₹{grand_total_row.get('Taxes', 0):,.2f}")

def display_expiry_comparison(pre_data: dict, post_data: dict):
    """Display comparison between pre and post trade for an expiry"""
    if not pre_data and not post_data:
        st.info("No data available for comparison")
        return
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("##### Pre-Trade Metrics")
        pre_deriv = len(pre_data.get('derivatives', pd.DataFrame()))
        pre_cash = len(pre_data.get('cash_trades', pd.DataFrame()))
        st.write(f"📊 Derivatives: **{pre_deriv}**")
        st.write(f"💵 Cash Trades: **{pre_cash}**")
        
        # Get total consideration
        pre_summary = pre_data.get('cash_summary', pd.DataFrame())
        if not pre_summary.empty and 'GRAND TOTAL' in pre_summary['Underlying'].values:
            pre_total = pre_summary[pre_summary['Underlying'] == 'GRAND TOTAL'].iloc[0]
            st.write(f"💰 Consideration: **₹{pre_total.get('Consideration', 0):,.2f}**")
    
    with col2:
        st.markdown("##### Post-Trade Metrics")
        post_deriv = len(post_data.get('derivatives', pd.DataFrame()))
        post_cash = len(post_data.get('cash_trades', pd.DataFrame()))
        st.write(f"📊 Derivatives: **{post_deriv}**")
        st.write(f"💵 Cash Trades: **{post_cash}**")
        
        # Get total consideration
        post_summary = post_data.get('cash_summary', pd.DataFrame())
        if not post_summary.empty and 'GRAND TOTAL' in post_summary['Underlying'].values:
            post_total = post_summary[post_summary['Underlying'] == 'GRAND TOTAL'].iloc[0]
            st.write(f"💰 Consideration: **₹{post_total.get('Consideration', 0):,.2f}**")
    
    st.markdown("---")
    
    # Show changes
    st.markdown("##### 📈 Changes Due to Trading")
    
    change_col1, change_col2, change_col3 = st.columns(3)
    
    with change_col1:
        deriv_change = post_deriv - pre_deriv
        color = "🟢" if deriv_change < 0 else "🔴" if deriv_change > 0 else "⚪"
        st.metric("Derivative Positions", f"{deriv_change:+d}", delta=f"{color}")
    
    with change_col2:
        cash_change = post_cash - pre_cash
        color = "🟢" if cash_change < 0 else "🔴" if cash_change > 0 else "⚪"
        st.metric("Cash Trades", f"{cash_change:+d}", delta=f"{color}")
    
    with change_col3:
        pre_consid = 0
        post_consid = 0
        
        if not pre_summary.empty and 'GRAND TOTAL' in pre_summary['Underlying'].values:
            pre_consid = pre_summary[pre_summary['Underlying'] == 'GRAND TOTAL'].iloc[0].get('Consideration', 0)
        
        if not post_summary.empty and 'GRAND TOTAL' in post_summary['Underlying'].values:
            post_consid = post_summary[post_summary['Underlying'] == 'GRAND TOTAL'].iloc[0].get('Consideration', 0)
        
        consid_change = post_consid - pre_consid
        st.metric("Net Consideration", f"₹{consid_change:+,.2f}")

def display_reconciliation_tab():
    """Display PMS reconciliation results"""
    st.header("🔄 PMS Position Reconciliation")
    
    if not st.session_state.get('recon_complete'):
        st.info("Run the pipeline with PMS reconciliation enabled to see this analysis")
        return
    
    data = st.session_state.recon_data
    pre_recon = data['pre_trade']
    post_recon = data['post_trade']
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Pre-Trade Reconciliation")
        st.metric("Total Discrepancies", pre_recon['summary']['total_discrepancies'])
        st.metric("Matched Positions", pre_recon['summary']['matched_count'])
        st.metric("Mismatches", pre_recon['summary']['mismatch_count'])
    
    with col2:
        st.subheader("Post-Trade Reconciliation")
        st.metric("Total Discrepancies", post_recon['summary']['total_discrepancies'])
        st.metric("Matched Positions", post_recon['summary']['matched_count'])
        st.metric("Mismatches", post_recon['summary']['mismatch_count'])
    
    if pre_recon['position_mismatches'] or post_recon['position_mismatches']:
        st.subheader("Position Mismatches")
        
        tab1, tab2 = st.tabs(["Pre-Trade", "Post-Trade"])
        
        with tab1:
            if pre_recon['position_mismatches']:
                df = pd.DataFrame(pre_recon['position_mismatches'])
                st.dataframe(df, use_container_width=True, hide_index=True)
        
        with tab2:
            if post_recon['position_mismatches']:
                df = pd.DataFrame(post_recon['position_mismatches'])
                st.dataframe(df, use_container_width=True, hide_index=True)

def display_downloads():
    """Display download section"""
    st.header("📥 Download Outputs")
    
    # Determine number of columns needed
    n_cols = 3
    if st.session_state.get('expiry_deliveries_complete', False):
        n_cols = 4
    
    cols = st.columns(n_cols)
    
    with cols[0]:
        st.markdown("### Stage 1 Outputs")
        
        if st.session_state.stage1_complete and st.session_state.stage1_outputs:
            for key, path in st.session_state.stage1_outputs.items():
                if path and Path(path).exists():
                    try:
                        with open(path, 'rb') as f:
                            data = f.read()
                        
                        mime = 'text/csv'
                        if 'excel' in key:
                            mime = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                        elif 'summary' in key:
                            mime = 'text/plain'
                        
                        label = key.replace('_', ' ').title()
                        st.download_button(
                            f"📄 {label}",
                            data,
                            file_name=Path(path).name,
                            mime=mime,
                            key=f"dl_s1_{key}",
                            use_container_width=True
                        )
                    except:
                        pass
        else:
            st.info("No outputs yet")
    
    with cols[1]:
        st.markdown("### Stage 2 Outputs")
        
        if st.session_state.stage2_complete and st.session_state.stage2_outputs:
            for key, path in st.session_state.stage2_outputs.items():
                if path and Path(path).exists():
                    try:
                        with open(path, 'rb') as f:
                            data = f.read()
                        
                        if 'acm' in key:
                            label = "📊 ACM ListedTrades"
                        elif 'error' in key:
                            label = "⚠️ Validation Errors"
                        elif 'schema' in key:
                            label = "📘 Schema Used"
                        else:
                            label = key.title()
                        
                        mime = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' if 'schema' in key else 'text/csv'
                        
                        st.download_button(
                            label,
                            data,
                            file_name=Path(path).name,
                            mime=mime,
                            key=f"dl_s2_{key}",
                            use_container_width=True
                        )
                    except:
                        pass
        else:
            st.info("No outputs yet")
    
    with cols[2]:
        st.markdown("### Enhanced Reports")
        
        # Deliverables download
        if st.session_state.get('deliverables_file'):
            try:
                with open(st.session_state.deliverables_file, 'rb') as f:
                    st.download_button(
                        "💰 Deliverables Report",
                        f.read(),
                        file_name=Path(st.session_state.deliverables_file).name,
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True,
                        key="dl_deliverables"
                    )
            except:
                pass
        
        # PMS Reconciliation download
        if st.session_state.get('recon_file'):
            try:
                with open(st.session_state.recon_file, 'rb') as f:
                    st.download_button(
                        "🔄 PMS Reconciliation",
                        f.read(),
                        file_name=Path(st.session_state.recon_file).name,
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True,
                        key="dl_recon"
                    )
            except:
                pass

        # Broker Reconciliation downloads
        if st.session_state.get('broker_recon_report'):
            try:
                with open(st.session_state.broker_recon_report, 'rb') as f:
                    st.download_button(
                        "🏦 Broker Recon Report",
                        f.read(),
                        file_name=Path(st.session_state.broker_recon_report).name,
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True,
                        key="dl_broker_recon"
                    )
            except:
                pass

        if st.session_state.get('enhanced_clearing_file'):
            try:
                with open(st.session_state.enhanced_clearing_file, 'rb') as f:
                    st.download_button(
                        "📊 Enhanced Clearing File",
                        f.read(),
                        file_name=Path(st.session_state.enhanced_clearing_file).name,
                        mime="text/csv",
                        use_container_width=True,
                        key="dl_enhanced_clearing"
                    )
            except:
                pass

        # Final Enhanced Clearing File (post-trade processing with splits)
        if st.session_state.get('final_enhanced_clearing_file'):
            try:
                with open(st.session_state.final_enhanced_clearing_file, 'rb') as f:
                    st.download_button(
                        "✅ Final Enhanced Clearing",
                        f.read(),
                        file_name=Path(st.session_state.final_enhanced_clearing_file).name,
                        mime="text/csv",
                        use_container_width=True,
                        key="dl_final_enhanced_clearing"
                    )
            except:
                pass

        # Positions by Underlying download
        if st.session_state.stage1_complete and POSITION_GROUPER_AVAILABLE:
            if st.button("📂 Generate Positions by Underlying Excel", use_container_width=True):
                with st.spinner("Generating positions by underlying report..."):
                    # Get the output generator and final positions
                    final_positions_df = st.session_state.dataframes['stage1']['final_positions']
                    output_gen = st.session_state.get('output_generator')

                    if output_gen and not final_positions_df.empty:
                        # Get price manager if available
                        price_manager = None
                        if SIMPLE_PRICE_MANAGER_AVAILABLE:
                            price_manager = get_price_manager()

                        # Generate the Excel file
                        excel_path = output_gen.save_positions_by_underlying_excel(
                            final_positions_df,
                            file_prefix="positions_by_underlying",
                            price_manager=price_manager
                        )

                        if excel_path and Path(excel_path).exists():
                            st.session_state['positions_by_underlying_file'] = excel_path
                            st.success("✅ Report generated successfully!")
                            st.rerun()
                        else:
                            st.error("Failed to generate report")
                    else:
                        st.error("No positions data available")

        # Show download button if report exists
        if st.session_state.get('positions_by_underlying_file'):
            try:
                with open(st.session_state.positions_by_underlying_file, 'rb') as f:
                    st.download_button(
                        "📂 Download Positions by Underlying",
                        f.read(),
                        file_name=Path(st.session_state.positions_by_underlying_file).name,
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True,
                        key="dl_positions_by_underlying"
                    )
            except:
                pass

        if not st.session_state.get('deliverables_file') and not st.session_state.get('recon_file') and not st.session_state.get('positions_by_underlying_file') and not st.session_state.get('broker_recon_report') and not st.session_state.get('enhanced_clearing_file'):
            st.info("Enable additional features in sidebar")
    
    # Add Expiry Deliveries column if available
    if st.session_state.get('expiry_deliveries_complete', False):
        with cols[3]:
            st.markdown("### 📅 Expiry Deliveries")
            
            files = st.session_state.get('expiry_delivery_files', {})
            if files:
                st.success(f"✅ {len(files)} reports ready")
                
                # Show first 3 files as download buttons
                for idx, (expiry_date, file_path) in enumerate(sorted(files.items())[:3]):
                    try:
                        with open(file_path, 'rb') as f:
                            st.download_button(
                                f"📅 {expiry_date.strftime('%m/%d')}",
                                data=f.read(),
                                file_name=f"EXPIRY_{expiry_date.strftime('%Y%m%d')}.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                use_container_width=True,
                                key=f"dl_quick_exp_{idx}"
                            )
                    except:
                        pass
                
                if len(files) > 3:
                    st.info(f"+ {len(files) - 3} more in Expiry tab")
            else:
                st.warning("No expiry files generated")

def display_positions_by_expiry():
    """Display positions grouped by expiry date"""

    if 'stage1' not in st.session_state.dataframes:
        st.info("No positions data available.")
        return

    final_positions_df = st.session_state.dataframes['stage1']['final_positions']

    if final_positions_df.empty:
        st.warning("No positions to display.")
        return

    # Initialize grouper
    grouper = PositionGrouper()
    price_manager = None
    if SIMPLE_PRICE_MANAGER_AVAILABLE:
        price_manager = get_price_manager()

    # Group by underlying first, then by expiry
    grouped_data = grouper.group_positions_from_dataframe(final_positions_df, price_manager=price_manager)
    expiry_groups = grouper.group_by_expiry(grouped_data)

    if not expiry_groups:
        st.warning("No expiry data found.")
        return

    # Sort by expiry date
    sorted_expiries = sorted(expiry_groups.keys())

    # Summary metrics
    st.subheader("Expiry Summary")
    cols = st.columns(4)
    with cols[0]:
        st.metric("Total Expiries", len(expiry_groups))
    with cols[1]:
        total_deliv = sum(data['total_deliverable'] for data in expiry_groups.values())
        st.metric("Total Deliverable", f"{total_deliv:,.0f} lots")
    with cols[2]:
        total_positions = sum(len(data['underlyings']) for data in expiry_groups.values())
        st.metric("Total Underlyings", total_positions)
    with cols[3]:
        nearest_expiry = sorted_expiries[0] if sorted_expiries else "N/A"
        st.metric("Nearest Expiry", nearest_expiry)

    st.divider()

    # Display each expiry
    for expiry_key in sorted_expiries:
        expiry_data = expiry_groups[expiry_key]

        with st.expander(
            f"📅 {expiry_key} | Deliverable: {expiry_data['total_deliverable']:+.0f} lots | "
            f"{len(expiry_data['underlyings'])} underlyings",
            expanded=False
        ):
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Futures", f"{expiry_data['total_futures']:+.0f}")
            with col2:
                st.metric("Calls", f"{expiry_data['total_calls']:+.0f}")
            with col3:
                st.metric("Puts", f"{expiry_data['total_puts']:+.0f}")
            with col4:
                st.metric("Net Deliverable", f"{expiry_data['total_deliverable']:+.0f}")

            # Show positions by underlying for this expiry
            for underlying, und_data in sorted(expiry_data['underlyings'].items()):
                st.markdown(f"**{underlying}** | Net Deliv: {und_data['net_deliverable']:+.0f} lots")

                # Create DataFrame for positions
                pos_data = []
                for pos in und_data['positions']:
                    moneyness = ""
                    deliverable = 0
                    spot_price = und_data.get('spot_price')

                    if pos['security_type'] == 'Futures':
                        deliverable = pos['position_lots']
                        moneyness = "N/A"
                    elif spot_price and pos['strike']:
                        if pos['security_type'] == 'Call':
                            if spot_price > pos['strike']:
                                moneyness = "ITM"
                                deliverable = pos['position_lots']  # Long call = long underlying
                            else:
                                moneyness = "OTM"
                        elif pos['security_type'] == 'Put':
                            if spot_price < pos['strike']:
                                moneyness = "ITM"
                                deliverable = -pos['position_lots']  # Long put = short underlying
                            else:
                                moneyness = "OTM"

                    pos_data.append({
                        'Symbol': pos['symbol'],
                        'Type': pos['security_type'],
                        'Strike': pos['strike'] if pos['strike'] else '',
                        'Position': pos['position_lots'],
                        'Moneyness': moneyness,
                        'Deliverable': deliverable
                    })

                if pos_data:
                    df = pd.DataFrame(pos_data)
                    st.dataframe(df, use_container_width=True, hide_index=True)

def display_pre_post_comparison():
    """Display pre-trade vs post-trade comparison"""

    if 'stage1' not in st.session_state.dataframes:
        st.info("No positions data available.")
        return

    starting_positions_df = st.session_state.dataframes['stage1']['starting_positions']
    final_positions_df = st.session_state.dataframes['stage1']['final_positions']

    if starting_positions_df.empty and final_positions_df.empty:
        st.warning("No positions to compare.")
        return

    # Initialize grouper
    grouper = PositionGrouper()
    price_manager = None
    if SIMPLE_PRICE_MANAGER_AVAILABLE:
        price_manager = get_price_manager()

    # Group both datasets
    pre_grouped = grouper.group_positions_from_dataframe(starting_positions_df, price_manager=price_manager)
    post_grouped = grouper.group_positions_from_dataframe(final_positions_df, price_manager=price_manager)

    # Get all underlyings
    all_underlyings = sorted(set(list(pre_grouped.keys()) + list(post_grouped.keys())))

    # Summary comparison
    st.subheader("Pre vs Post Trade Summary")

    comparison_data = []
    for underlying in all_underlyings:
        pre_data = pre_grouped.get(underlying, {'net_position': 0, 'net_deliverable': 0})
        post_data = post_grouped.get(underlying, {'net_position': 0, 'net_deliverable': 0})

        comparison_data.append({
            'Underlying': underlying,
            'Pre Position': pre_data['net_position'],
            'Post Position': post_data['net_position'],
            'Position Change': post_data['net_position'] - pre_data['net_position'],
            'Pre Deliverable': pre_data['net_deliverable'],
            'Post Deliverable': post_data['net_deliverable'],
            'Deliverable Change': post_data['net_deliverable'] - pre_data['net_deliverable']
        })

    comp_df = pd.DataFrame(comparison_data)

    # Show only changed positions
    show_all = st.checkbox("Show all underlyings (including unchanged)", value=False)

    if not show_all:
        comp_df = comp_df[comp_df['Position Change'] != 0]

    st.dataframe(
        comp_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            'Pre Position': st.column_config.NumberColumn(format="%.2f"),
            'Post Position': st.column_config.NumberColumn(format="%.2f"),
            'Position Change': st.column_config.NumberColumn(format="%+.2f"),
            'Pre Deliverable': st.column_config.NumberColumn(format="%.2f"),
            'Post Deliverable': st.column_config.NumberColumn(format="%.2f"),
            'Deliverable Change': st.column_config.NumberColumn(format="%+.2f")
        }
    )

    # Overall metrics
    st.divider()
    st.subheader("Overall Changes")

    col1, col2, col3 = st.columns(3)
    with col1:
        total_pre_pos = comp_df['Pre Position'].sum()
        total_post_pos = comp_df['Post Position'].sum()
        st.metric("Net Position Change", f"{total_post_pos - total_pre_pos:+.0f} lots",
                 delta=f"{total_post_pos - total_pre_pos:+.0f}")

    with col2:
        total_pre_deliv = comp_df['Pre Deliverable'].sum()
        total_post_deliv = comp_df['Post Deliverable'].sum()
        st.metric("Net Deliverable Change", f"{total_post_deliv - total_pre_deliv:+.0f} lots",
                 delta=f"{total_post_deliv - total_pre_deliv:+.0f}")

    with col3:
        new_underlyings = len(post_grouped) - len(pre_grouped)
        st.metric("Underlyings", len(post_grouped), delta=f"{new_underlyings:+d}")

def display_broker_reconciliation_tab():
    """Display broker reconciliation results with trade breaks and commission analysis"""
    st.header("🏦 Broker Reconciliation")

    if not st.session_state.get('broker_recon_complete'):
        st.info("No broker reconciliation results available. Run reconciliation in the Pipeline Overview tab.")
        return

    # Get the reconciliation report file
    recon_report = st.session_state.get('broker_recon_report')
    if not recon_report or not Path(recon_report).exists():
        st.warning("Reconciliation report file not found.")
        return

    try:
        # Read the Excel file with all sheets
        excel_file = pd.ExcelFile(recon_report)

        # Get summary data
        result = st.session_state.get('broker_recon_result', {})

        # Summary metrics at top
        st.subheader("📊 Reconciliation Summary")
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric("Matched Trades", result.get('matched_count', 0))
        with col2:
            st.metric("Match Rate", f"{result.get('match_rate', 0):.1f}%")
        with col3:
            st.metric("Unmatched Clearing", result.get('unmatched_clearing_count', 0))
        with col4:
            st.metric("Unmatched Broker", result.get('unmatched_broker_count', 0))

        st.divider()

        # Create tabs for different views
        tab1, tab2, tab3 = st.tabs(["📉 Trade Breaks", "💰 Commission Analysis", "📋 All Data"])

        with tab1:
            st.subheader("Unmatched Trades")

            # Unmatched Clearing Trades
            if 'Unmatched Clearing' in excel_file.sheet_names:
                unmatched_clearing = pd.read_excel(recon_report, sheet_name='Unmatched Clearing')

                if not unmatched_clearing.empty:
                    st.markdown(f"**🔴 Unmatched Clearing Trades: {len(unmatched_clearing)}**")

                    # Show diagnostic info
                    if 'DIAGNOSTIC_Match_Failure_Reason' in unmatched_clearing.columns:
                        st.caption("Reasons for failure:")
                        reason_counts = unmatched_clearing['DIAGNOSTIC_Match_Failure_Reason'].value_counts()
                        for reason, count in reason_counts.items():
                            st.write(f"  • {reason}: {count} trade(s)")

                    st.dataframe(unmatched_clearing, use_container_width=True, height=300)
                else:
                    st.success("✅ All clearing trades matched!")

            st.divider()

            # Unmatched Broker Trades
            if 'Unmatched Broker' in excel_file.sheet_names:
                unmatched_broker = pd.read_excel(recon_report, sheet_name='Unmatched Broker')

                if not unmatched_broker.empty:
                    st.markdown(f"**🔴 Unmatched Broker Trades: {len(unmatched_broker)}**")

                    # Show diagnostic info
                    if 'DIAGNOSTIC_Match_Failure_Reason' in unmatched_broker.columns:
                        st.caption("Reasons for failure:")
                        reason_counts = unmatched_broker['DIAGNOSTIC_Match_Failure_Reason'].value_counts()
                        for reason, count in reason_counts.items():
                            st.write(f"  • {reason}: {count} trade(s)")

                    st.dataframe(unmatched_broker, use_container_width=True, height=300)
                else:
                    st.success("✅ All broker trades matched!")

        with tab2:
            st.subheader("Commission & Tax Analysis")

            if 'Commission Report' in excel_file.sheet_names:
                comm_report = pd.read_excel(recon_report, sheet_name='Commission Report')

                if not comm_report.empty:
                    # Separate trade-level data from summary
                    # Summary rows have "BROKER SUMMARY" in Broker Name or "trades" in Bloomberg Ticker
                    summary_start = comm_report[comm_report['Broker Name'] == 'BROKER SUMMARY'].index

                    if len(summary_start) > 0:
                        trade_data = comm_report.iloc[:summary_start[0]]
                        summary_data = comm_report.iloc[summary_start[0]+1:]  # Skip the header row
                    else:
                        trade_data = comm_report
                        summary_data = pd.DataFrame()

                    # Display broker summary first
                    if not summary_data.empty:
                        st.markdown("### 📊 Summary by Broker & Product")

                        # Clean up summary data
                        summary_clean = summary_data.dropna(subset=['Broker Name'])

                        if not summary_clean.empty:
                            # Format for display
                            display_cols = ['Broker Name', 'Broker Code', 'Instrument', 'Quantity',
                                          'Trade Value', 'Brokerage', 'Comm Rate', 'Taxes', 'Tax Rate (%)']
                            display_cols = [col for col in display_cols if col in summary_clean.columns]

                            summary_display = summary_clean[display_cols].copy()

                            # Format numbers
                            if 'Trade Value' in summary_display.columns:
                                summary_display['Trade Value'] = summary_display['Trade Value'].apply(
                                    lambda x: f"₹{x:,.2f}" if pd.notna(x) and x != '' else ''
                                )
                            if 'Brokerage' in summary_display.columns:
                                summary_display['Brokerage'] = summary_display['Brokerage'].apply(
                                    lambda x: f"₹{x:,.2f}" if pd.notna(x) else ''
                                )
                            if 'Taxes' in summary_display.columns:
                                summary_display['Taxes'] = summary_display['Taxes'].apply(
                                    lambda x: f"₹{x:,.2f}" if pd.notna(x) else ''
                                )
                            if 'Tax Rate (%)' in summary_display.columns:
                                summary_display['Tax Rate (%)'] = summary_display['Tax Rate (%)'].apply(
                                    lambda x: f"{x:.4f}%" if pd.notna(x) and x != '' else ''
                                )

                            st.dataframe(summary_display, use_container_width=True, height=200)

                    st.divider()

                    # Trade-level details
                    if not trade_data.empty:
                        st.markdown("### 📝 Trade-Level Details")

                        # Add filters
                        col1, col2 = st.columns(2)
                        with col1:
                            if 'Broker Name' in trade_data.columns:
                                brokers = ['All'] + sorted(trade_data['Broker Name'].dropna().unique().tolist())
                                selected_broker = st.selectbox("Filter by Broker", brokers)

                        with col2:
                            if 'Instrument' in trade_data.columns:
                                instruments = ['All'] + sorted(trade_data['Instrument'].dropna().unique().tolist())
                                selected_instrument = st.selectbox("Filter by Instrument", instruments)

                        # Apply filters
                        filtered_data = trade_data.copy()
                        if selected_broker != 'All' and 'Broker Name' in filtered_data.columns:
                            filtered_data = filtered_data[filtered_data['Broker Name'] == selected_broker]
                        if selected_instrument != 'All' and 'Instrument' in filtered_data.columns:
                            filtered_data = filtered_data[filtered_data['Instrument'] == selected_instrument]

                        st.caption(f"Showing {len(filtered_data)} of {len(trade_data)} trades")
                        st.dataframe(filtered_data, use_container_width=True, height=400)
                else:
                    st.info("No commission data available")
            else:
                st.warning("Commission Report sheet not found in reconciliation file")

        with tab3:
            st.subheader("Complete Reconciliation Data")

            # Show all sheets
            sheet_tabs = st.tabs(excel_file.sheet_names)

            for i, sheet_name in enumerate(excel_file.sheet_names):
                with sheet_tabs[i]:
                    df = pd.read_excel(recon_report, sheet_name=sheet_name)
                    st.caption(f"{len(df)} rows")
                    st.dataframe(df, use_container_width=True, height=500)

    except Exception as e:
        st.error(f"Error loading reconciliation report: {e}")
        import traceback
        st.code(traceback.format_exc())


def display_email_reports_tab():
    """Display email reports configuration and sending"""
    st.header("📧 Email Reports")

    # Check if email is configured
    try:
        from email_config import EmailConfig, get_default_recipients
        from email_sender import EmailSender

        email_config = EmailConfig.from_streamlit_secrets()
        if not email_config.is_configured():
            st.warning("⚠️ Email not configured. Please configure Streamlit secrets first.")
            st.info("See sidebar for setup instructions")
            return
    except Exception as e:
        st.error(f"Error loading email configuration: {e}")
        return

    # Recipients section
    st.subheader("📬 Recipients")

    col1, col2 = st.columns([2, 1])
    with col1:
        default_ops = get_default_recipients()
        st.info(f"✅ Default recipient (always included): {', '.join(default_ops)}")

        additional_recipients = st.text_area(
            "Additional Recipients (optional)",
            value=st.session_state.get('email_additional_recipients', ''),
            placeholder="user1@example.com, user2@example.com",
            help="Enter additional email addresses (comma-separated)"
        )
        st.session_state.email_additional_recipients = additional_recipients

    with col2:
        # Calculate total recipients
        all_recipients = default_ops.copy()
        if additional_recipients:
            additional = [email.strip() for email in additional_recipients.split(',') if email.strip()]
            for email in additional:
                if email not in all_recipients:
                    all_recipients.append(email)

        st.metric("Total Recipients", len(all_recipients))
        with st.expander("View all recipients"):
            for i, email in enumerate(all_recipients, 1):
                st.write(f"{i}. {email}")

    st.divider()

    # Reports selection section
    st.subheader("📄 Select Reports to Email")

    # Collect available reports
    available_reports = {}

    # Deliverables report
    if st.session_state.get('deliverables_file'):
        deliverables_file = Path(st.session_state.deliverables_file)
        if deliverables_file.exists():
            available_reports['deliverables'] = {
                'name': 'Deliverables Report',
                'file': deliverables_file,
                'description': 'Physical deliverables calculation with formulas',
                'size': deliverables_file.stat().st_size / (1024 * 1024)  # MB
            }

    # Broker reconciliation report
    if st.session_state.get('broker_recon_report'):
        recon_file = Path(st.session_state.broker_recon_report)
        if recon_file.exists():
            available_reports['broker_recon'] = {
                'name': 'Broker Reconciliation Report',
                'file': recon_file,
                'description': 'Trade breaks, commission analysis (5 sheets)',
                'size': recon_file.stat().st_size / (1024 * 1024)  # MB
            }

    # Enhanced clearing file (from broker recon)
    if st.session_state.get('enhanced_clearing_file'):
        enhanced_file = Path(st.session_state.enhanced_clearing_file)
        if enhanced_file.exists():
            available_reports['enhanced_clearing'] = {
                'name': 'Enhanced Clearing File',
                'file': enhanced_file,
                'description': 'Clearing file with brokerage and taxes',
                'size': enhanced_file.stat().st_size / (1024 * 1024)  # MB
            }

    # Final enhanced clearing file (after all processing)
    if st.session_state.get('final_enhanced_clearing_file'):
        final_enhanced_file = Path(st.session_state.final_enhanced_clearing_file)
        if final_enhanced_file.exists():
            available_reports['final_enhanced_clearing'] = {
                'name': 'Final Enhanced Clearing File',
                'file': final_enhanced_file,
                'description': 'Final clearing file after all processing',
                'size': final_enhanced_file.stat().st_size / (1024 * 1024)  # MB
            }

    # Stage 1 output files (parsed trades, processed trades, final positions)
    if st.session_state.get('stage1_outputs'):
        output_files = st.session_state.stage1_outputs

        # Processed trades file
        if 'processed_trades' in output_files:
            processed_file = Path(output_files['processed_trades'])
            if processed_file.exists():
                available_reports['processed_trades'] = {
                    'name': 'Processed Trades File',
                    'file': processed_file,
                    'description': 'Trades with strategies and splits',
                    'size': processed_file.stat().st_size / (1024 * 1024)  # MB
                }

        # Parsed trades file
        if 'parsed_trades' in output_files:
            parsed_file = Path(output_files['parsed_trades'])
            if parsed_file.exists():
                available_reports['parsed_trades'] = {
                    'name': 'Parsed Trades File',
                    'file': parsed_file,
                    'description': 'Original parsed trades',
                    'size': parsed_file.stat().st_size / (1024 * 1024)  # MB
                }

        # Final positions file
        if 'final_positions' in output_files:
            positions_file = Path(output_files['final_positions'])
            if positions_file.exists():
                available_reports['final_positions'] = {
                    'name': 'Final Positions File',
                    'file': positions_file,
                    'description': 'Final positions after processing',
                    'size': positions_file.stat().st_size / (1024 * 1024)  # MB
                }

        # Summary report
        if 'summary' in output_files:
            summary_file = Path(output_files['summary'])
            if summary_file.exists():
                available_reports['summary_report'] = {
                    'name': 'Summary Report',
                    'file': summary_file,
                    'description': 'Processing summary with statistics',
                    'size': summary_file.stat().st_size / (1024 * 1024)  # MB
                }

    # PMS Reconciliation report
    if st.session_state.get('recon_file'):
        recon_file = Path(st.session_state.recon_file)
        if recon_file.exists():
            available_reports['pms_recon'] = {
                'name': 'PMS Reconciliation Report',
                'file': recon_file,
                'description': 'Position reconciliation report',
                'size': recon_file.stat().st_size / (1024 * 1024)  # MB
            }

    # Positions by Underlying
    if st.session_state.get('positions_by_underlying_file'):
        positions_file = Path(st.session_state.positions_by_underlying_file)
        if positions_file.exists():
            available_reports['positions_underlying'] = {
                'name': 'Positions by Underlying',
                'file': positions_file,
                'description': 'Positions grouped by underlying with Greeks',
                'size': positions_file.stat().st_size / (1024 * 1024)  # MB
            }

    # Expiry deliveries
    if st.session_state.get('expiry_delivery_file'):
        expiry_file = Path(st.session_state.expiry_delivery_file)
        if expiry_file.exists():
            available_reports['expiry_delivery'] = {
                'name': 'Expiry Delivery Report',
                'file': expiry_file,
                'description': 'Physical delivery for expiring positions',
                'size': expiry_file.stat().st_size / (1024 * 1024)  # MB
            }

    if not available_reports:
        st.info("No reports available yet. Process trades to generate reports.")
        return

    # Display available reports with checkboxes
    st.write("Select which reports to email:")

    selected_reports = {}
    for report_id, report_info in available_reports.items():
        col1, col2, col3 = st.columns([3, 2, 1])

        with col1:
            # Initialize session state for checkbox
            checkbox_key = f'email_select_{report_id}'
            if checkbox_key not in st.session_state:
                # Default to checked for key reports
                default_reports = ['deliverables', 'broker_recon', 'pms_recon', 'processed_trades', 'final_enhanced_clearing']
                st.session_state[checkbox_key] = report_id in default_reports

            selected = st.checkbox(
                report_info['name'],
                value=st.session_state[checkbox_key],
                key=f'email_checkbox_{report_id}',
                help=report_info['description']
            )
            st.session_state[checkbox_key] = selected

            if selected:
                selected_reports[report_id] = report_info

        with col2:
            st.caption(f"📄 {report_info['file'].name}")

        with col3:
            # Show size warning if > 5MB
            if report_info['size'] > 5:
                st.warning(f"⚠️ {report_info['size']:.1f}MB")
            else:
                st.caption(f"{report_info['size']:.1f}MB")

    st.divider()

    # Send email section
    st.subheader("📤 Send Email")

    col1, col2, col3 = st.columns([2, 1, 1])

    with col1:
        if len(selected_reports) == 0:
            st.warning("⚠️ No reports selected")
        else:
            st.success(f"✅ {len(selected_reports)} report(s) selected")

            # Show what will be sent
            with st.expander("Review email details"):
                st.markdown("**Recipients:**")
                for email in all_recipients:
                    st.write(f"  • {email}")

                st.markdown("**Attachments:**")
                total_size = 0
                for report_info in selected_reports.values():
                    st.write(f"  • {report_info['name']} ({report_info['size']:.1f}MB)")
                    total_size += report_info['size']

                st.caption(f"Total attachment size: {total_size:.1f}MB")

                if total_size > 25:
                    st.error("⚠️ Total size exceeds SendGrid limit (25MB)")

    with col2:
        # Email subject customization
        subject_suffix = st.text_input(
            "Subject suffix (optional)",
            value=st.session_state.get('email_subject_suffix', ''),
            placeholder="e.g., EOD Report"
        )
        st.session_state.email_subject_suffix = subject_suffix

    with col3:
        # Send button
        send_button = st.button(
            "📧 Send Now",
            type="primary",
            disabled=(len(selected_reports) == 0 or len(all_recipients) == 0),
            use_container_width=True
        )

    if send_button:
        if len(all_recipients) == 0:
            st.error("❌ No recipients specified")
            return

        if len(selected_reports) == 0:
            st.error("❌ No reports selected")
            return

        # Calculate total size
        total_size = sum(r['size'] for r in selected_reports.values())
        if total_size > 25:
            st.error(f"❌ Total attachment size ({total_size:.1f}MB) exceeds SendGrid limit (25MB)")
            st.info("💡 Tip: Deselect some reports or download them separately")
            return

        # Send email
        with st.spinner("Sending email..."):
            try:
                email_sender = EmailSender()

                # Prepare attachments
                attachments = [report_info['file'] for report_info in selected_reports.values()]

                # Prepare email body
                report_list = "\n".join([f"  • {r['name']}" for r in selected_reports.values()])

                # Format subject
                from datetime import datetime
                date_str = datetime.now().strftime('%d/%m/%Y')
                account_prefix = st.session_state.get('account_prefix', '').rstrip('_')
                fund_name = 'Aurigin' if account_prefix == 'AURIGIN' else account_prefix

                subject = f"{fund_name} | Reports | {date_str}"
                if subject_suffix:
                    subject += f" | {subject_suffix}"

                body = f"""
                <h2>Trade Processing Reports</h2>

                <p>Please find the requested reports attached.</p>

                <h3>Included Reports:</h3>
                <ul>
{chr(10).join([f"<li><strong>{r['name']}</strong>: {r['description']}</li>" for r in selected_reports.values()])}
                </ul>

                <h3>Summary:</h3>
                <ul>
                    <li><strong>Date:</strong> {date_str}</li>
                    <li><strong>Reports:</strong> {len(selected_reports)}</li>
                    <li><strong>Total Size:</strong> {total_size:.1f}MB</li>
                </ul>

                <hr>
                <p style="color: #666; font-size: 12px;">
                This is an automated email from the Trade Processing System.<br>
                Generated on {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}
                </p>
                """

                success = email_sender.send_email(
                    to_emails=all_recipients,
                    subject=subject,
                    html_body=body,
                    attachments=attachments
                )

                if success:
                    st.success(f"✅ Email sent successfully to {len(all_recipients)} recipient(s)!")
                    st.balloons()
                else:
                    st.error("❌ Failed to send email. Check logs for details.")

            except Exception as e:
                st.error(f"❌ Error sending email: {e}")
                import traceback
                st.code(traceback.format_exc())


def display_schema_info():
    """Display schema information"""
    st.header("📘 ACM Schema Information")
    
    tab1, tab2, tab3 = st.tabs(["Current Schema", "Field Mappings", "Transaction Rules"])
    
    with tab1:
        st.subheader("Current Schema Structure")
        
        mapper = st.session_state.acm_mapper if st.session_state.acm_mapper else ACMMapper()
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### Output Columns")
            for i, col in enumerate(mapper.columns_order, 1):
                mandatory = "🔴" if col in mapper.mandatory_columns else "⚪"
                st.write(f"{i}. {mandatory} {col}")
        
        with col2:
            st.markdown("#### Mandatory Fields")
            for col in mapper.mandatory_columns:
                st.write(f"✔ {col}")
    
    with tab2:
        st.subheader("Field Mapping Rules")
        
        mapping_data = []
        for col, rule in mapper.mapping_rules.items():
            mapping_data.append({
                "ACM Field": col,
                "Source": rule,
                "Required": "Yes" if col in mapper.mandatory_columns else "No"
            })
        
        mapping_df = pd.DataFrame(mapping_data)
        st.dataframe(mapping_df, use_container_width=True)
    
    with tab3:
        st.subheader("Transaction Type Rules")
        
        st.markdown("""
        Transaction Type is determined by combining **B/S** and **Opposite?** flags:
        """)
        
        rules_df = pd.DataFrame([
            {"B/S": "Buy", "Opposite?": "No", "→ Transaction Type": "Buy"},
            {"B/S": "Buy", "Opposite?": "Yes", "→ Transaction Type": "BuyToCover"},
            {"B/S": "Sell", "Opposite?": "No", "→ Transaction Type": "SellShort"},
            {"B/S": "Sell", "Opposite?": "Yes", "→ Transaction Type": "Sell"}
        ])
        
        st.dataframe(rules_df, use_container_width=True, hide_index=True)

# Footer
st.divider()
st.markdown("""
<div style='text-align: center; color: #666;'>
    Enhanced Trade Processing Pipeline v4.0 | Complete with Working Expiry Physical Delivery
</div>
""", unsafe_allow_html=True)

if __name__ == "__main__":
    main()
