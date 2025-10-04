"""
Enhanced Unified Trade Processing Pipeline - SIMPLIFIED & FLEXIBLE
Upload any combination of files - system automatically runs what it can
"""

import streamlit as st
import pandas as pd
from pathlib import Path
import logging
from datetime import datetime
import io

# Import our modular components
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
    page_title="FnO Position and Trade Reconciliation System",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Apply custom CSS
apply_custom_css()

# Ensure directories exist
ensure_directories()


def handle_encrypted_file(uploaded_file, file_type_name):
    """
    Handle encryption for any uploaded file (.xls, .xlsx, .csv)
    Returns: (file, password, is_encrypted)
    """
    password = None
    is_encrypted = False

    if not uploaded_file or not ENCRYPTED_FILE_SUPPORT:
        return uploaded_file, password, is_encrypted

    # Only check Excel files for encryption
    if not uploaded_file.name.endswith(('.xlsx', '.xls')):
        return uploaded_file, password, is_encrypted

    # Try known passwords first
    uploaded_file.seek(0)
    auto_password = try_known_passwords(uploaded_file)
    uploaded_file.seek(0)

    if auto_password:
        # Known password worked!
        is_encrypted = True
        password = auto_password
        st.success(f"✓ Decrypted {file_type_name} automatically")
        return uploaded_file, password, is_encrypted

    # Check if file is encrypted
    uploaded_file.seek(0)
    if is_encrypted_excel(uploaded_file):
        is_encrypted = True
        uploaded_file.seek(0)

        # Check for cached password
        cache_key = f'cached_{file_type_name}_password'
        if cache_key in st.session_state and st.session_state[cache_key]:
            password = st.session_state[cache_key]
        else:
            # Prompt user for password
            password = st.text_input(
                f"{file_type_name} password:",
                type="password",
                key=f"{file_type_name}_password",
                help="This file is encrypted. Known passwords failed, please enter password."
            )
            if password:
                st.session_state[cache_key] = password

    return uploaded_file, password, is_encrypted


def detect_and_validate_accounts(position_file, position_password, clearing_file, clearing_password, broker_files):
    """
    Detect accounts from uploaded files and validate they match.
    Returns: (detected_account, is_valid, message)
    """
    if not ACCOUNT_VALIDATION_AVAILABLE or not st.session_state.account_validator:
        return None, True, None

    validator = st.session_state.account_validator
    detected_account = None

    # Detect from position file
    if position_file:
        pos_account = validator.detect_account_in_position_file(position_file)
        if pos_account:
            detected_account = pos_account

    # Detect from clearing file
    if clearing_file:
        clearing_account = validator.detect_account_in_trade_file(clearing_file)
        if clearing_account:
            detected_account = clearing_account

    # Detect from broker files
    if broker_files:
        for broker_file in broker_files:
            broker_account = validator.detect_account_in_trade_file(broker_file)
            if broker_account:
                detected_account = broker_account
                break

    # Validate match if we have multiple file types
    if position_file and (clearing_file or broker_files):
        is_valid, status_type, message = validator.validate_account_match()

        # Update detected account
        if validator.get_account_info():
            detected_account = validator.get_account_info()

        return detected_account, is_valid, (status_type, message)

    return detected_account, True, None


def determine_workflows(position_file, clearing_file, broker_files, pms_file):
    """
    Determine what workflows can be run based on uploaded files.
    Returns: dict with workflow flags and messages
    """
    workflows = {
        'full_pipeline': False,
        'broker_recon': False,
        'pms_recon': False,
        'can_process': False,
        'messages': []
    }

    # Position + Clearing = Full Pipeline
    if position_file and clearing_file:
        workflows['full_pipeline'] = True
        workflows['can_process'] = True
        workflows['messages'].append("✅ Full Pipeline (Stage 1, Stage 2, Deliverables, Expiry)")

    # Broker files + Clearing = Broker Reconciliation
    if clearing_file and broker_files and len(broker_files) > 0:
        workflows['broker_recon'] = True
        workflows['can_process'] = True
        if not workflows['full_pipeline']:
            workflows['messages'].append("✅ Broker Reconciliation")
    elif broker_files and len(broker_files) > 0 and not clearing_file:
        workflows['messages'].append("⚠️ Broker files uploaded but no clearing file - skipping broker recon")

    # Position + PMS = PMS Reconciliation
    if position_file and pms_file:
        workflows['pms_recon'] = True
        workflows['can_process'] = True
        if not workflows['full_pipeline']:
            workflows['messages'].append("✅ PMS Position Reconciliation")
    elif pms_file and not position_file:
        workflows['messages'].append("⚠️ PMS file uploaded but no position file - skipping PMS recon")

    # Check for any valid workflow
    if not workflows['can_process']:
        workflows['messages'].append("❌ Please upload files to process")
        workflows['messages'].append("📌 Minimum: Position + Clearing OR Position + PMS OR Clearing + Broker")

    return workflows


def smart_process_everything(position_file, position_password, clearing_file, clearing_password,
                            broker_files, pms_file, mapping_file, use_default_mapping,
                            default_mapping, usdinr_rate):
    """
    Smart orchestrator - runs whatever workflows are possible based on uploaded files
    """
    # Get account prefix
    account_prefix = ""
    if ACCOUNT_VALIDATION_AVAILABLE and st.session_state.account_validator:
        account_prefix = st.session_state.account_validator.get_account_prefix()

    results = {
        'success': True,
        'workflows_run': [],
        'workflows_skipped': []
    }

    # Determine what can run
    workflows = determine_workflows(position_file, clearing_file, broker_files, pms_file)

    # 1. BROKER RECONCILIATION (if applicable and files present)
    if workflows['broker_recon'] and clearing_file and broker_files:
        st.info("🏦 Running Broker Reconciliation...")
        if BROKER_RECON_AVAILABLE:
            if run_broker_reconciliation(clearing_file, broker_files, mapping_file, account_prefix):
                results['workflows_run'].append("Broker Reconciliation")

                # Use enhanced clearing file for full pipeline if available
                if workflows['full_pipeline'] and st.session_state.get('enhanced_clearing_file'):
                    enhanced_file_path = st.session_state.enhanced_clearing_file
                    if Path(enhanced_file_path).exists():
                        with open(enhanced_file_path, 'rb') as f:
                            clearing_file = io.BytesIO(f.read())
                            clearing_file.name = Path(enhanced_file_path).name
                        st.success("✓ Using enhanced clearing file (with Comms, Taxes, TD) for pipeline")
            else:
                st.warning("⚠️ Broker reconciliation completed with issues - check results")
                results['workflows_run'].append("Broker Reconciliation (with issues)")
        else:
            results['workflows_skipped'].append("Broker Reconciliation (module not available)")
    elif broker_files and not clearing_file:
        results['workflows_skipped'].append("Broker Reconciliation (no clearing file)")

    # 2. FULL PIPELINE (if Position + Clearing present)
    if workflows['full_pipeline'] and position_file and clearing_file:
        st.info("🚀 Running Full Pipeline...")

        # Stage 1
        if process_stage1(position_file, clearing_file, mapping_file, use_default_mapping,
                         default_mapping, position_password, clearing_password, account_prefix):
            results['workflows_run'].append("Stage 1: Strategy Processing")

            # Stage 2
            if process_stage2("Use built-in schema (default)", None, account_prefix):
                results['workflows_run'].append("Stage 2: ACM Mapping")
            else:
                results['workflows_skipped'].append("Stage 2 (failed)")
                results['success'] = False

            # Deliverables
            if NEW_FEATURES_AVAILABLE:
                run_deliverables_calculation(usdinr_rate, account_prefix)
                results['workflows_run'].append("Deliverables Calculation")

            # Expiry Deliveries
            if EXPIRY_DELIVERY_AVAILABLE:
                run_expiry_delivery_generation(account_prefix)
                results['workflows_run'].append("Expiry Delivery Reports")
        else:
            results['workflows_skipped'].append("Full Pipeline (Stage 1 failed)")
            results['success'] = False

    # 3. PMS RECONCILIATION (if Position + PMS present)
    if workflows['pms_recon'] and position_file and pms_file:
        st.info("🔄 Running PMS Reconciliation...")
        if NEW_FEATURES_AVAILABLE:
            run_pms_reconciliation(
                pms_file,
                position_file=position_file,
                position_password=position_password,
                mapping_file=mapping_file,
                use_default_mapping=use_default_mapping,
                default_mapping=default_mapping
            )
            results['workflows_run'].append("PMS Position Reconciliation")
        else:
            results['workflows_skipped'].append("PMS Reconciliation (module not available)")
    elif pms_file and not position_file:
        results['workflows_skipped'].append("PMS Reconciliation (no position file)")

    return results


def main():
    st.title("🎯 FnO Position and Trade Reconciliation System")

    # Initialize price manager on first load
    if SIMPLE_PRICE_MANAGER_AVAILABLE and 'prices_initialized' not in st.session_state:
        with st.spinner("Loading prices from default_stocks.csv..."):
            pm = get_price_manager()
            if pm.load_default_stocks():
                st.session_state.prices_initialized = True
                st.session_state.price_manager = pm
                logger.info(f"✓ Loaded prices: {pm.price_source}")
            else:
                st.warning("Could not load default_stocks.csv - prices will need to be uploaded")
                st.session_state.prices_initialized = False

    # Initialize session state using utility function
    initialize_session_state()

    # Initialize account validator
    if 'account_validator' not in st.session_state:
        st.session_state.account_validator = AccountValidator() if ACCOUNT_VALIDATION_AVAILABLE else None

    # ==================== SIDEBAR (rendered first to define variables) ====================
    with st.sidebar:
        # Display detected account (if any)
        if ACCOUNT_VALIDATION_AVAILABLE and st.session_state.get('detected_account'):
            account = st.session_state.detected_account
            st.markdown(
                f"""
                <div style="background-color: {account['display_color']}15;
                            border: 2px solid {account['display_color']};
                            border-radius: 8px;
                            padding: 12px;
                            margin-bottom: 15px;">
                    <div style="font-size: 24px; text-align: center;">{account['icon']}</div>
                    <div style="font-weight: bold; font-size: 16px; text-align: center; color: {account['display_color']};">
                        {account['name']}
                    </div>
                    <div style="font-size: 11px; text-align: center; color: #666; margin-top: 4px;">
                        CP: {account['cp_code']}
                    </div>
                </div>
                """,
                unsafe_allow_html=True
            )

        # PRICE MANAGEMENT
        st.header("💰 Price Management")

        if SIMPLE_PRICE_MANAGER_AVAILABLE and st.session_state.get('price_manager'):
            pm = st.session_state.price_manager

            if pm.price_source != "Not initialized":
                st.info(f"📊 {pm.price_source}")

                # Show missing symbols if any
                if pm.missing_symbols:
                    with st.expander(f"⚠️ {len(pm.missing_symbols)} Missing", expanded=False):
                        missing_df = pm.get_missing_symbols_report()
                        if not missing_df.empty:
                            st.dataframe(missing_df, use_container_width=True, height=150)

            # Update prices button
            if st.button("📊 Update Prices", use_container_width=True):
                with st.spinner("Fetching..."):
                    progress = st.progress(0)

                    def update_progress(current, total):
                        progress.progress(current / total)

                    pm.fetch_all_prices_yahoo(update_progress)
                    st.session_state.price_manager = pm
                    st.success("✓ Updated")
                    st.rerun()

            # Price file uploader below
            price_file = st.file_uploader(
                "📂 Or upload price file",
                type=['csv', 'xlsx'],
                key="price_upload",
                help="Expected format: CSV/Excel with 2 columns - 'Symbol' or 'Ticker' and 'Price'"
            )

            if price_file:
                    price_file, price_password, price_encrypted = handle_encrypted_file(price_file, "price_file")

                    if ENCRYPTED_FILE_SUPPORT and price_encrypted and price_password:
                        success, price_df, error = read_csv_or_excel_with_password(price_file, price_password)
                        if success:
                            if pm.load_manual_prices(price_df):
                                st.session_state.price_manager = pm
                                st.success("✓ Loaded")
                                st.rerun()
                    else:
                        try:
                            if price_file.name.endswith('.csv'):
                                price_df = pd.read_csv(price_file)
                            else:
                                price_df = pd.read_excel(price_file)

                            if pm.load_manual_prices(price_df):
                                st.session_state.price_manager = pm
                                st.success("✓ Loaded")
                                st.rerun()
                        except Exception as e:
                            st.error(f"Error: {str(e)}")

            # Download updated prices if Yahoo was used
            if pm.price_source and "Yahoo Finance" in pm.price_source:
                updated_csv = pm.get_updated_csv_dataframe()
                if updated_csv is not None:
                    csv_data = updated_csv.to_csv(index=False)
                    st.download_button(
                        label="💾 Save Prices",
                        data=csv_data,
                        file_name="default_stocks_updated.csv",
                        mime="text/csv",
                        use_container_width=True
                    )

        st.divider()

        # SETTINGS
        st.header("⚙️ Settings")

        # Mapping file
        default_mapping = None
        mapping_locations = [
            "futures_mapping.csv", "futures mapping.csv",
            "data/futures_mapping.csv", "data/futures mapping.csv"
        ]

        for location in mapping_locations:
            if Path(location).exists():
                default_mapping = location
                break

        if default_mapping:
            use_default_mapping = st.checkbox(
                f"Use {Path(default_mapping).name}",
                value=True,
                key="use_default_mapping"
            )

            if not use_default_mapping:
                mapping_file = st.file_uploader(
                    "Custom Mapping",
                    type=['csv'],
                    key='mapping_file'
                )
            else:
                mapping_file = None
        else:
            st.warning("⚠️ No mapping found")
            mapping_file = st.file_uploader(
                "Upload Mapping",
                type=['csv'],
                key='mapping_file'
            )
            use_default_mapping = None

        # USD/INR Rate
        usdinr_rate = st.number_input(
            "USD/INR Rate",
            min_value=50.0,
            max_value=150.0,
            value=88.0,
            step=0.1,
            key="usdinr_rate"
        )

        # Reset button
        st.header("🔄 Reset")
        st.caption("Clear all data and start over")
        st.divider()
        if st.button("🔄 Reset", type="secondary", use_container_width=True):
            for key in list(st.session_state.keys()):
                del st.session_state[key]
            st.rerun()

        # Status area at bottom of sidebar
        st.divider()
        st.caption("📊 Processing Status")
        # Create empty placeholder for status messages
        if 'status_placeholder' not in st.session_state:
            st.session_state.status_placeholder = None
        status_placeholder = st.empty()

    # ==================== MAIN CONTENT AREA ====================
    # FILE UPLOADS IN MAIN AREA (moved from sidebar)
    header_col1, header_col2 = st.columns([3, 1])

    with header_col1:
        st.header("📂 Upload Files")

    # Process button will go in header_col2 after files are uploaded

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        position_file = st.file_uploader(
            "Clearing Position File",
            type=['xlsx', 'xls', 'csv'],
            key='position_file',
            help="BOD, Contract, or MS format"
        )

    with col2:
        clearing_file = st.file_uploader(
            "Clearing Trades",
            type=['xlsx', 'xls', 'csv'],
            key='clearing_file',
            help="Clearing broker trade file"
        )

    with col3:
        broker_files = st.file_uploader(
            "Broker Trades (multiple OK)",
            type=['xlsx', 'xls', 'csv'],
            accept_multiple_files=True,
            key='broker_files',
            help="Executing broker files"
        )

    with col4:
        pms_file = st.file_uploader(
            "PMS Position File",
            type=['xlsx', 'xls', 'csv'],
            key='pms_file',
            help="For PMS reconciliation"
        )

    # Determine what workflows can run
    workflows = determine_workflows(position_file, clearing_file, broker_files, pms_file)

    # Process button in header area
    with header_col2:
        st.write("")  # Spacing
        st.write("")  # Spacing
        if st.button("⚡ Process Everything", type="primary", use_container_width=True,
                    disabled=not workflows['can_process']):

            # Handle file encryption for all files
            position_file, position_password, pos_encrypted = handle_encrypted_file(position_file, "position_file")
            clearing_file, clearing_password, clear_encrypted = handle_encrypted_file(clearing_file, "clearing_file")

            # Handle broker files encryption
            if broker_files:
                for i, broker_file in enumerate(broker_files):
                    broker_files[i], _, _ = handle_encrypted_file(broker_file, f"broker_file_{i}")

            # Use sidebar status placeholder for all processing messages
            with status_placeholder.container():
                # Always detect account (for file naming), but only validate/block for full pipeline
                is_pms_only_mode = (position_file and pms_file and not clearing_file and not broker_files)

                detected_account, is_valid, validation_msg = detect_and_validate_accounts(
                    position_file, position_password, clearing_file, clearing_password, broker_files
                )

                st.session_state.detected_account = detected_account
                st.session_state.account_validated = is_valid

                # Only show validation messages and block if not PMS-only mode
                if not is_pms_only_mode and validation_msg:
                    status_type, message = validation_msg
                    if status_type == "success":
                        st.success(message)
                    elif status_type == "warning":
                        st.warning(message)
                    elif status_type == "error":
                        st.error(message)
                        st.stop()  # Block processing on account mismatch

                # Run smart processing
                with st.spinner("⚡ Processing..."):
                    results = smart_process_everything(
                        position_file, position_password, clearing_file, clearing_password,
                        broker_files, pms_file, mapping_file, use_default_mapping,
                        default_mapping, usdinr_rate
                    )

                # Show results in sidebar
                if results['workflows_run']:
                    st.success(f"✅ Completed: {', '.join(results['workflows_run'])}")

                if results['workflows_skipped']:
                    st.info(f"ℹ️ Skipped: {', '.join(results['workflows_skipped'])}")

            if results.get('success') and results.get('workflows_run'):
                st.balloons()

    # Show what will run
    if workflows['messages']:
        for msg in workflows['messages']:
            if msg.startswith("✅"):
                st.success(msg)
            elif msg.startswith("⚠️"):
                st.warning(msg)
            else:
                st.info(msg)

    st.divider()

    # Main content tabs
    tab_list = ["📊 Overview", "🔄 Stage 1", "📋 Stage 2"]

    if POSITION_GROUPER_AVAILABLE and 'stage1' in st.session_state.dataframes:
        tab_list.append("📂 Positions")

    if NEW_FEATURES_AVAILABLE and st.session_state.get('stage1_complete'):
        tab_list.append("💰 Deliverables")

    if EXPIRY_DELIVERY_AVAILABLE and st.session_state.get('stage1_complete'):
        tab_list.append("📅 Expiry")

    if NEW_FEATURES_AVAILABLE and st.session_state.get('recon_complete'):
        tab_list.append("🔄 PMS Recon")

    if BROKER_RECON_AVAILABLE and st.session_state.get('broker_recon_complete'):
        tab_list.append("🏦 Broker Recon")

    # Show email tab for any workflow that completed
    if EMAIL_AVAILABLE and (st.session_state.get('stage1_complete') or st.session_state.get('recon_complete')):
        tab_list.append("📧 Email")

    tab_list.extend(["📥 Downloads", "📘 Schema"])

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

    if POSITION_GROUPER_AVAILABLE and 'stage1' in st.session_state.dataframes:
        with tabs[tab_index]:
            display_positions_grouped()
        tab_index += 1

    if NEW_FEATURES_AVAILABLE and st.session_state.get('stage1_complete'):
        with tabs[tab_index]:
            display_deliverables_tab()
        tab_index += 1

    if EXPIRY_DELIVERY_AVAILABLE and st.session_state.get('stage1_complete'):
        with tabs[tab_index]:
            display_expiry_deliveries_tab()
        tab_index += 1

    if NEW_FEATURES_AVAILABLE and st.session_state.get('recon_complete'):
        with tabs[tab_index]:
            display_reconciliation_tab()
        tab_index += 1

    if BROKER_RECON_AVAILABLE and st.session_state.get('broker_recon_complete'):
        with tabs[tab_index]:
            display_broker_reconciliation_tab()
        tab_index += 1

    if EMAIL_AVAILABLE and (st.session_state.get('stage1_complete') or st.session_state.get('recon_complete')):
        with tabs[tab_index]:
            display_email_reports_tab()
        tab_index += 1

    with tabs[tab_index]:
        display_downloads()
    tab_index += 1

    with tabs[tab_index]:
        display_schema_info()


if __name__ == "__main__":
    main()
