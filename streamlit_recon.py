"""
Standalone Broker Reconciliation App
Matches clearing broker trades with executing broker trades
"""

import streamlit as st
import pandas as pd
from pathlib import Path
import logging
from datetime import datetime
from trade_reconciliation import TradeReconciler

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Page config
st.set_page_config(
    page_title="Broker Reconciliation",
    page_icon="🏦",
    layout="wide"
)

st.title("🏦 Broker Trade Reconciliation")
st.markdown("Match clearing broker trades with executing broker trades to extract commission and tax information")

# Create output directory
output_dir = Path("./output")
output_dir.mkdir(exist_ok=True)

# Sidebar for file uploads
st.sidebar.header("📁 Upload Files")

# 1. Clearing file
clearing_file = st.sidebar.file_uploader(
    "Clearing Trade File",
    type=['xlsx', 'xls'],
    help="Upload the clearing broker trade file"
)

# 2. Futures mapping
use_default_mapping = st.sidebar.checkbox("Use default futures mapping.csv", value=True)
if use_default_mapping:
    # Try multiple locations for futures mapping file
    possible_paths = [
        "futures mapping.csv",
        Path(__file__).parent / "futures mapping.csv",
        Path("./futures mapping.csv"),
    ]
    futures_mapping_file = None
    for path in possible_paths:
        if Path(path).exists():
            futures_mapping_file = str(path)
            st.sidebar.success(f"✅ Found: {Path(path).name}")
            break

    if not futures_mapping_file:
        st.sidebar.error("❌ futures mapping.csv not found!")
        st.error("**Error:** Could not find 'futures mapping.csv'. Please upload it manually or check deployment files.")
else:
    uploaded_mapping = st.sidebar.file_uploader(
        "Futures Mapping File",
        type=['csv'],
        help="Upload custom futures mapping file"
    )
    if uploaded_mapping:
        # Save temporarily
        temp_mapping = output_dir / f"temp_mapping_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        with open(temp_mapping, 'wb') as f:
            f.write(uploaded_mapping.read())
        futures_mapping_file = str(temp_mapping)
    else:
        futures_mapping_file = None

# 3. Broker files (multiple)
broker_files = st.sidebar.file_uploader(
    "Broker Files (ICICI/Kotak)",
    type=['xlsx', 'xls'],
    accept_multiple_files=True,
    help="Upload one or more broker files (both ICICI and Kotak supported)"
)

st.sidebar.markdown("---")

# Main content area
col1, col2, col3 = st.columns(3)

with col1:
    st.metric("Clearing File", "✅" if clearing_file else "❌")
with col2:
    st.metric("Futures Mapping", "✅" if futures_mapping_file else "❌")
with col3:
    st.metric("Broker Files", len(broker_files) if broker_files else 0)

# Run reconciliation button
if st.sidebar.button("🚀 Run Reconciliation", type="primary", disabled=not (clearing_file and futures_mapping_file and broker_files)):

    st.markdown("---")
    st.subheader("📊 Reconciliation Progress")

    progress_bar = st.progress(0)
    status_text = st.empty()

    try:
        # Convert uploaded files to BytesIO (works for both local and cloud)
        from io import BytesIO

        status_text.text("Loading uploaded files...")
        progress_bar.progress(5)

        # Convert clearing file to BytesIO
        clearing_file.seek(0)
        clearing_bytes = BytesIO(clearing_file.read())
        clearing_bytes.name = clearing_file.name  # Preserve filename
        clearing_file.seek(0)

        # Convert broker files to BytesIO
        broker_bytes_list = []
        for broker_file in broker_files:
            broker_file.seek(0)
            broker_bytes = BytesIO(broker_file.read())
            broker_bytes.name = broker_file.name  # Preserve filename
            broker_bytes_list.append(broker_bytes)
            broker_file.seek(0)

        # Initialize reconciler
        status_text.text("Initializing reconciliation engine...")
        progress_bar.progress(10)
        reconciler = TradeReconciler(output_dir=str(output_dir))

        # Run reconciliation
        status_text.text("Processing files and matching trades...")
        progress_bar.progress(30)

        result = reconciler.reconcile(
            clearing_file=clearing_bytes,
            broker_files=broker_bytes_list,
            futures_mapping_file=futures_mapping_file
        )

        progress_bar.progress(90)

        if result['success']:
            progress_bar.progress(100)
            status_text.text("✅ Reconciliation completed successfully!")

            st.markdown("---")

            # Display account name if detected
            if result.get('account_name') and result['account_name'] != 'Unknown':
                st.success(f"🏢 Account: **{result['account_name']}**")

            st.subheader("📈 Reconciliation Results")

            # Display metrics
            col1, col2, col3, col4 = st.columns(4)

            with col1:
                st.metric(
                    "Total Clearing Trades",
                    result['total_clearing']
                )

            with col2:
                st.metric(
                    "Total Broker Trades",
                    result['total_broker']
                )

            with col3:
                st.metric(
                    "Matched Trades",
                    result['matched_count'],
                    delta=f"{result['match_rate']:.1f}%"
                )

            with col4:
                match_rate = result['match_rate']
                if match_rate == 100:
                    st.success(f"🎯 {match_rate:.1f}%")
                    st.caption("Perfect Match!")
                elif match_rate >= 80:
                    st.warning(f"⚠️ {match_rate:.1f}%")
                    st.caption("Partial Match")
                else:
                    st.error(f"❌ {match_rate:.1f}%")
                    st.caption("Low Match")

            # Show unmatched counts
            if result['unmatched_clearing_count'] > 0 or result['unmatched_broker_count'] > 0:
                st.markdown("---")
                st.subheader("⚠️ Unmatched Trades")

                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Unmatched Clearing", result['unmatched_clearing_count'])
                    if result['unmatched_clearing_count'] > 0:
                        st.caption("Trades in clearing file not found in broker files")
                with col2:
                    st.metric("Unmatched Broker", result['unmatched_broker_count'])
                    if result['unmatched_broker_count'] > 0:
                        st.caption("Trades in broker files not found in clearing file")

                # Show warning if there are unmatched broker trades
                if result['unmatched_broker_count'] > 0:
                    st.warning(f"⚠️ {result['unmatched_broker_count']} broker trade(s) found that are not in the clearing file. Check the 'Unmatched Broker' sheet in the reconciliation report.")

            # Download buttons
            st.markdown("---")
            st.subheader("📥 Download Output Files")

            col1, col2 = st.columns(2)

            with col1:
                # Enhanced clearing file
                enhanced_file = result['enhanced_clearing_file']
                if enhanced_file and Path(enhanced_file).exists():
                    with open(enhanced_file, 'rb') as f:
                        st.download_button(
                            label="📄 Download Enhanced Clearing File",
                            data=f.read(),
                            file_name=Path(enhanced_file).name,
                            mime="text/csv",
                            use_container_width=True
                        )
                    st.caption(f"File: {Path(enhanced_file).name}")

            with col2:
                # Reconciliation report
                recon_file = result['reconciliation_report']
                if recon_file and Path(recon_file).exists():
                    with open(recon_file, 'rb') as f:
                        st.download_button(
                            label="📊 Download Reconciliation Report",
                            data=f.read(),
                            file_name=Path(recon_file).name,
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            use_container_width=True
                        )
                    st.caption(f"File: {Path(recon_file).name}")

            # Show file details
            st.markdown("---")
            st.info(f"📂 Output files saved to: `{output_dir.absolute()}`")

            # Success message for 100% match
            if result['match_rate'] == 100:
                st.success("""
                🎉 **100% Match Achieved!**

                The enhanced clearing file can now be used in the main ACM pipeline to include:
                - Actual Trade Date (from broker file)
                - Pure Brokerage amounts
                - Total Taxes
                """)

        else:
            progress_bar.progress(0)
            status_text.text("")

            # Check if it's a CP Code mismatch error
            if result.get('error_type') == 'cp_code_mismatch':
                st.error("❌ CP Code Validation Failed")
                st.error(result.get('error', 'Unknown error'))

                # Show detailed breakdown
                st.markdown("---")
                st.subheader("🔍 CP Code Details")

                col1, col2 = st.columns(2)

                with col1:
                    st.markdown("**Clearing File CP Codes:**")
                    clearing_codes = result.get('clearing_cp_codes', [])
                    if clearing_codes:
                        for code in sorted(clearing_codes):
                            st.markdown(f"- `{code}`")
                    else:
                        st.text("None found")

                with col2:
                    st.markdown("**Broker Files CP Codes:**")
                    broker_codes = result.get('broker_cp_codes', [])
                    if broker_codes:
                        for code in sorted(broker_codes):
                            st.markdown(f"- `{code}`")
                    else:
                        st.text("None found")

                st.warning("⚠️ All CP Codes in clearing file must match CP Codes in broker files. Please verify you've uploaded the correct files.")
            else:
                st.error(f"❌ Reconciliation failed: {result.get('error', 'Unknown error')}")

                # Show detailed parse errors if available
                if result.get('parse_errors'):
                    st.markdown("---")
                    st.subheader("🔍 Detailed Diagnostics")
                    st.markdown("**Individual file errors:**")
                    for error in result['parse_errors']:
                        # Format multi-line errors as code blocks
                        if '\n' in error:
                            st.code(error, language=None)
                        else:
                            st.markdown(f"- {error}")

                    st.info("""
                    **💡 Troubleshooting Tips:**
                    - Check Streamlit Cloud logs (Manage app → Logs) for column details
                    - Verify files aren't corrupted
                    - Try downloading and re-uploading files
                    - Check if files work in desktop version first
                    """)

    except Exception as e:
        progress_bar.progress(0)
        status_text.text("")
        st.error(f"❌ Error during reconciliation: {str(e)}")
        logger.exception("Reconciliation error")

# Information section
st.markdown("---")
st.subheader("ℹ️ How It Works")

col1, col2 = st.columns(2)

with col1:
    st.markdown("""
    **Matching Criteria:**

    Trades are matched on 6 fields:
    1. Bloomberg Ticker (normalized)
    2. CP Code (case-insensitive)
    3. Broker Code = TM Code (absolute value)
    4. Buy/Sell (normalized)
    5. Quantity (exact match)
    6. Price (0.001% tolerance)
    """)

with col2:
    st.markdown("""
    **Supported Brokers:**

    - **ICICI Securities** (Code: 7730)
      - Auto-detected from "Broker Code" column

    - **Kotak Securities** (Code: 8081)
      - Auto-detected from column structure

    - **IIFL Securities** (Code: 10975)
      - Auto-detected from "Broker Code" or column structure

    - **Axis Securities** (Code: 13872)
      - Auto-detected from "Broker Code" or column structure

    - **Equirus Securities** (Code: 13017)
      - Auto-detected from "Broker Code" or column structure

    - **Edelweiss Securities** (Code: 11933)
      - Auto-detected from "Broker Code" or column structure

    - **Nuvama Securities** (Code: 11933)
      - Auto-detected from "Broker Code" or filename
      - Uses same format as Edelweiss

    - **Morgan Stanley** (Code: 10542)
      - Auto-detected from "morgan" keyword in file
      - Handles files with header rows

    All brokers can be processed together in one run.
    """)

st.markdown("""
**Output Files:**

1. **Enhanced Clearing File** (CSV)
   - All original clearing columns preserved
   - Plus: Pure Brokerage AMT, Total Taxes, Trade Date, Match Status

2. **Reconciliation Report** (Excel with 4 sheets)
   - Sheet 1: Matched Trades (clearing format + 3 new columns)
   - Sheet 2: Unmatched Clearing (clearing format + 3 empty columns)
   - Sheet 3: Unmatched Broker (broker format with all data)
   - Sheet 4: Summary (totals and statistics)
""")

# Footer
st.markdown("---")
st.caption("🏦 Broker Reconciliation App | Version 1.0")
