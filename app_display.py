"""
Display functions for the Streamlit Trade Processing App
Handles all UI display tabs and visualization components
"""

import streamlit as st
import pandas as pd
from pathlib import Path
from datetime import datetime

# Import utilities
from app_utils import get_output_path

# Import processing functions (for buttons that trigger processing)
from app_processing import run_expiry_delivery_generation

# Import existing modules
from acm_mapper import ACMMapper

# Optional feature imports
try:
    from positions_grouper import PositionGrouper
    POSITION_GROUPER_AVAILABLE = True
except ImportError:
    POSITION_GROUPER_AVAILABLE = False

try:
    from simple_price_manager import get_price_manager
    SIMPLE_PRICE_MANAGER_AVAILABLE = True
except ImportError:
    SIMPLE_PRICE_MANAGER_AVAILABLE = False

def display_pipeline_overview():
    """Display flexible workflow system overview"""
    st.header("System Overview")

    # Show detected account
    if st.session_state.get('detected_account'):
        account = st.session_state.detected_account
        st.success(f"🔍 Detected Account: **{account['name']}** ({account['cp_code']})")

    st.markdown("---")

    # Flexible Workflow System
    st.subheader("📋 Available Workflows")
    st.caption("System automatically runs workflows based on uploaded files")

    col1, col2 = st.columns(2)

    with col1:
        # Full Pipeline
        st.markdown('<div class="stage-header">Full Trade Processing Pipeline</div>', unsafe_allow_html=True)
        st.info("""
        **Required Files:** Clearing Position + Clearing Trades

        **Stages:**
        1. **Strategy Processing** - FULO/FUSH assignment, trade splitting
        2. **ACM Mapping** - Field mapping for ACM system
        3. **Deliverables Calculation** - Physical delivery analysis
        4. **Expiry Delivery Reports** - Expiring position handling

        **Outputs:** ACM CSV, positions, deliverables, expiry reports
        """)

        if st.session_state.get('stage1_complete'):
            st.success("✅ Stage 1 Complete")
        if st.session_state.get('stage2_complete'):
            st.success("✅ Stage 2 Complete")
        if st.session_state.get('deliverables_complete'):
            st.success("✅ Deliverables Complete")
        if st.session_state.get('expiry_deliveries_complete'):
            st.success("✅ Expiry Delivery Complete")

    with col2:
        # PMS Reconciliation
        st.markdown('<div class="stage-header">PMS Position Reconciliation</div>', unsafe_allow_html=True)
        st.info("""
        **Required Files:** Clearing Position + PMS Position File

        **Modes:**
        - **Simple:** Current positions vs PMS
        - **Complex:** Pre-trade + Post-trade vs PMS (if clearing trades present)

        **Analysis:**
        - Position matching
        - Quantity mismatches
        - Missing positions

        **Output:** Reconciliation report with discrepancies
        """)

        if st.session_state.get('recon_complete'):
            recon_data = st.session_state.get('recon_data', {})
            pre_recon = recon_data.get('pre_trade', {})
            summary = pre_recon.get('summary', {})
            total_issues = summary.get('total_discrepancies', 0)

            if total_issues == 0:
                st.success(f"✅ Reconciliation Complete - All Matched!")
            else:
                st.warning(f"⚠️ Reconciliation Complete - {total_issues} discrepancies")

    st.markdown("---")

    # Additional Workflows
    col3, col4 = st.columns(2)

    with col3:
        st.markdown('<div class="stage-header">Broker Reconciliation</div>', unsafe_allow_html=True)
        st.info("""
        **Required Files:** Clearing Trades + Broker Trades (multiple OK)

        **Analysis:**
        - Trade breaks
        - Commission analysis
        - Brokerage calculations
        - Tax validation

        **Output:** 5-sheet Excel with detailed analysis
        """)

        if st.session_state.get('broker_recon_complete'):
            st.success("✅ Broker Recon Complete")

    with col4:
        st.markdown('<div class="stage-header">Price Management</div>', unsafe_allow_html=True)
        st.info("""
        **Sources:**
        - Override prices from CSV (default_stocks.csv)
        - Yahoo Finance auto-fetch
        - Manual price upload

        **Features:**
        - Missing symbol detection
        - Downloadable updated prices
        - Persistent price storage
        """)

        if st.session_state.get('price_manager'):
            pm = st.session_state.price_manager
            missing = len(pm.missing_symbols) if hasattr(pm, 'missing_symbols') else 0
            if missing > 0:
                st.warning(f"⚠️ {missing} symbols missing prices")
            else:
                st.success("✅ All prices loaded")

    st.markdown("---")

    # Quick Stats
    if st.session_state.get('stage1_complete'):
        st.subheader("📊 Processing Summary")
        cols = st.columns(4)

        stage1_data = st.session_state.get('dataframes', {}).get('stage1', {})

        with cols[0]:
            processed_trades = stage1_data.get('processed_trades')
            if processed_trades is not None:
                st.metric("Trades Processed", len(processed_trades))

        with cols[1]:
            final_positions = stage1_data.get('final_positions')
            if final_positions is not None:
                st.metric("Final Positions", len(final_positions))

        with cols[2]:
            if st.session_state.get('recon_complete'):
                recon_data = st.session_state.get('recon_data', {})
                pre_recon = recon_data.get('pre_trade', {})
                summary = pre_recon.get('summary', {})
                total_issues = summary.get('total_discrepancies', 0)
                st.metric("PMS Discrepancies", total_issues)

        with cols[3]:
            if st.session_state.get('broker_recon_complete'):
                st.metric("Broker Recon", "✅")

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
        if df.empty:
            st.info("No trades - running in position-only mode")
        else:
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
        if df.empty:
            st.info("No trades - running in position-only mode")
        else:
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
    """Display PMS reconciliation results with detailed discrepancies"""
    st.header("🔄 PMS Position Reconciliation")

    if not st.session_state.get('recon_complete'):
        st.info("Run the pipeline with PMS reconciliation enabled to see this analysis")
        return

    data = st.session_state.recon_data
    pre_recon = data['pre_trade']
    post_recon = data['post_trade']

    # Check if this is simple mode (same data for pre and post)
    is_simple_mode = (pre_recon == post_recon)

    if is_simple_mode:
        # Simple mode: Position + PMS only
        st.info("📊 Simple Reconciliation Mode (Current Positions vs PMS)")

        recon = pre_recon

        # Summary metrics
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("✅ Matched Positions", recon['summary']['matched_count'])
        with col2:
            st.metric("⚠️ Quantity Mismatches", recon['summary']['mismatch_count'])
        with col3:
            st.metric("❌ Total Discrepancies", recon['summary']['total_discrepancies'])

        st.divider()

        # Detailed discrepancies
        st.subheader("📋 Detailed Discrepancies")

        # Position mismatches (quantity differences)
        if recon.get('position_mismatches') and len(recon['position_mismatches']) > 0:
            with st.expander("⚠️ Quantity Mismatches", expanded=True):
                df = pd.DataFrame(recon['position_mismatches'])
                st.dataframe(df, use_container_width=True, hide_index=True, height=400)
                st.caption(f"**{len(df)} positions** with quantity differences between System and PMS")

        # Missing in PMS
        if recon.get('missing_in_pms') and len(recon['missing_in_pms']) > 0:
            with st.expander(f"❌ Missing in PMS ({len(recon['missing_in_pms'])} positions)", expanded=True):
                df = pd.DataFrame(recon['missing_in_pms'])
                st.dataframe(df, use_container_width=True, hide_index=True, height=300)
                st.caption("Positions in System but **not found** in PMS")

        # Missing in System
        if recon.get('missing_in_system') and len(recon['missing_in_system']) > 0:
            with st.expander(f"❌ Missing in System ({len(recon['missing_in_system'])} positions)", expanded=True):
                df = pd.DataFrame(recon['missing_in_system'])
                st.dataframe(df, use_container_width=True, hide_index=True, height=300)
                st.caption("Positions in PMS but **not found** in System")

        # Matched positions (collapsible)
        if recon.get('matched_positions') and len(recon['matched_positions']) > 0:
            with st.expander(f"✅ Perfectly Matched ({len(recon['matched_positions'])} positions)"):
                df = pd.DataFrame(recon['matched_positions'])
                st.dataframe(df, use_container_width=True, hide_index=True, height=300)
                st.caption("Positions with **exact** quantity match")

    else:
        # Complex mode: Pre-trade and Post-trade reconciliation
        st.info("📊 Complex Reconciliation Mode (Pre-Trade and Post-Trade vs PMS)")

        # Summary metrics comparison
        st.subheader("Summary Comparison")
        col1, col2 = st.columns(2)

        with col1:
            st.markdown("### Pre-Trade")
            st.metric("✅ Matched", pre_recon['summary']['matched_count'])
            st.metric("⚠️ Mismatches", pre_recon['summary']['mismatch_count'])
            st.metric("❌ Discrepancies", pre_recon['summary']['total_discrepancies'])

        with col2:
            st.markdown("### Post-Trade")
            st.metric("✅ Matched", post_recon['summary']['matched_count'])
            st.metric("⚠️ Mismatches", post_recon['summary']['mismatch_count'])
            st.metric("❌ Discrepancies", post_recon['summary']['total_discrepancies'])

        st.divider()

        # Detailed discrepancies in tabs
        st.subheader("📋 Detailed Discrepancies")

        tab1, tab2 = st.tabs(["Pre-Trade", "Post-Trade"])

        with tab1:
            # Pre-trade position mismatches
            if pre_recon.get('position_mismatches') and len(pre_recon['position_mismatches']) > 0:
                with st.expander("⚠️ Quantity Mismatches", expanded=True):
                    df = pd.DataFrame(pre_recon['position_mismatches'])
                    st.dataframe(df, use_container_width=True, hide_index=True, height=400)
                    st.caption(f"**{len(df)} positions** with quantity differences")

            # Missing in PMS
            if pre_recon.get('missing_in_pms') and len(pre_recon['missing_in_pms']) > 0:
                with st.expander(f"❌ Missing in PMS ({len(pre_recon['missing_in_pms'])} positions)", expanded=True):
                    df = pd.DataFrame(pre_recon['missing_in_pms'])
                    st.dataframe(df, use_container_width=True, hide_index=True, height=300)

            # Missing in System
            if pre_recon.get('missing_in_system') and len(pre_recon['missing_in_system']) > 0:
                with st.expander(f"❌ Missing in System ({len(pre_recon['missing_in_system'])} positions)", expanded=True):
                    df = pd.DataFrame(pre_recon['missing_in_system'])
                    st.dataframe(df, use_container_width=True, hide_index=True, height=300)

            # Matched positions
            if pre_recon.get('matched_positions') and len(pre_recon['matched_positions']) > 0:
                with st.expander(f"✅ Perfectly Matched ({len(pre_recon['matched_positions'])} positions)"):
                    df = pd.DataFrame(pre_recon['matched_positions'])
                    st.dataframe(df, use_container_width=True, hide_index=True, height=300)

        with tab2:
            # Post-trade position mismatches
            if post_recon.get('position_mismatches') and len(post_recon['position_mismatches']) > 0:
                with st.expander("⚠️ Quantity Mismatches", expanded=True):
                    df = pd.DataFrame(post_recon['position_mismatches'])
                    st.dataframe(df, use_container_width=True, hide_index=True, height=400)
                    st.caption(f"**{len(df)} positions** with quantity differences")

            # Missing in PMS
            if post_recon.get('missing_in_pms') and len(post_recon['missing_in_pms']) > 0:
                with st.expander(f"❌ Missing in PMS ({len(post_recon['missing_in_pms'])} positions)", expanded=True):
                    df = pd.DataFrame(post_recon['missing_in_pms'])
                    st.dataframe(df, use_container_width=True, hide_index=True, height=300)

            # Missing in System
            if post_recon.get('missing_in_system') and len(post_recon['missing_in_system']) > 0:
                with st.expander(f"❌ Missing in System ({len(post_recon['missing_in_system'])} positions)", expanded=True):
                    df = pd.DataFrame(post_recon['missing_in_system'])
                    st.dataframe(df, use_container_width=True, hide_index=True, height=300)

            # Matched positions
            if post_recon.get('matched_positions') and len(post_recon['matched_positions']) > 0:
                with st.expander(f"✅ Perfectly Matched ({len(post_recon['matched_positions'])} positions)"):
                    df = pd.DataFrame(post_recon['matched_positions'])
                    st.dataframe(df, use_container_width=True, hide_index=True, height=300)

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
        # Initialize default values in session state
        if 'include_ops_email' not in st.session_state:
            st.session_state.include_ops_email = True
        if 'email_additional_recipients' not in st.session_state:
            st.session_state.email_additional_recipients = ''

        # Checkbox for operations@aurigincm.com
        include_ops_email = st.checkbox(
            "✉️ operations@aurigincm.com",
            value=True,
            key='email_include_ops',
            help="Include operations@aurigincm.com in recipients"
        )

        additional_recipients = st.text_area(
            "Additional Recipients",
            placeholder="user1@example.com, user2@example.com",
            help="Enter additional email addresses (comma-separated)",
            key='email_additional_input',
            height=100
        )

    with col2:
        # Calculate total recipients based on widget values
        all_recipients = []

        # Add operations@aurigincm.com if checked
        if include_ops_email:
            all_recipients.append('operations@aurigincm.com')

        # Add additional recipients
        if additional_recipients and additional_recipients.strip():
            additional = [email.strip() for email in additional_recipients.split(',') if email.strip()]
            for email in additional:
                if email and email not in all_recipients:
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
        # Email subject suffix customization
        st.caption("📝 Subject suffix (optional)")

        # Preset options
        preset_options = ["None", "FnO position recon", "EOD FnO trade recon"]
        suffix_preset = st.radio(
            "Select preset:",
            options=preset_options,
            index=0,
            horizontal=False,
            key='email_suffix_radio'
        )

        # Custom suffix input
        custom_suffix = st.text_input(
            "Or custom suffix:",
            placeholder="e.g., Preliminary",
            key='email_custom_suffix',
            help="Custom suffix overrides preset selection"
        )

        # Determine final suffix based on widget values
        if custom_suffix and custom_suffix.strip():
            final_suffix = custom_suffix.strip()
        elif suffix_preset != "None":
            final_suffix = suffix_preset
        else:
            final_suffix = None

        # Store in session state for email sending
        st.session_state.email_subject_suffix = final_suffix

        # Show preview
        if final_suffix:
            st.caption(f"Preview: Aurigin | **{final_suffix}** | Date")

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

                # Get account prefix from validator (same way as file generation)
                account_prefix = ""
                if 'account_validator' in st.session_state and st.session_state.account_validator:
                    account_prefix = st.session_state.account_validator.get_account_prefix()
                account_prefix = account_prefix.rstrip('_') if account_prefix else ''

                # Extract trade date from processed trades if available
                trade_date_str = None
                if st.session_state.get('dataframes', {}).get('stage1'):
                    processed_trades = st.session_state['dataframes']['stage1'].get('processed_trades')
                    if processed_trades is not None and not processed_trades.empty:
                        from output_generator import OutputGenerator
                        temp_gen = OutputGenerator()
                        trade_date_str = temp_gen._extract_trade_date(processed_trades)

                # Use trade date if available, otherwise current date in DD-MMM-YYYY format
                if trade_date_str:
                    date_str = trade_date_str  # Already in DD-MMM-YYYY format
                else:
                    date_str = datetime.now().strftime("%d-%b-%Y")  # DD-MMM-YYYY format

                # Get fund name with proper fallback
                if account_prefix == 'AURIGIN':
                    fund_name = 'Aurigin'
                elif account_prefix:
                    fund_name = account_prefix
                else:
                    fund_name = 'Trade Processing'  # Default when no account prefix

                # Build subject - suffix replaces "Reports" if provided
                subject_suffix = st.session_state.get('email_subject_suffix')
                subject_label = subject_suffix if subject_suffix else "Reports"
                subject = f"{fund_name} | {subject_label} | {date_str}"

                # Build PMS Reconciliation summary if available
                pms_recon_section = ""
                if st.session_state.get('recon_complete') and st.session_state.get('recon_data'):
                    recon_data = st.session_state.recon_data

                    # Get pre-trade reconciliation data (or use it as single recon if both are same)
                    pre_recon = recon_data.get('pre_trade', {})
                    post_recon = recon_data.get('post_trade', {})

                    # Check if simple mode (pre == post)
                    is_simple_mode = (pre_recon == post_recon)

                    if is_simple_mode and pre_recon:
                        # Simple mode: single reconciliation
                        summary = pre_recon.get('summary', {})
                        total_pms = summary.get('total_pms_positions', 0)
                        total_system = summary.get('total_system_positions', 0)
                        mismatches = pre_recon.get('position_mismatches', [])
                        missing_in_pms = pre_recon.get('missing_in_pms', [])
                        missing_in_system = pre_recon.get('missing_in_system', [])

                        total_issues = len(mismatches) + len(missing_in_pms) + len(missing_in_system)

                        pms_recon_section = f"""
                <h3 style="color: {'#d32f2f' if total_issues > 0 else '#2e7d32'};">PMS Position Reconciliation:</h3>
                <ul>
                    <li><strong>Total PMS Positions:</strong> {total_pms}</li>
                    <li><strong>Total System Positions:</strong> {total_system}</li>
                    <li><strong>Quantity Mismatches:</strong> {len(mismatches)}</li>
                    <li><strong>Missing in PMS:</strong> {len(missing_in_pms)}</li>
                    <li><strong>Missing in System:</strong> {len(missing_in_system)}</li>
                    <li><strong>Total Issues:</strong> <span style="color: {'#d32f2f' if total_issues > 0 else '#2e7d32'};">{total_issues}</span></li>
                </ul>
"""

                        # Add list of failed positions if there are issues
                        if total_issues > 0:
                            pms_recon_section += """
                <h4 style="color: #d32f2f;">Failed Positions:</h4>
                <ul>
"""
                            # Add mismatches
                            for mismatch in mismatches[:10]:  # Limit to first 10
                                symbol = mismatch.get('Symbol', 'Unknown')
                                system_qty = mismatch.get('System_Position', 0)
                                pms_qty = mismatch.get('PMS_Position', 0)
                                pms_recon_section += f"""                    <li><strong>{symbol}:</strong> System={system_qty}, PMS={pms_qty} (Mismatch)</li>
"""

                            # Add missing in PMS
                            for missing in missing_in_pms[:5]:  # Limit to first 5
                                symbol = missing.get('Symbol', 'Unknown')
                                qty = missing.get('System_Position', 0)
                                pms_recon_section += f"""                    <li><strong>{symbol}:</strong> Qty={qty} (Missing in PMS)</li>
"""

                            # Add missing in system
                            for missing in missing_in_system[:5]:  # Limit to first 5
                                symbol = missing.get('Symbol', 'Unknown')
                                qty = missing.get('PMS_Position', 0)
                                pms_recon_section += f"""                    <li><strong>{symbol}:</strong> Qty={qty} (Missing in System)</li>
"""

                            if total_issues > 20:
                                pms_recon_section += f"""                    <li><em>... and {total_issues - 20} more (see attached report)</em></li>
"""

                            pms_recon_section += """                </ul>
"""

                    elif pre_recon and post_recon:
                        # Complex mode: show both pre and post trade
                        pre_mismatches = pre_recon.get('position_mismatches', [])
                        post_mismatches = post_recon.get('position_mismatches', [])
                        pre_missing_pms = pre_recon.get('missing_in_pms', [])
                        post_missing_pms = post_recon.get('missing_in_pms', [])

                        pre_issues = len(pre_mismatches) + len(pre_missing_pms) + len(pre_recon.get('missing_in_system', []))
                        post_issues = len(post_mismatches) + len(post_missing_pms) + len(post_recon.get('missing_in_system', []))

                        pms_recon_section = f"""
                <h3 style="color: {'#d32f2f' if (pre_issues > 0 or post_issues > 0) else '#2e7d32'};">PMS Reconciliation Summary:</h3>
                <ul>
                    <li><strong>Pre-Trade Issues:</strong> {pre_issues}</li>
                    <li><strong>Post-Trade Issues:</strong> {post_issues}</li>
                </ul>
                <p><em>See attached PMS Reconciliation report for details.</em></p>
"""

                body = f"""
                <h2>Trade Processing Reports</h2>

                <p>Please find the requested reports attached.</p>

{pms_recon_section}
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

