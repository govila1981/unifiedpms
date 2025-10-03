# 📊 Trade Processing Pipeline

A comprehensive trading strategy processing system for financial derivatives with automatic strategy assignment (FULO/FUSH), deliverables calculation, expiry-based grouping, and PMS reconciliation.

## 🌟 Features

### Core Processing
- **Multi-Format Support**: Bloomberg BOD, Contract, and MS formats
- **Automatic Strategy Assignment**: FULO (Following) and FUSH (Opposing) strategies
- **Bloomberg Ticker Generation**: Automatic ticker creation for derivatives
- **Trade Splitting**: Intelligent handling of partial offsetting trades
- **Position Tracking**: Real-time pre/post-trade position monitoring
- **Encrypted File Support**: Password-protected Excel file handling

### Advanced Analytics
- **Physical Deliverables Calculation**: ITM/OTM determination with spot prices
- **Intrinsic Value Analysis**: Options IV calculation in INR and USD
- **Expiry-Based Grouping**: Delivery obligations organized by expiry date
- **PMS Reconciliation**: Position verification against portfolio management systems
- **Multi-View Position Analysis**:
  - By Underlying Asset (with collapsible groups)
  - By Expiry Date
  - Pre vs Post Trade Comparison

### Integration & Outputs
- **ACM Format Mapping**: Automatic conversion to ACM ListedTrades format
- **Excel Report Generation**: Multi-sheet workbooks with formatting and Bloomberg formulas
- **Centralized Price Management**: Yahoo Finance integration + manual price upload
- **Position Caching**: Remembers files and prices for easy re-runs

## 🚀 Live Demo

**[Access the application here](https://your-app-url.streamlit.app)** *(Update URL after deployment)*

## 📋 Requirements

- Python 3.8+
- Streamlit >= 1.28.0
- pandas >= 2.0.0
- openpyxl >= 3.1.0
- yfinance >= 0.2.33
- msoffcrypto-tool >= 5.0.0

See `requirements.txt` for complete list

## 💻 Local Installation

### Quick Start
```bash
# Clone the repository
git clone https://github.com/YOUR_USERNAME/trade-processing-pipeline.git
cd trade-processing-pipeline

# Install dependencies
pip install -r requirements.txt

# Run the application
streamlit run app.py
```

### With Virtual Environment (Recommended)
```bash
# Create virtual environment
python -m venv venv

# Activate virtual environment
# Windows:
venv\Scripts\activate
# Mac/Linux:
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Run the application
streamlit run app.py
```

## 📖 Usage Guide

### Basic Workflow

1. **Upload Files** (Sidebar)
   - Position File (BOD/Contract/MS format)
   - Trade File (MS format)
   - Mapping File (use default `futures mapping.csv` or upload custom)

2. **Set Exchange Rate**
   - Enter USD/INR rate (default: 88.0)

3. **Fetch Prices**
   - Click "📊 Fetch Yahoo Prices" for automatic price fetch
   - Or upload manual price file (CSV/Excel with Symbol and Price columns)

4. **Optional: Enable PMS Reconciliation**
   - Check the box if you want to compare with PMS
   - Upload PMS file

5. **Run Pipeline**
   - Click "⚡ Run Complete Enhanced Pipeline"
   - All features run automatically (deliverables, expiry reports, etc.)

6. **View Results**
   - Navigate through tabs to see different analyses
   - Download outputs from the Downloads tab

### Sticky File Behavior

The app remembers your files between runs:
- ✅ Upload position file once → cached for future runs
- ✅ Upload new trade file → uses cached position file
- ✅ Prices persist in session
- ✅ Just upload new trade file and re-run for "what-if" scenarios

### Re-running with New Prices

Keep same files but update prices:
1. Files remain cached (position, trade, mapping)
2. Click "Fetch Yahoo Prices" or upload new price file
3. Click "Run Complete Enhanced Pipeline"
4. All calculations update with new prices

## 📂 Project Structure

```
trade-processing-pipeline/
├── app.py                          # Main Streamlit application
├── input_parser.py                 # Position file parser
├── Trade_Parser.py                 # Trade file parser
├── position_manager.py             # Position state management
├── trade_processor.py              # Strategy assignment logic
├── output_generator.py             # File output generation
├── acm_mapper.py                   # ACM format conversion
├── deliverables_calculator.py      # Deliverables & IV calculation
├── positions_grouper.py            # Position grouping utilities
├── simple_price_manager.py         # Centralized price management
├── enhanced_recon_module.py        # PMS reconciliation
├── expiry_delivery_module.py       # Expiry delivery generation
├── encrypted_file_handler.py       # Password-protected file handling
├── bloomberg_ticker_generator.py   # Ticker generation utilities
├── excel_writer.py                 # Excel formatting
├── price_manager.py                # Price fetching
├── default_stocks.csv              # Default symbol mappings
├── futures mapping.csv             # Default futures lot sizes
├── requirements.txt                # Python dependencies
├── .gitignore                      # Git ignore rules
└── .streamlit/
    └── config.toml                 # Streamlit configuration
```

## 📊 Output Files

### Stage 1: Strategy Processing
- `output_1_parsed_trades_[timestamp].csv` - Raw parsed trades
- `output_2_starting_positions_[timestamp].csv` - Initial positions
- `output_3_processed_trades_[timestamp].csv` - Trades with strategies
- `output_4_final_positions_[timestamp].csv` - Post-trade positions
- `summary_report_[timestamp].txt` - Processing summary

### Stage 2: ACM Mapping
- `acm_listedtrades_[timestamp].csv` - ACM format output
- `acm_listedtrades_[timestamp].xlsx` - Excel version
- `acm_listedtrades_[timestamp]_errors.csv` - Validation errors
- `acm_schema_used_[timestamp].xlsx` - Schema reference

### Deliverables Reports (Auto-Generated)
- `DELIVERABLES_REPORT_[timestamp].xlsx` - Physical deliverables analysis
  - PRE_Master_All_Expiries
  - POST_Master_All_Expiries
  - PRE_Expiry_[date] (per expiry)
  - POST_Expiry_[date] (per expiry)
  - PRE_IV_All_Expiries
  - POST_IV_All_Expiries
  - Comparison sheet

### Expiry Deliveries (Auto-Generated)
- `EXPIRY_[date].xlsx` - Per-expiry delivery reports
  - Physical delivery trades per expiry
  - Tax calculations (STT, stamp duty)
  - Comprehensive delivery obligations

### Position Grouping
- `positions_by_underlying_[timestamp].xlsx` - Grouped position analysis
  - Summary sheet
  - Master_All_Positions (collapsible by underlying)

### PMS Reconciliation (Optional)
- `PMS_RECONCILIATION_[timestamp].xlsx` - Position reconciliation
  - Executive Summary
  - Pre-Trade reconciliation
  - Post-Trade reconciliation
  - Trade Impact Analysis

## 🔧 Configuration

### Input File Formats

#### Position File
Supported formats:
- **BOD Format**: Bloomberg Beginning of Day format
- **Contract Format**: Contract-based position format
- **MS Format**: Morgan Stanley format

Must include: Symbol, Quantity, Expiry (for derivatives)

#### Trade File
MS Format with columns:
- Symbol
- B/S (Buy/Sell)
- Quantity
- Expiry Date (for derivatives)
- Strike Price (for options)
- Option Type (Call/Put for options)

#### Mapping File (Optional)
CSV format with columns: Symbol, Ticker, Underlying, Exchange, Lot_Size

Example:
```csv
Symbol,Ticker,Underlying,Exchange,Lot_Size
NIFTY,NZ,NIFTY Index,NSE,50
BANKNIFTY,AF,BANKNIFTY Index,NSE,25
RELIANCE,RIL,RELIANCE Industries,NSE,1
```

If not provided, uses `futures mapping.csv` from repository.

#### PMS File Format (Optional)
For reconciliation, PMS file should have:
- Column: Symbol/Ticker (Bloomberg format)
- Column: Position/Quantity (numeric)

Example:
```csv
Symbol,Position
RIL IS Equity,100
NZH5 Index,-50
TCS IS 03/27/25 C3500 Equity,25
```

### Settings

**USD/INR Rate**: Default 88.0 (adjustable in sidebar)

**Price Sources**:
1. Yahoo Finance (automatic)
   - NSE stocks: Symbol.NS
   - Indices: ^NSEI, ^NSEBANK, ^NSEMDCP50
2. Manual Upload (overrides Yahoo)
   - CSV/Excel with Symbol/Ticker and Price columns

**Lot Sizes** (priority order):
1. Position/Trade file (if Lot Size column exists)
2. Mapping file (futures mapping.csv)
3. Default: 1

## 🎯 Key Concepts

### Strategy Types
- **FULO (Following)**: Trade in same direction as position
- **FUSH (Opposing)**: Trade opposite to position (offsetting)
- Trades may be split if partially offsetting

### Deliverables Calculation

**Net Deliverable Formula:**
```
Net Deliverable = Futures Position
                + ITM Call Positions
                - ITM Put Positions
```

**By Security Type:**
- **Futures**: Always deliver (Deliverable = Position)
- **Call Options** (ITM when Spot > Strike): Deliverable = Position (long underlying)
- **Put Options** (ITM when Spot < Strike): Deliverable = -Position (short underlying)
- **OTM Options**: Deliverable = 0 (expire worthless)

**Intrinsic Value:**
- Call IV = Max(0, Spot - Strike) × Position × Lot Size
- Put IV = Max(0, Strike - Spot) × Position × Lot Size

## 🔄 Reconciliation Process

1. **Position Matching**: Compares Bloomberg tickers between system and PMS
2. **Discrepancy Types**:
   - **Matched**: Same position in both systems
   - **Mismatch**: Different quantities for same ticker
   - **Missing in PMS**: Position in system but not in PMS
   - **Missing in System**: Position in PMS but not in system
3. **Impact Analysis**: Shows how trades improved or deteriorated reconciliation

## 🐛 Troubleshooting

### "No positions found"
- Check file format matches expected format (BOD/Contract/MS)
- Verify file is not corrupted or password-protected
- Try different format if uncertain

### "Unmapped symbols"
- Upload custom mapping file with missing symbols
- Check symbol names match exactly (case-sensitive)
- Review `MISSING_MAPPINGS_[timestamp].csv` output
- Use `MAPPING_TEMPLATE_[timestamp].csv` to add to mapping file

### "Price not found" / Spot Price N/A
- Click "📊 Fetch Yahoo Prices" to refresh
- Upload manual price file with missing symbols
- Check underlying format matches price manager keys
- Verify Bloomberg codes match between files and price data

### Price Fetching Issues
- Ensure internet connection for Yahoo Finance
- Check symbol format (NSE stocks need .NS suffix)
- Indices use special symbols (^NSEI, ^NSEBANK)
- Yahoo Finance may have rate limits

### Memory Issues on Streamlit Cloud
- Streamlit Cloud free tier: 1GB RAM limit
- Process smaller batches if needed
- Use CSV outputs for large datasets

### Reconciliation Mismatches
- Verify Bloomberg ticker format matches exactly
- Check for trailing spaces in files
- Ensure position signs are correct (+/- for long/short)

## 🤝 Contributing

This is a private/internal tool. For issues or feature requests, create an issue in this repository.

## 📝 License

Proprietary - Internal Use Only

## 🔗 Resources

- [Streamlit Documentation](https://docs.streamlit.io/)
- [pandas Documentation](https://pandas.pydata.org/docs/)
- [yfinance Documentation](https://github.com/ranaroussi/yfinance)
- [DEPLOYMENT_CHECKLIST.md](./DEPLOYMENT_CHECKLIST.md) - Deployment guide

## 📧 Support

For questions or support:
- Create an issue in this repository
- Contact: [Your contact information]

---

**Version**: 4.0
**Last Updated**: 2025-01-10
**Powered by**: Streamlit + Python

## 🔒 Important Notes

- Yahoo Finance data is for reference only
- Always verify deliverables with official exchange data
- PMS reconciliation should be reviewed by operations team
- Keep sensitive position files secure
- All calculations use simple date format (YYYY-MM-DD) for consistency
