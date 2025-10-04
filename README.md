# 📊 Unified Trade Processing Pipeline

A comprehensive trade processing system for financial derivatives with automatic strategy assignment, broker reconciliation, deliverables calculation, email automation, and multi-account support.

## 🌟 Key Features

### 🎯 Core Trade Processing
- **Multi-Format Support**: Bloomberg BOD, Contract, and MS formats with auto-detection
- **Encrypted File Handling**: Automatic password detection for encrypted Excel files (Aurigin2017/Aurigin2024)
- **Account Validation**: Auto-detection of account prefix from CP codes in files
- **Automatic Strategy Assignment**: FULO (Following) and FUSH (Opposing) strategies
- **Bloomberg Ticker Generation**: Automatic ticker creation for all derivatives
- **Intelligent Trade Splitting**: Handles partial offsetting trades with precision
- **Position Tracking**: Real-time pre/post-trade position monitoring with change detection

### 📨 Email Automation
- **SendGrid Integration**: Automated email reports via SendGrid API
- **Smart Recipients**: Toggle operations@ email + add custom recipients
- **Customizable Subjects**: Preset suffixes (FnO position recon, EOD FnO trade recon) + custom text
- **Pre vs Post Summary**: Inline table showing position changes in email body
- **Selective Attachments**: Choose which reports to attach (5MB file size warnings)
- **Trade Date Formatting**: All emails show trade date in DD-MMM-YYYY format (e.g., 03-Oct-2025)

### 🔄 Broker Reconciliation
- **Multi-Broker Support**: Axis, ICICI, Kotak, Zerodha with auto-detection
- **Enhanced Clearing File**: Adds Comms, Taxes, and TD columns to clearing file
- **4-Sheet Reconciliation Report**:
  - Matched Trades (with commissions and taxes)
  - Unmatched Clearing Trades
  - Unmatched Broker Trades
  - Trade Breaks Analysis
- **Match Rate Calculation**: Tracks reconciliation percentage

### 📊 Advanced Analytics
- **Physical Deliverables**: ITM/OTM determination with live spot prices
- **Intrinsic Value Analysis**: Options IV calculation in INR and USD
- **Expiry-Based Grouping**: Delivery obligations organized by expiry date
- **PMS Reconciliation**: Position verification against portfolio systems
- **Pre vs Post Comparison**: Visual tables showing position changes by underlying

### 🎨 Smart Outputs
- **Account-Prefixed Files**: All files automatically prefixed with account name (e.g., AURIGIN_)
- **Trade Date Naming**: Files named with trade date (DD-MMM-YYYY) not processing date
- **ACM Format Export**: Ready-to-upload ACM ListedTrades format
- **Multi-Sheet Excel Reports**: Formatted workbooks with Bloomberg formulas
- **Centralized Price Manager**: Yahoo Finance + manual price upload with caching

## 🚀 Quick Start

### Local Installation

```bash
# Clone repository
git clone <repository-url>
cd unifiedpms

# Create virtual environment (recommended)
python -m venv venv
venv\Scripts\activate  # Windows
source venv/bin/activate  # Mac/Linux

# Install dependencies
pip install -r requirements.txt

# Run the application
streamlit run unified-streamlit-app.py
```

### Railway Deployment

```bash
# Railway will auto-detect Python and use:
# Build: pip install -r requirements.txt
# Start: streamlit run unified-streamlit-app.py --server.port $PORT

# Set environment variables in Railway:
SENDGRID_API_KEY=your_api_key
SENDGRID_FROM_EMAIL=your_email@domain.com
```

## 📋 Requirements

- Python 3.8+
- Streamlit >= 1.28.0
- pandas >= 2.0.0
- openpyxl >= 3.1.0
- yfinance >= 0.2.33
- msoffcrypto-tool >= 5.0.0
- sendgrid >= 6.11.0 (for email features)

See `requirements.txt` for complete list

## 📖 Usage Guide

### Basic Workflow

1. **Upload Files** (Sidebar)
   - Position File: BOD/Contract/MS format (password-protected files auto-detected)
   - Trade File: MS format (encrypted files supported)
   - Optional: Futures mapping file

2. **Account Validation**
   - System auto-detects account from CP codes
   - All output files prefixed with account name (e.g., AURIGIN_)

3. **Set Exchange Rate**
   - Enter USD/INR rate (default: 88.0)

4. **Fetch Prices**
   - Auto-fetch from Yahoo Finance
   - Or upload manual price CSV/Excel

5. **Run Stage 1: Trade Processing**
   - Processes trades with strategy assignment
   - Generates enhanced clearing file

6. **Optional: Broker Reconciliation**
   - Upload broker contract notes (auto-detects broker)
   - Generate reconciliation report with commissions/taxes

7. **Run Stage 2: ACM Mapping**
   - Converts to ACM ListedTrades format
   - Validates all required fields

8. **Generate Reports**
   - Deliverables calculation
   - Expiry delivery reports
   - Positions by underlying

9. **Send Email**
   - Select reports to attach
   - Choose recipients and subject suffix
   - Send via SendGrid

### Encrypted Files

The system automatically handles password-protected Excel files:
- Tries known passwords: `Aurigin2017`, `Aurigin2024`
- Only prompts for password if known passwords fail
- Works for both position and trade files

### File Caching

Files persist in session for easy re-runs:
- ✅ Upload position file once → cached
- ✅ Upload new trade file → uses cached position
- ✅ Prices persist in session
- ✅ Re-run with different parameters without re-uploading

## 📂 Project Structure

```
unifiedpms/
├── unified-streamlit-app.py        # Main application (use this)
├── cli-pipeline.py                 # Command-line interface
│
├── Core Processing Modules
├── input_parser.py                 # Position file parser
├── Trade_Parser.py                 # Trade file parser
├── position_manager.py             # Position state management
├── trade_processor.py              # Strategy assignment logic
├── output_generator.py             # File output generation
│
├── Advanced Features
├── account_validator.py            # Account prefix detection
├── trade_reconciliation.py         # Broker reconciliation
├── broker_parser.py                # Multi-broker parser
├── broker_config.py                # Broker registry
├── deliverables_calculator.py      # Deliverables & IV
├── expiry_delivery_module.py       # Expiry deliveries
├── enhanced_recon_module.py        # PMS reconciliation
│
├── Integration & Support
├── acm_mapper.py                   # ACM format conversion
├── email_sender.py                 # SendGrid integration
├── email_config.py                 # Email templates
├── encrypted_file_handler.py       # Password-protected files
├── simple_price_manager.py         # Price management
├── positions_grouper.py            # Position grouping
├── bloomberg_ticker_generator.py   # Ticker generation
├── excel_writer.py                 # Excel formatting
│
├── Configuration Files
├── account_config.py               # Account registry
├── futures mapping.csv             # Default lot sizes
├── requirements.txt                # Python dependencies
└── README.md                       # This file
```

## 📊 Output Files

All files are prefixed with account name and use trade date (DD-MMM-YYYY format).

### Stage 1: Trade Processing
- `AURIGIN_stage1_1_parsed_trades_03-Oct-2025.csv`
- `AURIGIN_stage1_2_starting_positions_03-Oct-2025.csv`
- `AURIGIN_stage1_3_processed_trades_03-Oct-2025.csv`
- `AURIGIN_stage1_4_final_positions_03-Oct-2025.csv`
- `AURIGIN_summary_report_03-Oct-2025.txt`
- `AURIGIN_final_enhanced_clearing_03-Oct-2025.csv` - With Comms, Taxes, TD columns

### Stage 2: ACM Mapping
- `AURIGIN_acm_listedtrades_03-Oct-2025.csv`
- `AURIGIN_acm_listedtrades_03-Oct-2025_errors.csv`
- `AURIGIN_acm_schema_used_03-Oct-2025.xlsx`

### Broker Reconciliation
- `AURIGIN_broker_recon_report_03-Oct-2025.xlsx` - 4 sheets
- `AURIGIN_clearing_enhanced_03-Oct-2025.csv` - Enhanced clearing file

### Deliverables & Analytics
- `AURIGIN_DELIVERABLES_REPORT_03-Oct-2025.xlsx`
  - Pre/Post Master sheets (all expiries)
  - Per-expiry sheets
  - IV analysis sheets
  - Comparison sheet
- `AURIGIN_positions_by_underlying_03-Oct-2025.xlsx`
  - Collapsible groups by underlying
  - Summary sheet

### Expiry Deliveries
- `AURIGIN_EXPIRY_DELIVERY_27-Jan-2025.xlsx` - Per expiry
  - Physical delivery obligations
  - Tax calculations (STT, stamp duty)

## 📧 Email Configuration

### Setup (One-time)

1. **Get SendGrid API Key**
   - Sign up at sendgrid.com
   - Create API key with Mail Send permission
   - Verify sender email address

2. **Configure Environment Variables**

**For Local Development (.env file):**
```bash
SENDGRID_API_KEY=SG.xxxxxxxxxxxxxx
SENDGRID_FROM_EMAIL=your_email@domain.com
SENDGRID_FROM_NAME=Aurigin Trade Processing
```

**For Railway Deployment:**
Add in Railway dashboard → Variables:
```
SENDGRID_API_KEY=SG.xxxxxxxxxxxxxx
SENDGRID_FROM_EMAIL=your_email@domain.com
SENDGRID_FROM_NAME=Aurigin Trade Processing
```

### Using Email Features

1. **Process Trades** (Stage 1, Stage 2, etc.)
2. **Navigate to Email Reports Tab**
3. **Configure Recipients**:
   - ☑ operations@aurigincm.com (toggle on/off)
   - Add additional recipients (comma-separated)
4. **Select Subject Suffix** (optional):
   - ( ) None
   - ( ) FnO position recon
   - ( ) EOD FnO trade recon
   - Or enter custom suffix
5. **Select Reports to Attach**
   - Check reports to include
   - System warns if file > 5MB or total > 25MB
6. **Send Email**

### Email Subject Format

```
{Fund Name} | {Suffix} | {Trade Date}
```

Examples:
- `Aurigin | EOD FnO trade recon | 03-Oct-2025`
- `Aurigin | FnO position recon | 03-Oct-2025`
- `Aurigin | Reports | 03-Oct-2025` (no suffix)

## 🔧 Configuration

### Input File Formats

#### Position File
Supported formats (auto-detected):
- **BOD Format**: Bloomberg Beginning of Day
- **Contract Format**: Contract-based positions
- **MS Format**: Morgan Stanley format

Required columns:
- Symbol/Ticker
- Quantity/Position
- Expiry Date (for derivatives)

#### Trade File
MS Format with columns:
- Symbol
- B/S (Buy/Sell)
- Quantity
- Expiry Date
- Strike Price (options)
- Option Type (Call/Put)
- TD column (trade date)

#### Broker Files
Supported brokers (auto-detected from filename/content):
- Axis Securities
- ICICI Securities
- Kotak Securities
- Zerodha

File should be contract note in Excel/CSV format.

### Mapping File
CSV format: `Symbol,Ticker,Underlying,Exchange,Lot_Size`

Example:
```csv
Symbol,Ticker,Underlying,Exchange,Lot_Size
NIFTY,NZ,NIFTY Index,NSE,50
BANKNIFTY,AF,BANKNIFTY Index,NSE,25
```

## 🎯 Key Concepts

### Strategy Assignment
- **FULO (Following)**: Trade in same direction as position
- **FUSH (Opposing)**: Trade opposite to position (offsetting)
- Trades split automatically if partially offsetting

### Deliverables Formula
```
Net Deliverable = Futures Position
                + ITM Call Positions
                - ITM Put Positions
```

**By Security Type:**
- **Futures**: Always deliver (Deliverable = Position)
- **Call Options** (ITM when Spot > Strike): +Position
- **Put Options** (ITM when Spot < Strike): -Position
- **OTM Options**: 0 (expire worthless)

### Account Detection
System auto-detects account from CP code in files:
- Position file CP → Account prefix
- Trade file CP → Validates consistency
- Mismatch → Warning + manual selection

## 🐛 Troubleshooting

### Encrypted Files
**Issue**: "File is password protected"
- System tries: Aurigin2017, Aurigin2024
- If both fail, prompts for password
- Works for .xls and .xlsx files

### Email Not Sending
1. Check environment variables are set
2. Verify SendGrid API key is valid
3. Confirm sender email is verified in SendGrid
4. Check file sizes (max 5MB per file, 25MB total)
5. Review logs for detailed error messages

### Account Prefix Issues
- Upload files with CP codes
- CP codes must match between position and trade files
- Manual override available if auto-detection fails

### Broker Reconciliation
- Upload correct broker file (auto-detection may fail with custom formats)
- Ensure date formats match
- Check ticker formats are consistent

### Price Fetching
- Yahoo Finance requires internet connection
- NSE stocks need .NS suffix
- Indices use special codes (^NSEI, ^NSEBANK)
- Manual price upload always overrides Yahoo

### Railway/Cloud Deployment
- Ensure all environment variables are set
- Check Railway logs for errors
- Streamlit Cloud/Railway have 1GB RAM limit
- Large files may need local processing

## 🔐 Security Notes

- Keep `.env` file secure (never commit to git)
- SendGrid API keys should have minimal permissions
- Password-protected files use msoffcrypto (secure)
- All email addresses validated before sending
- Files uploaded to Streamlit Cloud are temporary

## 📝 Version History

**Version 5.0** (Current)
- ✅ Broker reconciliation with multi-broker support
- ✅ Email automation via SendGrid
- ✅ Account prefix auto-detection
- ✅ Encrypted file handling
- ✅ Trade date extraction (DD-MMM-YYYY)
- ✅ Pre vs Post trade summary in emails
- ✅ Railway deployment ready

**Version 4.0**
- PMS reconciliation
- Expiry delivery module
- Enhanced position grouping

**Version 3.0**
- Deliverables calculation
- ACM format mapping
- Multi-view position analysis

## 🤝 Support

For issues or questions:
- Create issue in repository
- Contact: operations@aurigincm.com

## 📝 License

Proprietary - Internal Use Only

---

**Last Updated**: October 2025
**Powered by**: Streamlit + Python
**Deployment**: Railway + Streamlit Cloud Compatible
