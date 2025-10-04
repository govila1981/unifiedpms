"""
Build script to create standalone .exe for Trade Processing Pipeline
Run this script to generate the executable
"""

import PyInstaller.__main__
import os
import sys
from pathlib import Path

def build_exe():
    """Build the standalone executable"""

    print("=" * 70)
    print("  Building Standalone Executable")
    print("  Trade Processing Pipeline v5.0")
    print("=" * 70)
    print()

    # Get current directory
    current_dir = Path(__file__).parent

    # List of all Python modules to include
    modules = [
        'unified-streamlit-app.py',
        'input_parser.py',
        'Trade_Parser.py',
        'position_manager.py',
        'trade_processor.py',
        'output_generator.py',
        'acm_mapper.py',
        'deliverables_calculator.py',
        'enhanced_recon_module.py',
        'expiry_delivery_module.py',
        'account_validator.py',
        'account_config.py',
        'trade_reconciliation.py',
        'broker_parser.py',
        'broker_config.py',
        'simple_price_manager.py',
        'price_manager.py',
        'encrypted_file_handler.py',
        'positions_grouper.py',
        'bloomberg_ticker_generator.py',
        'excel_writer.py',
        'email_sender.py',
        'email_config.py',
    ]

    # Data files to include
    data_files = [
        ('futures mapping.csv', '.'),
    ]

    # Check if all modules exist
    missing = []
    for module in modules:
        if not (current_dir / module).exists():
            missing.append(module)

    if missing:
        print("WARNING: The following modules are missing:")
        for m in missing:
            print(f"  - {m}")
        print()

    # PyInstaller arguments
    args = [
        'app_launcher.py',                          # Main script
        '--name=TradeProcessingPipeline',           # Exe name
        '--onefile',                                 # Single executable
        '--windowed',                                # No console window (can change to --console for debug)
        '--icon=NONE',                               # Add icon file if you have one

        # Add all modules
        *[f'--add-data={m};.' for m in modules if (current_dir / m).exists()],

        # Add data files
        *[f'--add-data={src};{dst}' for src, dst in data_files if (current_dir / src).exists()],

        # Hidden imports (packages that PyInstaller might miss)
        '--hidden-import=streamlit',
        '--hidden-import=pandas',
        '--hidden-import=openpyxl',
        '--hidden-import=yfinance',
        '--hidden-import=sendgrid',
        '--hidden-import=msoffcrypto',
        '--hidden-import=pkg_resources.py2_warn',
        '--hidden-import=streamlit.web.cli',
        '--hidden-import=streamlit.runtime.scriptrunner.magic_funcs',

        # Collect all submodules
        '--collect-all=streamlit',
        '--collect-all=altair',
        '--collect-all=plotly',

        # Additional options
        '--clean',                                   # Clean cache
        '--noconfirm',                              # Overwrite without asking

        # Output directory
        '--distpath=dist',
        '--workpath=build',
        '--specpath=.',
    ]

    print("Starting PyInstaller build...")
    print()
    print("This may take 5-10 minutes...")
    print()

    try:
        PyInstaller.__main__.run(args)

        print()
        print("=" * 70)
        print("  BUILD SUCCESSFUL!")
        print("=" * 70)
        print()
        print(f"  Executable created at: dist/TradeProcessingPipeline.exe")
        print()
        print("  File size: ~500-800 MB (includes Python + all dependencies)")
        print()
        print("  To distribute:")
        print("  1. Copy 'TradeProcessingPipeline.exe' to target PC")
        print("  2. Double-click to run - no installation needed")
        print("  3. First launch may be slow (10-20 seconds)")
        print()
        print("  Requirements on target PC:")
        print("  - Windows 7 or later")
        print("  - Web browser (Chrome, Firefox, Edge)")
        print("  - Internet connection (for Yahoo Finance prices)")
        print()
        print("=" * 70)

    except Exception as e:
        print()
        print("=" * 70)
        print("  BUILD FAILED!")
        print("=" * 70)
        print()
        print(f"Error: {e}")
        print()
        print("Common issues:")
        print("1. PyInstaller not installed: pip install pyinstaller")
        print("2. Missing dependencies: pip install -r requirements.txt")
        print("3. Antivirus blocking: Add exception for Python and PyInstaller")
        print()
        sys.exit(1)

if __name__ == "__main__":
    build_exe()
