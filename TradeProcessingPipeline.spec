# -*- mode: python ; coding: utf-8 -*-
from PyInstaller.utils.hooks import collect_all

datas = [('unified-streamlit-app.py', '.'), ('input_parser.py', '.'), ('Trade_Parser.py', '.'), ('position_manager.py', '.'), ('trade_processor.py', '.'), ('output_generator.py', '.'), ('acm_mapper.py', '.'), ('deliverables_calculator.py', '.'), ('enhanced_recon_module.py', '.'), ('expiry_delivery_module.py', '.'), ('account_validator.py', '.'), ('account_config.py', '.'), ('trade_reconciliation.py', '.'), ('broker_parser.py', '.'), ('broker_config.py', '.'), ('simple_price_manager.py', '.'), ('price_manager.py', '.'), ('encrypted_file_handler.py', '.'), ('positions_grouper.py', '.'), ('bloomberg_ticker_generator.py', '.'), ('excel_writer.py', '.'), ('email_sender.py', '.'), ('email_config.py', '.'), ('futures mapping.csv', '.')]
binaries = []
hiddenimports = ['streamlit', 'pandas', 'openpyxl', 'yfinance', 'sendgrid', 'msoffcrypto', 'pkg_resources.py2_warn', 'streamlit.web.cli', 'streamlit.runtime.scriptrunner.magic_funcs']
tmp_ret = collect_all('streamlit')
datas += tmp_ret[0]; binaries += tmp_ret[1]; hiddenimports += tmp_ret[2]
tmp_ret = collect_all('altair')
datas += tmp_ret[0]; binaries += tmp_ret[1]; hiddenimports += tmp_ret[2]
tmp_ret = collect_all('plotly')
datas += tmp_ret[0]; binaries += tmp_ret[1]; hiddenimports += tmp_ret[2]


a = Analysis(
    ['app_launcher.py'],
    pathex=[],
    binaries=binaries,
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name='TradeProcessingPipeline',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon='NONE',
)
