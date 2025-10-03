"""
Account Configuration Registry
Central configuration for all known trading accounts
"""

# Account Registry - Single source of truth for all account information
ACCOUNT_REGISTRY = {
    'ECASL0000094': {
        'name': 'AURIGIN',
        'cp_code': 'ECASL0000094',
        'display_color': '#2E7D32',  # Green
        'icon': '🟢',
        'description': 'AURIGIN Trading Account'
    },
    'CITI00007707': {
        'name': 'WAFRA',
        'cp_code': 'CITI00007707',
        'display_color': '#1565C0',  # Blue
        'icon': '🔵',
        'description': 'WAFRA Trading Account'
    },
    # Future accounts - add new entries here when available
    # Format:
    # 'CP_CODE_HERE': {
    #     'name': 'ACCOUNT_NAME',
    #     'cp_code': 'CP_CODE_HERE',
    #     'display_color': '#HEX_COLOR',
    #     'icon': '🟠',
    #     'description': 'Account Description'
    # }
}


def get_account_by_cp_code(cp_code: str):
    """Get account information by CP code"""
    return ACCOUNT_REGISTRY.get(cp_code)


def get_all_cp_codes():
    """Get list of all known CP codes"""
    return list(ACCOUNT_REGISTRY.keys())


def get_account_name(cp_code: str):
    """Get account name for a CP code, returns 'Unknown' if not found"""
    account = ACCOUNT_REGISTRY.get(cp_code)
    return account['name'] if account else 'Unknown'


def is_known_account(cp_code: str):
    """Check if CP code is in registry"""
    return cp_code in ACCOUNT_REGISTRY
