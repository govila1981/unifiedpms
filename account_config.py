"""
Account Configuration Registry
Central configuration for all known trading accounts
"""

# Account Registry - Single source of truth for all account information
ACCOUNT_REGISTRY = {
    'ECASL0000094': {
        'name': 'AURIGIN',
        'cp_code': 'ECASL0000094',
        'entity_code': None,  # No entity code for Aurigin
        'display_color': '#2E7D32',  # Green
        'icon': '🟢',
        'description': 'AURIGIN Trading Account'
    },
    'CITI00007707': {
        'name': 'WAFRA',
        'cp_code': 'CITI00007707',
        'entity_code': 'WASIAOPPSL',  # MS entity code for Wafra
        'display_color': '#1565C0',  # Blue
        'icon': '🔵',
        'description': 'WAFRA Trading Account'
    },
    # Future accounts - add new entries here when available
    # Format:
    # 'CP_CODE_HERE': {
    #     'name': 'ACCOUNT_NAME',
    #     'cp_code': 'CP_CODE_HERE',
    #     'entity_code': 'ENTITY_CODE_HERE',  # Optional: MS entity code
    #     'display_color': '#HEX_COLOR',
    #     'icon': '🟠',
    #     'description': 'Account Description'
    # }
}

# Entity Code to CP Code mapping (for MS position files)
ENTITY_CODE_MAP = {
    'WASIAOPPSL': 'CITI00007707',  # Wafra
    # Add more entity code mappings here as needed
}


def get_account_by_cp_code(cp_code: str):
    """Get account information by CP code"""
    return ACCOUNT_REGISTRY.get(cp_code)


def get_account_by_entity_code(entity_code: str):
    """Get account information by entity code (MS files)"""
    cp_code = ENTITY_CODE_MAP.get(entity_code)
    if cp_code:
        return ACCOUNT_REGISTRY.get(cp_code)
    return None


def get_all_cp_codes():
    """Get list of all known CP codes"""
    return list(ACCOUNT_REGISTRY.keys())


def get_all_entity_codes():
    """Get list of all known entity codes"""
    return list(ENTITY_CODE_MAP.keys())


def get_account_name(cp_code: str):
    """Get account name for a CP code, returns 'Unknown' if not found"""
    account = ACCOUNT_REGISTRY.get(cp_code)
    return account['name'] if account else 'Unknown'


def is_known_account(cp_code: str):
    """Check if CP code is in registry"""
    return cp_code in ACCOUNT_REGISTRY


def is_known_entity_code(entity_code: str):
    """Check if entity code is in registry"""
    return entity_code in ENTITY_CODE_MAP


def get_account_by_name(name: str):
    """Get account information by name (case-insensitive)"""
    name_upper = name.upper()
    for account in ACCOUNT_REGISTRY.values():
        if account['name'].upper() == name_upper:
            return account
    return None
