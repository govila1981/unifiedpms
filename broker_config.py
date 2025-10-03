"""
Broker Configuration Module
Registry for executing broker configurations
"""

BROKER_REGISTRY = {
    'ICICI': {
        'name': 'ICICI Securities Limited',
        'broker_code': 7730,
        'parser_class': 'IciciParser',
        'file_patterns': ['icici', 'ICICI'],
        'description': 'ICICI Securities executing broker'
    },
    'KOTAK': {
        'name': 'Kotak Securities',
        'broker_code': 8081,
        'parser_class': 'KotakParser',
        'file_patterns': ['kotak', 'KOTAK'],
        'description': 'Kotak Securities executing broker'
    },
    'IIFL': {
        'name': 'IIFL Securities',
        'broker_code': 10975,
        'parser_class': 'IIFLParser',
        'file_patterns': ['iifl', 'IIFL'],
        'description': 'IIFL Securities executing broker'
    },
    'AXIS': {
        'name': 'Axis Securities',
        'broker_code': 13872,
        'parser_class': 'AxisParser',
        'file_patterns': ['axis', 'AXIS'],
        'description': 'Axis Securities executing broker'
    },
    'EQUIRUS': {
        'name': 'Equirus Securities',
        'broker_code': 13017,
        'parser_class': 'EquirusParser',
        'file_patterns': ['equirus', 'EQUIRUS'],
        'description': 'Equirus Securities executing broker'
    },
    'EDELWEISS': {
        'name': 'Edelweiss Securities',
        'broker_code': 11933,
        'parser_class': 'EdelweissParser',
        'file_patterns': ['edelweiss', 'EDELWEISS'],
        'description': 'Edelweiss Securities executing broker'
    },
    'NUVAMA': {
        'name': 'Nuvama Securities',
        'broker_code': 11933,  # Same as Edelweiss
        'parser_class': 'EdelweissParser',  # Uses same parser
        'file_patterns': ['nuvama', 'NUVAMA'],
        'description': 'Nuvama Securities executing broker (formerly Edelweiss)'
    },
    'MORGAN': {
        'name': 'Morgan Stanley',
        'broker_code': 10542,
        'parser_class': 'MorganStanleyParser',
        'file_patterns': ['morgan', 'MORGAN', 'MS', 'ms'],
        'description': 'Morgan Stanley executing broker'
    },
    'ANTIQUE': {
        'name': 'Antique Stock Broking',
        'broker_code': 12987,
        'parser_class': 'AntiqueParser',
        'file_patterns': ['antique', 'ANTIQUE'],
        'description': 'Antique Stock Broking executing broker'
    }
}


def get_broker_by_code(broker_code: int):
    """Get broker config by broker code"""
    for broker_id, config in BROKER_REGISTRY.items():
        if config['broker_code'] == broker_code or config['broker_code'] == int(str(broker_code).lstrip('0')):
            return {**config, 'broker_id': broker_id}
    return None


def detect_broker_from_filename(filename: str):
    """Detect broker from filename"""
    filename_lower = filename.lower()
    for broker_id, config in BROKER_REGISTRY.items():
        for pattern in config['file_patterns']:
            if pattern.lower() in filename_lower:
                return {**config, 'broker_id': broker_id}
    return None


def get_all_broker_codes():
    """Get list of all broker codes"""
    return [config['broker_code'] for config in BROKER_REGISTRY.values()]
