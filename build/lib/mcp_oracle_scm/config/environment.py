"""Oracle SCM Configuration Module"""

import os
from typing import Dict, Any

# Oracle Environment Configuration
ORACLE_CONFIGS = {
    'DEV1': {
        'base_url': "https://ehsg-dev1.fa.us6.oraclecloud.com",
        'auth_url': "https://idcs-10a29e74c18944ec81b18f08f9fc1362.identity.oraclecloud.com/oauth2/v1/authorize",
        'token_url': "https://idcs-10a29e74c18944ec81b18f08f9fc1362.identity.oraclecloud.com/oauth2/v1/token",
        'client_id': "522f1ba486ff4d32951f94faba65ec64",
        'scope': "urn:opc:resource:fa:instanceid=589866548urn:opc:resource:consumer::all"
    },
    'TEST': {
        'base_url': "https://ehsg-test.fa.us6.oraclecloud.com",
        'auth_url': "https://idcs-a57149b4f14045039295c03cb5771671.identity.oraclecloud.com/oauth2/v1/authorize",
        'token_url': "https://idcs-a57149b4f14045039295c03cb5771671.identity.oraclecloud.com/oauth2/v1/token",
        'client_id': "b2be270e16a64940ad414af496795d93",
        'scope': "urn:opc:resource:fa:instanceid=589866549urn:opc:resource:consumer::all"
    },
    'PROD': {
        'base_url': "https://ehsg.fa.us6.oraclecloud.com",
        'auth_url': "https://idcs-24cbca7916e44e3da0d4b5bfda3820a3.identity.oraclecloud.com/oauth2/v1/authorize",
        'token_url': "https://idcs-24cbca7916e44e3da0d4b5bfda3820a3.identity.oraclecloud.com/oauth2/v1/token",
        'client_id': "d93df868766f45d9b6ac4fb9c6d8bcad",
        'scope': "urn:opc:resource:fa:instanceid=1716952urn:opc:resource:consumer::all"
    }
}

# API Configuration
API_CONFIG = {
    'timeout': {
        'default': 180,  # seconds
        'cancel': 300,   # 5 minutes for cancellation requests
        'connect': 60.0,
        'sock_connect': 30.0
    },
    'paths': {
        'base_api': '/fscmRestApi/resources/11.13.18.05',
        'soap_service': '/fscmService'
    }
}

def get_env_config() -> Dict[str, Any]:
    """Get Oracle configuration based on environment"""
    env = os.environ.get("ORACLE_ENV", "PROD").upper()
    
    if env not in ORACLE_CONFIGS:
        raise ValueError(f"Invalid Oracle Environment: {env}. Valid values are: {', '.join(ORACLE_CONFIGS.keys())}")
    
    config = ORACLE_CONFIGS[env].copy()
    config.update({
        'env': env,
        'api': API_CONFIG
    })
    
    return config