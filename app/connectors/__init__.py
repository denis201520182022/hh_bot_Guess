# app/connectors/__init__.py
from .avito.service import avito_connector
from .hh.service import hh_connector  # Когда создадим его

CONNECTORS = {
    "avito": avito_connector,
    "hh": hh_connector,
}

def get_connector(platform: str):
    connector = CONNECTORS.get(platform)
    if not connector:
        raise ValueError(f"Connector for platform '{platform}' not found")
    return connector