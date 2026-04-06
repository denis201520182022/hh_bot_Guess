# app/connectors/__init__.py
from .avito.service import avito_connector
from .hh.service import hh_connector
from .console.service import console_connector

CONNECTORS = {
    "avito": avito_connector,
    "hh": hh_connector,
    "console": console_connector,
}

def get_connector(platform: str):
    connector = CONNECTORS.get(platform)
    if not connector:
        raise ValueError(f"Connector for platform '{platform}' not found")
    return connector