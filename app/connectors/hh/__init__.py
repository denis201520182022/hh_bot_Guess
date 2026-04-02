# app/connectors/hh/__init__.py
from .service import hh_connector
from .client import hh

__all__ = ["hh_connector", "hh"]
