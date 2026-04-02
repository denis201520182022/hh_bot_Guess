#app\connectors\avito\__init__.py

from .service import avito_connector
from .client import avito

__all__ = ["avito_connector", "avito"]