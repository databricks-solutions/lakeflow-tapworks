"""
Core connector architecture for lakehouse-tapworks.

This module provides the foundational base classes for building connectors
to various data sources (databases, SaaS applications, etc.).

Base Classes:
- BaseConnector: Root abstract base class for all connectors
- DatabaseConnector: Base class for database sources with gateway support
- SaaSConnector: Base class for SaaS sources without gateway support
"""

from .connectors import BaseConnector, DatabaseConnector, SaaSConnector

__all__ = [
    'BaseConnector',
    'DatabaseConnector',
    'SaaSConnector'
]
