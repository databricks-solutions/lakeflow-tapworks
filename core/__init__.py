"""
Core connector architecture for lakehouse-tapworks.

This module provides the foundational base classes for building connectors
to various data sources (databases, SaaS applications, etc.).

Base Classes:
- BaseConnector: Root abstract base class for all connectors
- DatabaseConnector: Base class for database sources with gateway support
- SaaSConnector: Base class for SaaS sources without gateway support

Exceptions:
- LakehouseTapworksError: Base exception for all package errors
- ConfigurationError: Invalid or missing configuration
- ValidationError: Data validation failures
- YAMLGenerationError: YAML file generation failures
"""

from .connectors import BaseConnector, DatabaseConnector, SaaSConnector
from .exceptions import (
    LakehouseTapworksError,
    ConfigurationError,
    ValidationError,
    YAMLGenerationError
)

__all__ = [
    'BaseConnector',
    'DatabaseConnector',
    'SaaSConnector',
    'LakehouseTapworksError',
    'ConfigurationError',
    'ValidationError',
    'YAMLGenerationError'
]
