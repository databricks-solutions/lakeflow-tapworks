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

Registry & Runner:
- get_connector: Get a connector instance by name
- list_connectors: List all available connectors
- run_pipeline_generation: Unified pipeline generation function
"""

from .connectors import BaseConnector, DatabaseConnector, SaaSConnector
from .exceptions import (
    LakehouseTapworksError,
    ConfigurationError,
    ValidationError,
    YAMLGenerationError
)
from .registry import (
    get_connector,
    get_connector_class,
    list_connectors,
    get_connector_info,
    resolve_connector_name,
    CONNECTORS,
)
from .runner import run_pipeline_generation
from .utilities import convert_cron_to_quartz

__all__ = [
    # Base classes
    'BaseConnector',
    'DatabaseConnector',
    'SaaSConnector',
    # Exceptions
    'LakehouseTapworksError',
    'ConfigurationError',
    'ValidationError',
    'YAMLGenerationError',
    # Registry
    'get_connector',
    'get_connector_class',
    'list_connectors',
    'get_connector_info',
    'resolve_connector_name',
    'CONNECTORS',
    # Runner
    'run_pipeline_generation',
    # Utilities
    'convert_cron_to_quartz',
]
