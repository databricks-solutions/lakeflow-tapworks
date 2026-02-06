"""
Custom exceptions for lakehouse-tapworks.

This module defines a hierarchy of exceptions for better error handling
and more informative error messages throughout the codebase.
"""


class LakehouseTapworksError(Exception):
    """
    Base exception for all lakehouse-tapworks errors.

    All custom exceptions in this package inherit from this class,
    allowing callers to catch all package-specific errors with a single except clause.
    """
    pass


class ConfigurationError(LakehouseTapworksError):
    """
    Raised when configuration is invalid or missing.

    Examples:
        - Missing required columns in input DataFrame
        - Empty DataFrame
        - Invalid target environment configuration
        - Missing workspace_host in targets
    """
    pass


class ValidationError(LakehouseTapworksError):
    """
    Raised when data validation fails.

    Examples:
        - Invalid cron expression format
        - Invalid resource name (doesn't meet Databricks naming rules)
        - Invalid SCD type
        - Empty or invalid connection name
    """
    pass


class YAMLGenerationError(LakehouseTapworksError):
    """
    Raised when YAML file generation fails.

    Examples:
        - File write permission denied
        - Disk full
        - Invalid path
        - YAML serialization error
    """
    pass
