"""
Base connector classes for Databricks Lakeflow Connect pipeline generation.

This module provides abstract base classes for implementing connectors to various
data sources. Each connector handles CSV input processing, load balancing, and
YAML generation for Databricks Asset Bundle deployment.

Architecture:
    BaseConnector (ABC)
    ├── DatabaseConnector (ABC) - For database sources with gateways
    │   └── SQLServerConnector
    └── SaaSConnector (ABC) - For SaaS sources without gateways
        ├── SalesforceConnector
        └── GoogleAnalyticsConnector

Usage:
    # Instantiate a connector
    connector = SQLServerConnector()

    # Run complete pipeline generation
    result_df = connector.run_complete_pipeline_generation(
        df=input_df,
        output_dir='output',
        targets={'dev': {'workspace_host': '...', 'root_path': '...'}},
        default_values={'project_name': 'my_project'}
    )
"""

import logging
import json
import re
import time
from abc import ABC, abstractmethod
import pandas as pd
import yaml
from typing import Dict, Optional
from pathlib import Path
import sys

# Import shared utilities
from .utilities import load_input_csv, convert_cron_to_quartz

# Import custom exceptions
from .exceptions import ConfigurationError, ValidationError, YAMLGenerationError

# Configure module logger
logger = logging.getLogger(__name__)


def _normalize_to_grouped_config(config: Optional[Dict]) -> Dict:
    """
    Normalize config to grouped format.

    Converts flat config dicts to grouped format with '*' as the global key.
    If already in grouped format (values are dicts), returns as-is.

    Args:
        config: Configuration dict, either flat or grouped format

    Returns:
        Grouped config dict with '*' as global key

    Examples:
        >>> _normalize_to_grouped_config({'schedule': '*/15 * * * *'})
        {'*': {'schedule': '*/15 * * * *'}}

        >>> _normalize_to_grouped_config({'*': {'schedule': '*/15 * * * *'}, 'sales': {'schedule': '*/30 * * * *'}})
        {'*': {'schedule': '*/15 * * * *'}, 'sales': {'schedule': '*/30 * * * *'}}
    """
    if not config:
        return {}

    # Check if already in grouped format (first value is a dict)
    first_value = next(iter(config.values()))
    if isinstance(first_value, dict):
        return config

    # Flat format - wrap in '*' key
    return {'*': config}


def _get_match_key(row: pd.Series, df: pd.DataFrame) -> str:
    """
    Get the match key for a row to use for group-based config lookup.

    Priority:
    1. 'prefix' column if it exists and has a value
    2. 'project_name' column as fallback

    Args:
        row: DataFrame row
        df: Full DataFrame (to check if prefix column exists)

    Returns:
        Match key string for config lookup
    """
    if 'prefix' in df.columns and pd.notna(row.get('prefix')) and str(row.get('prefix')).strip():
        return str(row['prefix']).strip()
    if 'project_name' in df.columns and pd.notna(row.get('project_name')):
        return str(row['project_name']).strip()
    return ''


class BaseConnector(ABC):
    """
    Abstract base class for all connectors.

    Defines the common interface and workflow that all connectors must implement.
    Each connector provides connector-specific configuration (required columns,
    defaults, etc.) and inherits shared logic for CSV processing and pipeline generation.
    """

    # Default configuration constants
    DEFAULT_MAX_TABLES_PER_PIPELINE = 250
    DEFAULT_MAX_TABLES_PER_GATEWAY = 250

    # Validation configuration - fields that must be consistent for jobs
    JOB_CONSISTENCY_FIELDS = ['schedule', 'pause_status', 'tags']

    def __init__(self):
        """Initialize the connector with its configuration."""
        self._validate_configuration()

    def _validate_group_consistency(
        self,
        group_df: pd.DataFrame,
        group_name: str,
        fields_to_validate: list,
        context: str = "pipeline group"
    ):
        """
        Validate that specified fields have consistent values within a group.

        This generic validation method ensures that critical configuration fields
        have the same value for all tables in a group (pipeline_group, gateway, etc.).

        Args:
            group_df: DataFrame for a single group (pipeline_group, gateway, etc.)
            group_name: Name of the group for error messages (e.g., 'sales_01_g01')
            fields_to_validate: List of field names to check for consistency
            context: Context for error message (e.g., 'pipeline group', 'gateway')

        Raises:
            ValidationError: If any field has conflicting values within the group

        Example:
            self._validate_group_consistency(
                group_df=group_df,
                group_name=pipeline_group,
                fields_to_validate=['schedule', 'pause_status'],
                context='pipeline group'
            )
        """
        for field in fields_to_validate:
            if field not in group_df.columns:
                continue

            # Get non-empty values
            values = group_df[field].dropna()
            values = values[values.astype(str).str.strip() != '']
            unique_values = values.unique()

            if len(unique_values) > 1:
                raise ValidationError(
                    f"{context.capitalize()} '{group_name}' has conflicting {field} values: {list(unique_values)}. "
                    f"All tables in the same {context} must have the same {field}. "
                    f"Solutions: (1) Use the same {field} value for all tables, or "
                    f"(2) Use different 'subgroup' values to separate tables with different {field} values."
                )

    @property
    @abstractmethod
    def connector_type(self) -> str:
        """
        Return the connector type identifier (e.g., 'sql_server', 'salesforce', 'google_analytics').

        Used for:
        - Naming conventions in generated resources
        - Default project naming
        - Logging and error messages
        """
        pass

    @property
    @abstractmethod
    def required_columns(self) -> list:
        """
        Return list of required columns for this connector's input CSV.

        These columns must be present and non-empty in the input data.
        The base class will validate their presence before processing.

        Example:
            ['source_database', 'source_schema', 'source_table_name',
             'target_catalog', 'target_schema', 'target_table_name',
             'connection_name']
        """
        pass

    @property
    @abstractmethod
    def default_values(self) -> Dict[str, any]:
        """
        Return default values for optional columns.

        These values if specified are used when columns are missing or contain empty/NaN values.

        Example:
            {
                'schedule': '*/15 * * * *',
                'gateway_driver_type': 'node type'
            }
        """
        pass

    @property
    def default_project_name(self) -> str:
        """
        Return default project name if not provided.

        Uses connector_type as the default project name suffix.
        Override in subclass if different default is needed.
        """
        return f"{self.connector_type}_ingestion"

    @property
    def supported_scd_types(self) -> list:
        """
        Return list of SCD types supported by this connector.

        Override in subclass to specify supported SCD types.
        Empty list means SCD type configuration is not supported.

        Example: ['SCD_TYPE_1', 'SCD_TYPE_2'] or ['SCD_TYPE_1']
        """
        return []

    def _is_value_set(self, value) -> bool:
        """
        Check if a value is meaningfully set (not None, NaN, or empty string).

        Use this method for consistent null-checking across the codebase.

        Args:
            value: Any value to check

        Returns:
            True if value is set and non-empty, False otherwise
        """
        if value is None:
            return False
        if pd.isna(value):
            return False
        if isinstance(value, str) and not value.strip():
            return False
        return True

    def _parse_tags(self, tags_value) -> Dict[str, str]:
        """
        Parse a tags value into a Databricks-compatible tags dictionary.

        Supported formats:
        - dict-like: {"team": "field-eng", "demo": "true"}
        - JSON string: '{"team":"field-eng","demo":"true"}'
        - key/value string: 'team=field-eng,demo=true' (commas or semicolons)
        """
        if not self._is_value_set(tags_value):
            return {}

        if isinstance(tags_value, dict):
            return {str(k): str(v) for k, v in tags_value.items()}

        s = str(tags_value).strip()
        if not s:
            return {}

        # JSON dict string
        if s.startswith("{") and s.endswith("}"):
            try:
                obj = json.loads(s)
            except Exception:
                return {}
            if isinstance(obj, dict):
                return {str(k): str(v) for k, v in obj.items()}
            return {}

        # key=value pairs
        tags: Dict[str, str] = {}
        parts = [p.strip() for p in s.replace(";", ",").split(",") if p.strip()]
        for part in parts:
            if "=" not in part:
                continue
            k, v = part.split("=", 1)
            k = k.strip()
            v = v.strip()
            if k:
                tags[str(k)] = str(v)
        return tags

    def _validate_cron_expression(self, cron: str, context: str = "schedule") -> str:
        """
        Validate cron expression format.

        Accepts both standard 5-field cron and 6-field Quartz format.

        Args:
            cron: Cron expression string to validate
            context: Context for error message (e.g., "schedule", "job")

        Returns:
            The validated cron expression (stripped)

        Raises:
            ValidationError: If cron expression is invalid
        """
        if not self._is_value_set(cron):
            raise ValidationError(f"Empty {context} cron expression")

        cron = str(cron).strip()
        parts = cron.split()

        # Standard cron: 5 fields (minute hour day month weekday)
        # Quartz cron: 6 fields (second minute hour day month weekday) or 7 fields (+ year)
        if len(parts) not in [5, 6, 7]:
            raise ValidationError(
                f"Invalid {context} cron expression '{cron}': "
                f"expected 5-7 fields, got {len(parts)}"
            )

        return cron

    def _validate_resource_name(self, name: str, resource_type: str = "resource") -> str:
        """
        Validate Databricks resource naming rules.

        Databricks resource names must:
        - Start with a letter
        - Contain only letters, numbers, underscores, and hyphens
        - Be no longer than 128 characters

        Args:
            name: Resource name to validate
            resource_type: Type of resource for error message (e.g., "pipeline", "job")

        Returns:
            The validated resource name (stripped)

        Raises:
            ValidationError: If resource name is invalid
        """
        if not self._is_value_set(name):
            raise ValidationError(f"Empty {resource_type} name")

        name = str(name).strip()

        if not name[0].isalpha():
            raise ValidationError(
                f"Invalid {resource_type} name '{name}': must start with a letter"
            )

        if not re.match(r'^[a-zA-Z][a-zA-Z0-9_-]*$', name):
            raise ValidationError(
                f"Invalid {resource_type} name '{name}': "
                f"can only contain letters, numbers, underscores, and hyphens"
            )

        if len(name) > 128:
            raise ValidationError(
                f"Invalid {resource_type} name '{name}': "
                f"exceeds maximum length of 128 characters ({len(name)} chars)"
            )

        return name

    def _write_yaml_file(
        self,
        path: Path,
        content: dict,
        retries: int = 3,
        retry_delay: float = 0.5
    ) -> None:
        """
        Write YAML file with retry logic and error handling.

        Args:
            path: Path to write the YAML file
            content: Dictionary content to serialize as YAML
            retries: Number of retry attempts (default: 3)
            retry_delay: Delay between retries in seconds (default: 0.5)

        Raises:
            YAMLGenerationError: If file write fails after all retries
        """
        last_error = None

        for attempt in range(retries):
            try:
                with open(path, 'w') as f:
                    yaml.dump(content, f, sort_keys=False, default_flow_style=False, indent=2)
                logger.debug(f"Written: {path}")
                return
            except (IOError, OSError, yaml.YAMLError) as e:
                last_error = e
                if attempt < retries - 1:
                    logger.warning(
                        f"Retry {attempt + 1}/{retries} writing {path}: {e}"
                    )
                    time.sleep(retry_delay)

        raise YAMLGenerationError(
            f"Failed to write {path} after {retries} attempts: {last_error}"
        )

    def _validate_scd_type(self, scd_type: str, item_name: str) -> str:
        """
        Validate and return SCD type if valid, None otherwise.

        Args:
            scd_type: The SCD type value from config (may be None or empty)
            item_name: Name of the item (table/report) for logging

        Returns:
            Validated SCD type string or None if not specified/invalid
        """
        if not scd_type or (isinstance(scd_type, str) and not scd_type.strip()):
            return None

        import pandas as pd
        if pd.isna(scd_type):
            return None

        scd_type = str(scd_type).strip().upper()

        if not self.supported_scd_types:
            logger.warning(f"SCD type '{scd_type}' specified for {item_name} but connector doesn't support SCD types, ignoring")
            return None

        if scd_type not in self.supported_scd_types:
            logger.warning(f"Invalid scd_type '{scd_type}' for {item_name}, supported: {self.supported_scd_types}, ignoring")
            return None

        return scd_type

    def _validate_configuration(self):
        """
        Validate that the connector is properly configured.

        Ensures that all abstract properties have been implemented in the subclass.
        Raises TypeError if connector is not properly configured.
        """
        # This will fail if abstract properties aren't implemented
        try:
            _ = self.connector_type
            _ = self.required_columns
            _ = self.default_values
        except NotImplementedError:
            raise TypeError(
                f"{self.__class__.__name__} must implement all abstract properties"
            )

    def _process_input_config(
        self,
        df: pd.DataFrame,
        required_columns: list,
        default_values: Dict = None,
        override_input_config: Dict = None
    ) -> pd.DataFrame:
        """
        Validate and normalize input configuration DataFrame.

        This method ensures all required columns are present, adds optional columns
        with defaults if missing, and fills empty/NaN values appropriately.

        Supports group-based configuration where defaults/overrides can be specified
        per group (matching on 'prefix' or 'project_name'):

            default_values = {
                '*': {'schedule': '*/15 * * * *'},      # Global default
                'sales': {'schedule': '*/30 * * * *'},  # Group-specific
            }

        Args:
            df: Input DataFrame from any source (CSV, Delta, code)
            required_columns: List of required column names that must be present
            default_values: Dictionary of defaults (flat or grouped format)
            override_input_config: Dictionary of overrides (flat or grouped format)

        Returns:
            Normalized DataFrame with all required and optional columns

        Raises:
            ConfigurationError: If required columns are missing or DataFrame is empty
        """
        # Make a copy to avoid modifying the original dataframe
        df = df.copy()

        # Check if dataframe is empty
        if df.empty:
            raise ConfigurationError("Input DataFrame is empty")

        # Validate required columns exist
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ConfigurationError(
                f"Missing required columns: {missing_columns}\n"
                f"Required columns: {', '.join(required_columns)}"
            )

        # Normalize configs to grouped format
        grouped_defaults = _normalize_to_grouped_config(default_values)
        grouped_overrides = _normalize_to_grouped_config(override_input_config)

        # Track which values were originally empty (before any defaults applied)
        # These are the only values that defaults should fill
        empty_mask = {}
        for col in set(col for d in grouped_defaults.values() for col in d.keys()):
            if col in df.columns:
                empty_mask[col] = df[col].isna() | (df[col].astype(str).str.strip() == '')
            else:
                empty_mask[col] = pd.Series([True] * len(df), index=df.index)

        # Step 1: Apply global defaults first (ensures project_name exists for matching)
        global_defaults = grouped_defaults.get('*', {})
        df = self._apply_defaults_to_df(df, global_defaults, empty_mask=empty_mask)

        # Step 2: Apply group-specific defaults (these override global defaults but not CSV values)
        # Sort keys by specificity: less specific first (e.g., 'sales'), more specific last (e.g., 'sales_02')
        # This ensures more specific matches override less specific ones
        sorted_default_keys = self._sort_keys_by_specificity(
            [k for k in grouped_defaults.keys() if k != '*']
        )
        for group_key in sorted_default_keys:
            group_defaults = grouped_defaults[group_key]
            mask = self._get_group_mask(df, group_key)
            if mask.any():
                df = self._apply_defaults_to_df(df, group_defaults, row_mask=mask, empty_mask=empty_mask)

        # Step 3: Apply global overrides
        global_overrides = grouped_overrides.get('*', {})
        df = self._apply_overrides_to_df(df, global_overrides)

        # Step 4: Apply group-specific overrides (sorted by specificity)
        sorted_override_keys = self._sort_keys_by_specificity(
            [k for k in grouped_overrides.keys() if k != '*']
        )
        for group_key in sorted_override_keys:
            group_overrides = grouped_overrides[group_key]
            mask = self._get_group_mask(df, group_key)
            if mask.any():
                df = self._apply_overrides_to_df(df, group_overrides, mask)

        logger.info(f"Configuration validated: {len(df)} rows")

        return df

    def _sort_keys_by_specificity(self, keys: list) -> list:
        """
        Sort config keys by specificity (less specific first, more specific last).

        Keys containing '_' are considered more specific (pipeline_group format)
        and should be processed last so they can override less specific matches.

        Args:
            keys: List of config keys to sort

        Returns:
            Sorted list with less specific keys first
        """
        # Keys with '_' are more specific (pipeline_group like 'sales_02')
        # Keys without '_' are less specific (prefix like 'sales')
        return sorted(keys, key=lambda k: ('_' in k, k))

    def _get_group_mask(self, df: pd.DataFrame, group_key: str) -> pd.Series:
        """
        Get a boolean mask for rows matching a group key.

        Matching precedence:
        1. pipeline_group (prefix_subgroup) - exact match
        2. prefix - exact match
        3. project_name - exact match

        Args:
            df: DataFrame to match against
            group_key: Group key to match

        Returns:
            Boolean Series mask for matching rows
        """
        # Try pipeline_group match (prefix_subgroup)
        if 'prefix' in df.columns and 'subgroup' in df.columns:
            pipeline_group = (
                df['prefix'].astype(str).str.strip() + '_' +
                df['subgroup'].astype(str).str.strip()
            )
            pg_match = pipeline_group == group_key
            if pg_match.any():
                return pg_match

        # Try prefix match
        if 'prefix' in df.columns:
            prefix_match = df['prefix'].astype(str).str.strip() == group_key
            if prefix_match.any():
                return prefix_match

        # Try project_name match
        if 'project_name' in df.columns:
            return df['project_name'].astype(str).str.strip() == group_key

        return pd.Series([False] * len(df), index=df.index)

    def _apply_defaults_to_df(
        self,
        df: pd.DataFrame,
        defaults: Dict,
        row_mask: pd.Series = None,
        empty_mask: Dict[str, pd.Series] = None
    ) -> pd.DataFrame:
        """
        Apply default values to DataFrame (only fills originally empty values).

        Args:
            df: DataFrame to apply defaults to
            defaults: Dictionary of column defaults
            row_mask: Optional boolean mask to apply defaults only to matching rows
            empty_mask: Dict mapping column names to boolean masks indicating
                       which rows were originally empty (before any defaults)

        Returns:
            DataFrame with defaults applied
        """
        if not defaults:
            return df

        for col_name, default_value in defaults.items():
            if col_name not in df.columns:
                # Add new column
                if row_mask is None:
                    df[col_name] = default_value
                else:
                    df[col_name] = None
                    df.loc[row_mask, col_name] = default_value
                logger.debug(f"Column '{col_name}' not found, adding with default: {default_value}")
            else:
                # Fill originally empty values only
                if default_value is not None:
                    # Get the original empty mask for this column
                    col_empty_mask = empty_mask.get(col_name) if empty_mask else None

                    if col_empty_mask is None:
                        # Fallback: check current empty state
                        col_empty_mask = df[col_name].isna() | (df[col_name].astype(str).str.strip() == '')

                    if row_mask is None:
                        # Global: fill all originally empty values
                        df.loc[col_empty_mask, col_name] = default_value
                    else:
                        # Group-specific: fill originally empty values for matching rows
                        combined_mask = row_mask & col_empty_mask
                        df.loc[combined_mask, col_name] = default_value

        return df

    def _apply_overrides_to_df(
        self,
        df: pd.DataFrame,
        overrides: Dict,
        mask: pd.Series = None
    ) -> pd.DataFrame:
        """
        Apply override values to DataFrame (overwrites all values).

        Args:
            df: DataFrame to apply overrides to
            overrides: Dictionary of column overrides
            mask: Optional boolean mask to apply overrides only to matching rows

        Returns:
            DataFrame with overrides applied
        """
        if not overrides:
            return df

        for col_name, override_value in overrides.items():
            if mask is None:
                df[col_name] = override_value
            else:
                if col_name not in df.columns:
                    df[col_name] = None
                df.loc[mask, col_name] = override_value
            logger.debug(f"Overriding '{col_name}' with value: {override_value}")

        return df

    def load_and_normalize_input(
        self,
        df: pd.DataFrame,
        default_values: Optional[Dict] = None,
        override_input_config: Optional[Dict] = None
    ) -> pd.DataFrame:
        """
        Load and normalize input CSV data.

        Applies the configuration hierarchy:
        1. User config values (base)
        2. Connector default_values (for missing/empty)
        3. User-provided default_values parameter (overrides connector defaults)
        4. override_input_config (overrides everything)

        Args:
            df: Input DataFrame to normalize
            default_values: User-provided defaults (overrides connector defaults)
            override_input_config: Values to force for all rows

        Returns:
            Normalized DataFrame with all required columns and defaults applied
        """
        # Build connector defaults (flat format)
        # Note: project_name is required and has no default
        connector_defaults = dict(self.default_values)

        # Merge connector defaults with user-provided defaults
        # Handle both flat and grouped user defaults
        if default_values:
            # Check if user defaults are grouped (first value is a dict)
            first_value = next(iter(default_values.values())) if default_values else None
            if isinstance(first_value, dict):
                # User provided grouped defaults - merge connector defaults into global '*'
                final_defaults = dict(default_values)  # Copy user defaults
                user_global = final_defaults.get('*', {})
                # Connector defaults go in '*', user global overrides them
                final_defaults['*'] = {**connector_defaults, **user_global}
            else:
                # User provided flat defaults - simple merge
                final_defaults = {**connector_defaults, **default_values}
        else:
            final_defaults = connector_defaults

        # Process input configuration
        normalized_df = self._process_input_config(
            df=df,
            required_columns=self.required_columns,
            default_values=final_defaults,
            override_input_config=override_input_config
        )

        # Apply connector-specific normalization
        normalized_df = self._apply_connector_specific_normalization(normalized_df)

        return normalized_df

    def _apply_connector_specific_normalization(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply connector-specific normalization logic.

        Override in subclass to add custom normalization steps.
        Called after standard normalization is complete.

        Args:
            df: Normalized DataFrame

        Returns:
            DataFrame with connector-specific normalization applied
        """
        # Validate project_name is provided (required field)
        if 'project_name' not in df.columns:
            raise ConfigurationError(
                "Missing required field: project_name\n"
                "Provide via: input config column, default_values, or override_config"
            )
        missing_project = df['project_name'].isna() | (df['project_name'].astype(str).str.strip() == '')
        if missing_project.any():
            raise ConfigurationError(
                "Missing required field: project_name (some rows have empty values)\n"
                "Provide via: input config column, default_values, or override_config"
            )

        # Handle prefix defaults (common to all connectors)
        if 'prefix' not in df.columns:
            df['prefix'] = df['project_name']
        else:
            mask = df['prefix'].isna() | (df['prefix'].astype(str).str.strip() == '')
            df.loc[mask, 'prefix'] = df.loc[mask, 'project_name']

        # Handle subgroup - validate no mixed usage within a prefix
        if 'subgroup' not in df.columns:
            df['subgroup'] = '01'
        else:
            df['_subgroup_empty'] = df['subgroup'].isna() | (df['subgroup'].astype(str).str.strip() == '')

            # Check for mixed subgroup usage within each prefix
            for prefix in df['prefix'].unique():
                prefix_mask = df['prefix'] == prefix
                prefix_df = df[prefix_mask]

                has_empty = prefix_df['_subgroup_empty'].any()
                has_defined = (~prefix_df['_subgroup_empty']).any()

                if has_empty and has_defined:
                    defined_subgroups = prefix_df.loc[~prefix_df['_subgroup_empty'], 'subgroup'].unique()[:3]
                    empty_count = prefix_df['_subgroup_empty'].sum()
                    raise ValidationError(
                        f"Mixed subgroup usage in prefix '{prefix}': {empty_count} table(s) have empty "
                        f"subgroups while others use {list(defined_subgroups)}. "
                        f"When using subgroups, all tables in a prefix must have explicit subgroups."
                    )

            # All subgroups empty for this prefix - default to '01'
            df.loc[df['_subgroup_empty'], 'subgroup'] = '01'
            df.drop(columns=['_subgroup_empty'], inplace=True)

        return df

    def _generate_resource_names(self, pipeline_group: str) -> Dict[str, str]:
        """
        Generate consistent resource names for pipelines and jobs.

        Args:
            pipeline_group: Pipeline group identifier (e.g., 'sales_01')

        Returns:
            Dictionary containing all resource names:
                - pipeline_name: Display name for pipeline
                - pipeline_resource_name: Resource identifier for pipeline
                - job_name: Resource identifier for job
                - job_display_name: Display name for job
                - task_key: Task key for pipeline task

        Raises:
            ValidationError: If generated resource names are invalid
        """
        pipeline_resource_name = f"pipeline_{pipeline_group}"
        job_name = f"job_{pipeline_group}"

        # Validate generated resource names
        self._validate_resource_name(pipeline_resource_name, "pipeline")
        self._validate_resource_name(job_name, "job")

        return {
            'pipeline_name': pipeline_group,
            'pipeline_resource_name': pipeline_resource_name,
            'job_name': job_name,
            'job_display_name': pipeline_group,
            'task_key': "run_pipeline"
        }

    def _split_groups_by_size(
        self,
        df: pd.DataFrame,
        group_column: str,
        max_size: int,
        output_column: str,
        suffix: str
    ) -> pd.DataFrame:
        """
        Split groups that exceed max_size into smaller chunks with sequential suffixes.

        This method is used for load balancing - splitting large groups of tables
        into smaller chunks that fit within pipeline or gateway capacity limits.

        Args:
            df: Input DataFrame with groups to split
            group_column: Column containing group identifiers to split
            max_size: Maximum rows per group
            output_column: Column name for output group names
            suffix: Suffix pattern for split groups ('p' for pipelines, 'g' for gateways)

        Returns:
            DataFrame with output_column populated with split group names
        """
        df = df.copy()
        df[output_column] = ''

        for group_name in df[group_column].unique():
            group_df = df[df[group_column] == group_name]

            if len(group_df) > max_size:
                # Split into chunks
                num_chunks = (len(group_df) - 1) // max_size + 1

                for i in range(num_chunks):
                    start_idx = i * max_size
                    end_idx = min((i + 1) * max_size, len(group_df))
                    chunk_indices = group_df.iloc[start_idx:end_idx].index
                    chunk_name = f"{group_name}_{suffix}{i+1:02d}"
                    df.loc[chunk_indices, output_column] = chunk_name
            else:
                # No split needed - still add suffix for stable naming
                df.loc[group_df.index, output_column] = f"{group_name}_{suffix}01"

        return df

    def _create_jobs(self, df: pd.DataFrame, project_name: str) -> Dict:
        """
        Create job YAML configuration from dataframe.

        Creates a scheduled job for each pipeline that triggers the pipeline on a cron schedule.

        Args:
            df: DataFrame containing pipeline_group and schedule columns
            project_name: Project name prefix for all resources

        Returns:
            Dictionary with jobs YAML structure

        Raises:
            ValidationError: If tables in the same pipeline group have conflicting schedules
        """
        jobs = {}

        for pipeline_group, group_df in df.groupby('pipeline_group'):
            # Validate all job-level fields at once
            self._validate_group_consistency(
                group_df=group_df,
                group_name=pipeline_group,
                fields_to_validate=self.JOB_CONSISTENCY_FIELDS,
                context='pipeline group'
            )

            schedule = group_df.iloc[0]['schedule']

            # Only create job if schedule is defined
            if self._is_value_set(schedule):
                # Validate cron expression format
                try:
                    self._validate_cron_expression(schedule, f"schedule for {pipeline_group}")
                except ValidationError as e:
                    logger.warning(f"Skipping job for {pipeline_group}: {e}")
                    continue

                names = self._generate_resource_names(pipeline_group)

                # Convert standard cron to Quartz format
                quartz_schedule = convert_cron_to_quartz(schedule)

                job_config = {
                    'name': names['job_display_name'],
                    'schedule': {
                        'quartz_cron_expression': quartz_schedule,
                        'timezone_id': 'UTC'
                    },
                    'tasks': [{
                        'task_key': names['task_key'],
                        'pipeline_task': {
                            'pipeline_id': f"${{resources.pipelines.{names['pipeline_resource_name']}.id}}"
                        }
                    }]
                }

                # Add pause_status if specified
                if 'pause_status' in group_df.columns:
                    pause_status = group_df.iloc[0]['pause_status']
                    if pd.notna(pause_status) and pause_status and str(pause_status).strip():
                        job_config['pause_status'] = str(pause_status).upper()

                # Optional: tags
                if 'tags' in group_df.columns:
                    tags = self._parse_tags(group_df.iloc[0]['tags'])
                    if tags:
                        job_config['tags'] = tags

                jobs[names['job_name']] = job_config

        return {'resources': {'jobs': jobs}}

    def _create_databricks_yml(
        self,
        project_name: str,
        targets: Dict[str, Dict],
        default_target: str = 'dev'
    ) -> Dict:
        """
        Create the main databricks.yml file with flexible target environments.

        Args:
            project_name: Project name for the bundle
            targets: Dictionary of target configurations
                Format: {'env_name': {'workspace_host': '...', 'root_path': '...', 'mode': '...'}}
            default_target: Which target should be the default

        Returns:
            Dictionary with databricks.yml structure

        Raises:
            ConfigurationError: If targets configuration is invalid
        """
        if not targets:
            raise ConfigurationError("At least one target must be provided")

        if default_target not in targets:
            raise ConfigurationError(f"default_target '{default_target}' must be one of: {list(targets.keys())}")

        config = {
            'bundle': {'name': project_name},
            'include': ['resources/*.yml'],
            'targets': {}
        }

        for target_name, target_config in targets.items():
            if 'workspace_host' not in target_config:
                raise ConfigurationError(f"Target '{target_name}' must have 'workspace_host'")

            # Auto-determine mode if not explicitly provided
            mode = target_config.get('mode')
            if not mode:
                mode = 'production' if target_name == 'prod' else 'development'

            # Build target configuration
            target_def = {
                'mode': mode,
                'workspace': {
                    'host': target_config['workspace_host']
                }
            }

            # Add root_path if provided
            if 'root_path' in target_config and target_config['root_path']:
                target_def['workspace']['root_path'] = target_config['root_path']

            # Mark as default if this is the default target
            if target_name == default_target:
                target_def['default'] = True

            config['targets'][target_name] = target_def

        return config

    @abstractmethod
    def generate_pipeline_config(
        self,
        df: pd.DataFrame,
        **kwargs
    ) -> pd.DataFrame:
        """
        Generate pipeline configuration with load balancing.

        Implements connector-specific load balancing logic.
        Must add 'pipeline_group' column to the DataFrame.
        Database connectors must also add 'gateway' column.

        Args:
            df: Normalized input DataFrame
            **kwargs: Connector-specific parameters (e.g., max_tables_per_pipeline)

        Returns:
            DataFrame with pipeline_group (and gateway for database connectors)
        """
        pass

    @abstractmethod
    def generate_yaml_files(
        self,
        df: pd.DataFrame,
        output_dir: str,
        targets: Dict[str, Dict]
    ):
        """
        Generate Databricks Asset Bundle YAML files.

        Creates the DAB project structure with all necessary YAML files.
        Implementation depends on whether connector is database (with gateways)
        or SaaS (without gateways).

        Args:
            df: DataFrame with pipeline configuration
            output_dir: Output directory for DAB files
            targets: Dictionary of target environments
                Format: {'env_name': {'workspace_host': '...', 'root_path': '...'}}
        """
        pass

    def run_complete_pipeline_generation(
        self,
        df: pd.DataFrame,
        output_dir: str,
        targets: Dict[str, Dict],
        output_config: Optional[str] = None,
        default_values: Optional[Dict] = None,
        override_input_config: Optional[Dict] = None,
        **kwargs
    ) -> pd.DataFrame:
        """
        Run the complete pipeline generation workflow.

        This is the main entry point for connector usage. Executes the full
        workflow from input CSV to generated DAB files:
        1. Normalize input data
        2. Generate pipeline configuration (load balancing)
        3. Generate YAML files

        Args:
            df: Input DataFrame with source table list
            output_dir: Output directory for DAB project(s)
            targets: Target environments configuration
                Format: {'env_name': {'workspace_host': '...', 'root_path': '...'}}
            output_config: Optional path to save intermediate CSV
            default_values: Default values for optional columns
            override_input_config: Values to force for all rows
            **kwargs: Connector-specific parameters passed to generate_pipeline_config

        Returns:
            DataFrame with complete pipeline configuration
        """
        logger.info(f"Starting {self.connector_type} pipeline generation")
        logger.debug(f"Input rows: {len(df)}")

        # Step 1: Normalize and validate configuration
        normalized_df = self.load_and_normalize_input(
            df=df,
            default_values=default_values,
            override_input_config=override_input_config
        )

        # Step 2: Generate pipeline configuration
        pipeline_config_df = self.generate_pipeline_config(
            df=normalized_df,
            **kwargs
        )

        logger.info(f"Created {pipeline_config_df['pipeline_group'].nunique()} pipelines with {len(pipeline_config_df)} items")

        # Save intermediate configuration if requested
        if output_config:
            pipeline_config_df.to_csv(output_config, index=False)
            logger.debug(f"Saved configuration to: {output_config}")

        # Step 3: Generate YAML files
        logger.debug(f"Output directory: {output_dir}, targets: {list(targets.keys())}")

        self.generate_yaml_files(
            df=pipeline_config_df,
            output_dir=output_dir,
            targets=targets
        )

        logger.info(f"Pipeline generation complete for {self.connector_type}")

        return pipeline_config_df


class DatabaseConnector(BaseConnector):
    """
    Abstract base class for database connectors with gateway support.

    Database connectors use two-level load balancing:
    1. Split into gateways (max_tables_per_gateway)
    2. Split each gateway into pipelines (max_tables_per_pipeline)

    Examples: SQL Server, MySQL, PostgreSQL, Oracle
    """

    # Validation configuration - fields that must be consistent within groups
    GATEWAY_CONSISTENCY_FIELDS = ['gateway_catalog', 'gateway_schema', 'connection_name', 'tags']
    PIPELINE_CONSISTENCY_FIELDS = ['tags']

    def _apply_connector_specific_normalization(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply database connector normalization including gateway defaults.

        Extends base normalization to handle gateway-specific columns.
        """
        # Call parent normalization first
        df = super()._apply_connector_specific_normalization(df)

        # Handle gateway_catalog and gateway_schema defaults
        # Use target values if gateway values are not provided
        if 'gateway_catalog' in df.columns:
            df['gateway_catalog'] = df['gateway_catalog'].astype(object)
            mask = df['gateway_catalog'].isna()
            df.loc[mask, 'gateway_catalog'] = df.loc[mask, 'target_catalog']

        if 'gateway_schema' in df.columns:
            df['gateway_schema'] = df['gateway_schema'].astype(object)
            mask = df['gateway_schema'].isna()
            df.loc[mask, 'gateway_schema'] = df.loc[mask, 'target_schema']

        return df

    def generate_pipeline_config(
        self,
        df: pd.DataFrame,
        max_tables_per_gateway: int = None,
        max_tables_per_pipeline: int = None
    ) -> pd.DataFrame:
        """
        Generate database pipeline configuration with two-level load balancing.

        Uses prefix + subgroup grouping with two-level splitting:
        1. Gateway level: Split into gateways if exceeds max_tables_per_gateway
        2. Pipeline level: Split gateways into pipelines if exceeds max_tables_per_pipeline

        Args:
            df: Normalized input DataFrame
            max_tables_per_gateway: Maximum tables per gateway (default: DEFAULT_MAX_TABLES_PER_GATEWAY)
            max_tables_per_pipeline: Maximum tables per pipeline (default: DEFAULT_MAX_TABLES_PER_PIPELINE)

        Returns:
            DataFrame with 'gateway' and 'pipeline_group' columns added
        """
        # Apply default constants if not specified
        if max_tables_per_gateway is None:
            max_tables_per_gateway = self.DEFAULT_MAX_TABLES_PER_GATEWAY
        if max_tables_per_pipeline is None:
            max_tables_per_pipeline = self.DEFAULT_MAX_TABLES_PER_PIPELINE

        df = df.copy()

        # Ensure consistent string formatting
        df['prefix'] = df['prefix'].astype(str)
        df['subgroup'] = df['subgroup'].astype(str).str.zfill(2)

        # Generate base group from prefix + subgroup
        df['base_group'] = df['prefix'] + '_' + df['subgroup']

        # Step 1: Split by gateway capacity
        df = self._split_groups_by_size(
            df=df,
            group_column='base_group',
            max_size=max_tables_per_gateway,
            output_column='gateway',
            suffix='g'
        )

        # Step 2: Split each gateway by pipeline capacity
        df = self._split_groups_by_size(
            df=df,
            group_column='gateway',
            max_size=max_tables_per_pipeline,
            output_column='pipeline_group',
            suffix='p'
        )

        # Drop temporary base_group column
        df = df.drop(columns=['base_group'])

        return df

    def _create_gateways(self, df: pd.DataFrame, project_name: str) -> Dict:
        """
        Create gateway YAML configuration from dataframe.

        Args:
            df: DataFrame with gateway configuration
            project_name: Project name for resource naming

        Returns:
            Dictionary with gateway YAML configuration
        """
        gateways = {}

        # Validate gateway consistency before processing
        for gateway_id, gateway_df in df.groupby('gateway'):
            self._validate_group_consistency(
                group_df=gateway_df,
                group_name=gateway_id,
                fields_to_validate=self.GATEWAY_CONSISTENCY_FIELDS,
                context='gateway'
            )

        unique_gateways = df.groupby('gateway').first()

        for gateway_id, row in unique_gateways.iterrows():
            gateway_name = f"{gateway_id}"
            gateway_resource_name = f"gateway_{gateway_id}"

            gateway_catalog = row['gateway_catalog']
            gateway_schema = row['gateway_schema']
            worker_type = row.get('gateway_worker_type')
            driver_type = row.get('gateway_driver_type')

            gateway_config = {
                'name': gateway_name,
                'gateway_definition': {
                    'connection_name': row['connection_name'],
                    'gateway_storage_catalog': gateway_catalog,
                    'gateway_storage_schema': gateway_schema,
                    'gateway_storage_name': gateway_name,
                },
                'schema': gateway_schema,
                'continuous': True,
                'catalog': gateway_catalog
            }

            # Optional: tags (applied to the gateway pipeline)
            tags = self._parse_tags(row.get("tags"))
            if tags:
                gateway_config["tags"] = tags

            # Add cluster configuration if node types are provided
            has_worker_type = self._is_value_set(worker_type)
            has_driver_type = self._is_value_set(driver_type)

            if has_worker_type or has_driver_type:
                cluster_config = {'num_workers': 1}
                if has_worker_type:
                    cluster_config['node_type_id'] = worker_type
                if has_driver_type:
                    cluster_config['driver_node_type_id'] = driver_type
                gateway_config['clusters'] = [cluster_config]

            gateways[gateway_resource_name] = gateway_config

        return {'resources': {'pipelines': gateways}}

    def _create_pipelines(self, df: pd.DataFrame, project_name: str) -> Dict:
        """
        Create pipeline YAML configuration from dataframe.

        Args:
            df: DataFrame with pipeline configuration
            project_name: Project name for resource naming

        Returns:
            Dictionary with pipeline YAML configuration
        """
        pipelines = {}

        for pipeline_group, group_df in df.groupby('pipeline_group'):
            # Validate pipeline-level fields
            self._validate_group_consistency(
                group_df=group_df,
                group_name=pipeline_group,
                fields_to_validate=self.PIPELINE_CONSISTENCY_FIELDS,
                context='pipeline group'
            )

            gateway_id = group_df.iloc[0]['gateway']

            # Generate resource names
            names = self._generate_resource_names(pipeline_group)

            # Get target catalog and schema
            target_catalog = group_df.iloc[0]['target_catalog']
            target_schema = group_df.iloc[0]['target_schema']

            tables = [{
                'table': {
                    'source_catalog': row['source_database'],
                    'source_schema': row['source_schema'],
                    'source_table': row['source_table_name'],
                    'destination_catalog': row['target_catalog'],
                    'destination_schema': row['target_schema'],
                    'destination_table': row['target_table_name']
                }
            } for _, row in group_df.iterrows()]

            pipelines[names['pipeline_resource_name']] = {
                'name': names['pipeline_name'],
                'ingestion_definition': {
                    'ingestion_gateway_id': f"${{resources.pipelines.gateway_{gateway_id}.id}}",
                    'objects': tables
                },
                'schema': target_schema,
                'catalog': target_catalog
            }

            # Optional: tags (applied to the ingestion pipeline)
            tags = self._parse_tags(group_df.iloc[0].get("tags"))
            if tags:
                pipelines[names["pipeline_resource_name"]]["tags"] = tags

        return {'resources': {'pipelines': pipelines}}


class SaaSConnector(BaseConnector):
    """
    Abstract base class for SaaS connectors without gateway support.

    SaaS connectors use single-level load balancing:
    - Split into pipelines when group exceeds max_tables_per_pipeline

    Examples: Salesforce, Google Analytics, ServiceNow, Workday
    """

    # Validation configuration - fields that must be consistent within pipeline groups (SaaS-specific)
    PIPELINE_CONSISTENCY_FIELDS = ['connection_name', 'tags']

    def _create_pipelines(self, df: pd.DataFrame, project_name: str) -> Dict:
        """
        Validate pipeline consistency and prepare for pipeline creation.

        This method validates that pipeline-level fields are consistent within
        each pipeline group. Child classes should call super()._create_pipelines()
        first to get validation, then implement their connector-specific pipeline
        structure (table mappings, column filters, etc.).

        Args:
            df: DataFrame with pipeline configuration for a single project
            project_name: Project name for resource naming

        Returns:
            None - validation only, children implement the actual pipeline creation
        """
        # Validate pipeline-level fields
        for pipeline_group, group_df in df.groupby('pipeline_group'):
            self._validate_group_consistency(
                group_df=group_df,
                group_name=pipeline_group,
                fields_to_validate=self.PIPELINE_CONSISTENCY_FIELDS,
                context='pipeline group'
            )

    def generate_pipeline_config(
        self,
        df: pd.DataFrame,
        max_tables_per_pipeline: int = None
    ) -> pd.DataFrame:
        """
        Generate SaaS pipeline configuration with single-level load balancing.

        Uses prefix + subgroup grouping with single-level splitting:
        - Split into pipelines if exceeds max_tables_per_pipeline

        Args:
            df: Normalized input DataFrame
            max_tables_per_pipeline: Maximum items per pipeline (default: DEFAULT_MAX_TABLES_PER_PIPELINE)

        Returns:
            DataFrame with 'pipeline_group' column added
        """
        # Apply default constant if not specified
        if max_tables_per_pipeline is None:
            max_tables_per_pipeline = self.DEFAULT_MAX_TABLES_PER_PIPELINE

        df = df.copy()

        # Ensure consistent string formatting
        df['prefix'] = df['prefix'].astype(str)
        df['subgroup'] = df['subgroup'].astype(str).str.zfill(2)

        # Generate base group from prefix + subgroup
        df['base_group'] = df['prefix'] + '_' + df['subgroup']

        # Split groups by capacity
        df = self._split_groups_by_size(
            df=df,
            group_column='base_group',
            max_size=max_tables_per_pipeline,
            output_column='pipeline_group',
            suffix='p'
        )

        # Drop temporary base_group column
        df = df.drop(columns=['base_group'])

        return df

    def generate_yaml_files(
        self,
        df: pd.DataFrame,
        output_dir: str,
        targets: Dict[str, Dict]
    ):
        """
        Generate YAML files for SaaS connectors (no gateways).

        Creates a DAB structure for each project:
        - databricks.yml (root configuration)
        - resources/pipelines.yml (pipeline definitions)
        - resources/jobs.yml (scheduled jobs)

        Args:
            df: DataFrame with pipeline configuration
            output_dir: Output directory for DAB files
            targets: Dictionary of target environments

        Raises:
            YAMLGenerationError: If file writing fails
        """
        logger.info(f"Generating DAB YAML for {self.connector_type}")
        logger.debug(f"Total items: {len(df)}, pipelines: {df['pipeline_group'].nunique()}")

        # Group by project_name and create separate DAB packages
        for project_name, project_df in df.groupby('project_name'):
            project_output_dir = Path(output_dir) / str(project_name)
            logger.info(f"Creating DAB for project: {project_name}")
            logger.debug(f"  Items: {len(project_df)}, pipelines: {project_df['pipeline_group'].nunique()}")

            # Generate YAML content for this project
            pipelines_yaml = self._create_pipelines(project_df, str(project_name))
            jobs_yaml = self._create_jobs(project_df, str(project_name))
            databricks_yaml = self._create_databricks_yml(
                project_name=str(project_name),
                targets=targets,
                default_target='dev'
            )

            # Create directory structure
            resources_dir = project_output_dir / 'resources'
            try:
                resources_dir.mkdir(parents=True, exist_ok=True)
            except OSError as e:
                raise YAMLGenerationError(f"Failed to create directory {resources_dir}: {e}")

            # Write YAML files with retry logic
            databricks_yml_path = project_output_dir / 'databricks.yml'
            pipelines_yml_path = resources_dir / 'pipelines.yml'
            jobs_yml_path = resources_dir / 'jobs.yml'

            self._write_yaml_file(databricks_yml_path, databricks_yaml)
            self._write_yaml_file(pipelines_yml_path, pipelines_yaml)
            self._write_yaml_file(jobs_yml_path, jobs_yaml)
