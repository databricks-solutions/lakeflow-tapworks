"""
Tests for error handling functionality.

Tests custom exceptions, validation methods, and file I/O error handling.
"""

import pytest
import pandas as pd
import tempfile
import os
from pathlib import Path
from unittest.mock import patch, mock_open

from tapworks.core import (
    ConfigurationError,
    ValidationError,
    YAMLGenerationError,
    LakehouseTapworksError
)


class TestExceptionHierarchy:
    """Tests for custom exception hierarchy."""

    def test_configuration_error_inherits_from_base(self):
        """ConfigurationError should inherit from LakehouseTapworksError."""
        with pytest.raises(LakehouseTapworksError):
            raise ConfigurationError("test")

    def test_validation_error_inherits_from_base(self):
        """ValidationError should inherit from LakehouseTapworksError."""
        with pytest.raises(LakehouseTapworksError):
            raise ValidationError("test")

    def test_yaml_generation_error_inherits_from_base(self):
        """YAMLGenerationError should inherit from LakehouseTapworksError."""
        with pytest.raises(LakehouseTapworksError):
            raise YAMLGenerationError("test")

    def test_all_exceptions_inherit_from_exception(self):
        """All custom exceptions should inherit from Exception."""
        with pytest.raises(Exception):
            raise ConfigurationError("test")
        with pytest.raises(Exception):
            raise ValidationError("test")
        with pytest.raises(Exception):
            raise YAMLGenerationError("test")


class TestCronValidation:
    """Tests for cron expression validation."""

    def test_valid_5_field_cron(self, salesforce_connector):
        """Should accept valid 5-field cron expression."""
        result = salesforce_connector._validate_cron_expression("*/15 * * * *")
        assert result == "*/15 * * * *"

    def test_valid_6_field_cron(self, salesforce_connector):
        """Should accept valid 6-field cron expression (with seconds)."""
        result = salesforce_connector._validate_cron_expression("0 */15 * * * *")
        assert result == "0 */15 * * * *"

    def test_valid_7_field_quartz_cron(self, salesforce_connector):
        """Should accept valid 7-field Quartz cron expression."""
        result = salesforce_connector._validate_cron_expression("0 0 12 * * ? 2024")
        assert result == "0 0 12 * * ? 2024"

    def test_complex_cron_expression(self, salesforce_connector):
        """Should accept complex but valid cron expressions."""
        result = salesforce_connector._validate_cron_expression("0 0,30 9-17 * * MON-FRI")
        assert result == "0 0,30 9-17 * * MON-FRI"

    def test_empty_cron_raises_validation_error(self, salesforce_connector):
        """Should raise ValidationError for empty cron expression."""
        with pytest.raises(ValidationError, match="Empty"):
            salesforce_connector._validate_cron_expression("")

    def test_whitespace_cron_raises_validation_error(self, salesforce_connector):
        """Should raise ValidationError for whitespace-only cron expression."""
        with pytest.raises(ValidationError, match="Empty"):
            salesforce_connector._validate_cron_expression("   ")

    def test_none_cron_raises_validation_error(self, salesforce_connector):
        """Should raise ValidationError for None cron expression."""
        with pytest.raises(ValidationError, match="Empty"):
            salesforce_connector._validate_cron_expression(None)

    def test_invalid_field_count_raises_validation_error(self, salesforce_connector):
        """Should raise ValidationError for wrong number of fields."""
        with pytest.raises(ValidationError, match="expected 5-7 fields"):
            salesforce_connector._validate_cron_expression("* * *")

        with pytest.raises(ValidationError, match="expected 5-7 fields"):
            salesforce_connector._validate_cron_expression("* * * * * * * *")

    def test_strips_whitespace(self, salesforce_connector):
        """Should strip leading/trailing whitespace."""
        result = salesforce_connector._validate_cron_expression("  */15 * * * *  ")
        assert result == "*/15 * * * *"


class TestResourceNameValidation:
    """Tests for resource name validation."""

    def test_valid_simple_name(self, salesforce_connector):
        """Should accept simple alphanumeric names."""
        result = salesforce_connector._validate_resource_name("my_pipeline")
        assert result == "my_pipeline"

    def test_valid_name_with_numbers(self, salesforce_connector):
        """Should accept names with numbers (not at start)."""
        result = salesforce_connector._validate_resource_name("pipeline_123")
        assert result == "pipeline_123"

    def test_valid_name_with_hyphens(self, salesforce_connector):
        """Should accept names with hyphens."""
        result = salesforce_connector._validate_resource_name("my-pipeline-name")
        assert result == "my-pipeline-name"

    def test_valid_name_with_underscores(self, salesforce_connector):
        """Should accept names with underscores."""
        result = salesforce_connector._validate_resource_name("my_pipeline_name")
        assert result == "my_pipeline_name"

    def test_empty_name_raises_validation_error(self, salesforce_connector):
        """Should raise ValidationError for empty name."""
        with pytest.raises(ValidationError, match="Empty"):
            salesforce_connector._validate_resource_name("")

    def test_whitespace_name_raises_validation_error(self, salesforce_connector):
        """Should raise ValidationError for whitespace-only name."""
        with pytest.raises(ValidationError, match="Empty"):
            salesforce_connector._validate_resource_name("   ")

    def test_name_starting_with_number_raises_error(self, salesforce_connector):
        """Should raise ValidationError for names starting with number."""
        with pytest.raises(ValidationError, match="must start with a letter"):
            salesforce_connector._validate_resource_name("123pipeline")

    def test_name_starting_with_underscore_raises_error(self, salesforce_connector):
        """Should raise ValidationError for names starting with underscore."""
        with pytest.raises(ValidationError, match="must start with a letter"):
            salesforce_connector._validate_resource_name("_pipeline")

    def test_name_with_invalid_chars_raises_error(self, salesforce_connector):
        """Should raise ValidationError for names with invalid characters."""
        with pytest.raises(ValidationError, match="can only contain"):
            salesforce_connector._validate_resource_name("my.pipeline")

        with pytest.raises(ValidationError, match="can only contain"):
            salesforce_connector._validate_resource_name("my pipeline")

        with pytest.raises(ValidationError, match="can only contain"):
            salesforce_connector._validate_resource_name("my@pipeline")

    def test_name_exceeding_max_length_raises_error(self, salesforce_connector):
        """Should raise ValidationError for names exceeding 128 characters."""
        long_name = "a" * 129
        with pytest.raises(ValidationError, match="exceeds maximum length"):
            salesforce_connector._validate_resource_name(long_name)

    def test_name_at_max_length_accepted(self, salesforce_connector):
        """Should accept names exactly at 128 characters."""
        name = "a" * 128
        result = salesforce_connector._validate_resource_name(name)
        assert result == name

    def test_strips_whitespace(self, salesforce_connector):
        """Should strip leading/trailing whitespace."""
        result = salesforce_connector._validate_resource_name("  my_pipeline  ")
        assert result == "my_pipeline"


class TestYAMLWriteRetryLogic:
    """Tests for YAML file write with retry logic."""

    def test_successful_write(self, salesforce_connector, temp_output_dir):
        """Should successfully write YAML file."""
        test_path = temp_output_dir / "test.yml"
        content = {"key": "value", "nested": {"inner": 123}}

        salesforce_connector._write_yaml_file(test_path, content)

        assert test_path.exists()
        with open(test_path) as f:
            import yaml
            loaded = yaml.safe_load(f)
        assert loaded == content

    def test_retry_on_transient_failure(self, salesforce_connector, temp_output_dir):
        """Should retry on transient IO errors."""
        test_path = temp_output_dir / "test.yml"
        content = {"key": "value"}

        call_count = [0]
        original_open = open

        def failing_open(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] < 2:
                raise IOError("Transient error")
            return original_open(*args, **kwargs)

        with patch("builtins.open", failing_open):
            salesforce_connector._write_yaml_file(test_path, content, retries=3, retry_delay=0.01)

        assert call_count[0] == 2

    def test_raises_after_max_retries(self, salesforce_connector, temp_output_dir):
        """Should raise YAMLGenerationError after exhausting retries."""
        test_path = temp_output_dir / "test.yml"
        content = {"key": "value"}

        with patch("builtins.open", side_effect=IOError("Persistent error")):
            with pytest.raises(YAMLGenerationError, match="Failed to write.*after 3 attempts"):
                salesforce_connector._write_yaml_file(test_path, content, retries=3, retry_delay=0.01)


class TestConfigurationErrorInPipeline:
    """Tests for ConfigurationError in pipeline processing."""

    def test_empty_targets_raises_configuration_error(self, salesforce_connector, sample_salesforce_df, temp_output_dir):
        """Should raise ConfigurationError when targets dict is empty."""
        with pytest.raises(ConfigurationError, match="target"):
            salesforce_connector.run_complete_pipeline_generation(
                df=sample_salesforce_df,
                output_dir=str(temp_output_dir),
                targets={}
            )

    def test_missing_required_columns_raises_configuration_error(self, salesforce_connector, temp_output_dir, sample_targets_minimal):
        """Should raise ConfigurationError when required columns are missing."""
        incomplete_df = pd.DataFrame({
            'source_database': ['Salesforce'],
            # Missing other required columns
        })

        with pytest.raises(ConfigurationError, match="Missing required columns"):
            salesforce_connector.run_complete_pipeline_generation(
                df=incomplete_df,
                output_dir=str(temp_output_dir),
                targets=sample_targets_minimal
            )

    def test_empty_dataframe_raises_configuration_error(self, salesforce_connector, temp_output_dir, sample_targets_minimal):
        """Should raise ConfigurationError for empty DataFrame."""
        empty_df = pd.DataFrame()

        with pytest.raises(ConfigurationError, match="empty"):
            salesforce_connector.run_complete_pipeline_generation(
                df=empty_df,
                output_dir=str(temp_output_dir),
                targets=sample_targets_minimal
            )
