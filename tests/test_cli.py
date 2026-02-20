"""
Tests for the CLI entry point.

Tests argument parsing, settings file loading, and end-to-end CLI execution.
"""

import json
import pytest
from unittest.mock import patch
from pathlib import Path

from tapworks.cli import main, load_config_file, parse_json_arg


class TestParseJsonArg:
    """Tests for JSON argument parsing."""

    def test_valid_json(self):
        result = parse_json_arg('{"key": "value"}')
        assert result == {"key": "value"}

    def test_empty_string_returns_empty_dict(self):
        result = parse_json_arg("")
        assert result == {}

    def test_none_returns_empty_dict(self):
        result = parse_json_arg(None)
        assert result == {}

    def test_invalid_json_raises_error(self):
        import argparse
        with pytest.raises(argparse.ArgumentTypeError, match="Invalid JSON"):
            parse_json_arg("{invalid}")

    def test_nested_json(self):
        result = parse_json_arg('{"targets": {"dev": {"host": "https://..."}}}')
        assert result["targets"]["dev"]["host"] == "https://..."


class TestLoadConfigFile:
    """Tests for settings file loading."""

    def test_load_json_file(self, tmp_path):
        config = {"targets": {"dev": {"workspace_host": "https://dev"}}}
        config_path = tmp_path / "settings.json"
        config_path.write_text(json.dumps(config))

        result = load_config_file(str(config_path))
        assert result == config

    def test_load_yaml_file(self, tmp_path):
        config_path = tmp_path / "settings.yaml"
        config_path.write_text("targets:\n  dev:\n    workspace_host: https://dev\n")

        result = load_config_file(str(config_path))
        assert result["targets"]["dev"]["workspace_host"] == "https://dev"

    def test_load_yml_extension(self, tmp_path):
        config_path = tmp_path / "settings.yml"
        config_path.write_text("targets:\n  dev:\n    workspace_host: https://dev\n")

        result = load_config_file(str(config_path))
        assert "targets" in result

    def test_missing_file_raises_error(self):
        with pytest.raises(FileNotFoundError):
            load_config_file("/nonexistent/settings.json")


class TestCLIMain:
    """Tests for main CLI entry point."""

    def test_list_flag(self, capsys):
        with patch("sys.argv", ["tapworks", "--list"]):
            with pytest.raises(SystemExit, match="0"):
                main()
        output = capsys.readouterr().out
        assert "salesforce" in output
        assert "sql_server" in output

    def test_info_flag(self, capsys):
        with patch("sys.argv", ["tapworks", "salesforce", "--info"]):
            with pytest.raises(SystemExit, match="0"):
                main()
        output = capsys.readouterr().out
        assert "salesforce" in output
        assert "Required columns" in output

    def test_no_connector_shows_help(self, capsys):
        with patch("sys.argv", ["tapworks"]):
            with pytest.raises(SystemExit, match="1"):
                main()
        output = capsys.readouterr().err
        assert "connector name is required" in output

    def test_missing_input_config_errors(self, capsys):
        with patch("sys.argv", ["tapworks", "salesforce"]):
            with pytest.raises(SystemExit, match="1"):
                main()
        output = capsys.readouterr().err
        assert "--input-config is required" in output

    def test_missing_targets_errors(self, capsys, tmp_path):
        csv_path = tmp_path / "input.csv"
        csv_path.write_text("col1\nval1\n")

        with patch("sys.argv", ["tapworks", "salesforce", "--input-config", str(csv_path)]):
            with pytest.raises(SystemExit, match="1"):
                main()
        output = capsys.readouterr().err
        assert "No targets" in output

    def test_end_to_end_with_settings_file(
        self,
        sample_salesforce_df,
        tmp_path,
        capsys,
    ):
        csv_path = tmp_path / "input.csv"
        sample_salesforce_df.to_csv(csv_path, index=False)

        output_dir = tmp_path / "output"

        settings = {
            "targets": {"dev": {"workspace_host": "https://dev.databricks.com"}},
            "default_values": {"project_name": "cli_test"},
        }
        settings_path = tmp_path / "settings.json"
        settings_path.write_text(json.dumps(settings))

        with patch("sys.argv", [
            "tapworks", "salesforce",
            "--input-config", str(csv_path),
            "--output-dir", str(output_dir),
            "--settings", str(settings_path),
        ]):
            main()

        assert (output_dir / "cli_test" / "databricks.yml").exists()
        assert (output_dir / "cli_test" / "resources" / "pipelines.yml").exists()

    def test_end_to_end_with_inline_json(
        self,
        sample_salesforce_df,
        tmp_path,
        capsys,
    ):
        csv_path = tmp_path / "input.csv"
        sample_salesforce_df.to_csv(csv_path, index=False)

        output_dir = tmp_path / "output"

        with patch("sys.argv", [
            "tapworks", "salesforce",
            "--input-config", str(csv_path),
            "--output-dir", str(output_dir),
            "--targets", '{"dev": {"workspace_host": "https://dev"}}',
            "--default-values", '{"project_name": "inline_test"}',
        ]):
            main()

        assert (output_dir / "inline_test" / "databricks.yml").exists()

    def test_unknown_connector_errors(self, capsys):
        with patch("sys.argv", ["tapworks", "nonexistent", "--input-config", "x.csv",
                                 "--targets", '{"dev": {"workspace_host": "https://dev"}}']):
            with pytest.raises(SystemExit, match="1"):
                main()
