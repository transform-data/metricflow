from datetime import datetime
from unittest.mock import patch, MagicMock

from metricflow.cli.cli_context import CLIContext
from metricflow.cli.main import (
    drop_materialization,
    get_dimension_values,
    health_checks,
    list_dimensions,
    list_materializations,
    list_metrics,
    materialize,
    query,
    validate_configs,
    version,
)
from metricflow.model.model_validator import ModelValidator
from metricflow.model.parsing.config_linter import ConfigLinter
from metricflow.model.validations.validator_helpers import (
    ModelValidationResults,
    ValidationError,
    ValidationFatal,
    ValidationFutureError,
    ValidationWarning,
)
from metricflow.test.fixtures.cli_fixtures import MetricFlowCliRunner


def test_query(cli_runner: MetricFlowCliRunner) -> None:  # noqa: D
    resp = cli_runner.run(query, args=["--metrics", "bookings", "--dimensions", "ds"])
    assert "bookings" in resp.output
    assert resp.exit_code == 0


def test_list_dimensions(cli_runner: MetricFlowCliRunner) -> None:  # noqa: D
    resp = cli_runner.run(list_dimensions, args=["--metric-names", "bookings"])

    assert "ds" in resp.output
    assert resp.exit_code == 0


def test_list_metrics(cli_runner: MetricFlowCliRunner) -> None:  # noqa: D
    resp = cli_runner.run(list_metrics)

    assert "bookings_per_listing: ds" in resp.output
    assert resp.exit_code == 0


def test_get_dimension_values(cli_runner: MetricFlowCliRunner) -> None:  # noqa: D
    resp = cli_runner.run(get_dimension_values, args=["--metric-name", "bookings", "--dimension-name", "is_instant"])

    actual_output_lines = sorted(resp.output.split("\n"))
    assert ["", "• False", "• True"] == actual_output_lines
    assert resp.exit_code == 0


def test_list_materializations(cli_runner: MetricFlowCliRunner) -> None:  # noqa: D
    resp = cli_runner.run(list_materializations)

    assert "materialization: details related to materialization" in resp.output
    assert resp.exit_code == 0


def test_materialize(cli_runner: MetricFlowCliRunner) -> None:  # noqa: D
    resp = cli_runner.run(
        materialize, args=["--materialization-name", "test_materialization", "--start-time", "2020-01-02"]
    )

    assert "Materialized table created at" in resp.output
    assert "test_materialization" in resp.output
    assert resp.exit_code == 0


def test_drop_materialization(cli_runner: MetricFlowCliRunner) -> None:  # noqa: D
    resp = cli_runner.run(drop_materialization, args=["--materialization-name", "test_materialization"])

    assert resp.exit_code == 0


def test_validate_configs(cli_runner: MetricFlowCliRunner) -> None:  # noqa: D
    # Mock build result for `model_build_result_from_config`
    mocked_parsing_result = MagicMock(issues=ModelValidationResults())
    # Mock validation errors in validate_model function
    issues = (
        ValidationWarning(context=None, message="warning_message"),  # type: ignore
        ValidationFutureError(context=None, message="future_error_message", error_date=datetime.now()),  # type: ignore
        ValidationError(context=None, message="error_message"),  # type: ignore
        ValidationFatal(context=None, message="fatal_message"),  # type: ignore
    )
    mocked_build_result = MagicMock(issues=ModelValidationResults.from_issues_sequence(issues))
    with patch("metricflow.cli.main.model_build_result_from_config", return_value=mocked_parsing_result):
        with patch("metricflow.cli.main.path_to_models", return_value=""):
            with patch.object(ModelValidator, "validate_model", return_value=mocked_build_result):
                resp = cli_runner.run(validate_configs)

    assert "fatal_message" in resp.output
    assert "error_message" in resp.output
    assert resp.exit_code == 0


def test_future_errors_and_warnings_conditionally_show_up(cli_runner: MetricFlowCliRunner) -> None:  # noqa: D
    # Mock build result for `model_build_result_from_config`
    mocked_parsing_result = MagicMock(issues=ModelValidationResults())
    # Mock validation errors in validate_model function
    issues = (
        ValidationWarning(context=None, message="warning_message"),  # type: ignore
        ValidationFutureError(context=None, message="future_error_message", error_date=datetime.now()),  # type: ignore
        ValidationError(context=None, message="error_message"),  # type: ignore
        ValidationFatal(context=None, message="fatal_message"),  # type: ignore
    )
    mocked_build_result = MagicMock(issues=ModelValidationResults.from_issues_sequence(issues))
    with patch("metricflow.cli.main.model_build_result_from_config", return_value=mocked_parsing_result):
        with patch("metricflow.cli.main.path_to_models", return_value=""):
            with patch.object(ModelValidator, "validate_model", return_value=mocked_build_result):
                resp = cli_runner.run(validate_configs)

    assert "warning_message" not in resp.output
    assert "future_error_message" not in resp.output
    assert resp.exit_code == 0

    with patch("metricflow.cli.main.model_build_result_from_config", return_value=mocked_parsing_result):
        with patch("metricflow.cli.main.path_to_models", return_value=""):
            with patch.object(ModelValidator, "validate_model", return_value=mocked_build_result):
                resp = cli_runner.run(validate_configs, ["--show-all"])

    assert "warning_message" in resp.output
    assert "future_error_message" in resp.output
    assert resp.exit_code == 0


def test_validate_configs_data_warehouse_validations(cli_runner: MetricFlowCliRunner) -> None:  # noqa: D
    # Mock build result for `model_build_result_from_config`
    mocked_parsing_result = MagicMock(issues=ModelValidationResults())
    dw_validation_issues = [
        ValidationError(context=None, message="Data Warehouse Error"),  # type: ignore
    ]
    with patch("metricflow.cli.main.model_build_result_from_config", return_value=mocked_parsing_result):
        with patch("metricflow.cli.main.path_to_models", return_value=""):
            with patch.object(CLIContext, "sql_client", return_value=None):  # type: ignore
                with patch(
                    "metricflow.cli.main._run_dw_validations",
                    return_value=ModelValidationResults(errors=dw_validation_issues),
                ):
                    resp = cli_runner.run(validate_configs)

    assert "Data Warehouse Error" in resp.output
    assert resp.exit_code == 0


def test_validate_configs_skip_data_warehouse_validations(cli_runner: MetricFlowCliRunner) -> None:  # noqa: D
    # Mock build result for `model_build_result_from_config`
    mocked_parsing_result = MagicMock(issues=ModelValidationResults())
    with patch("metricflow.cli.main.model_build_result_from_config", return_value=mocked_parsing_result):
        with patch("metricflow.cli.main.path_to_models", return_value=""):
            with patch.object(
                ModelValidator, "validate_model", return_value=MagicMock(issues=ModelValidationResults())
            ):
                resp = cli_runner.run(validate_configs, args=["--skip-dw"])

    assert "Data Warehouse Error" not in resp.output
    assert resp.exit_code == 0


def test_validate_configs_with_lint_issues(cli_runner: MetricFlowCliRunner) -> None:  # noqa: D
    lint_issues = [ValidationError(context=None, message="YAML Lint Error")]
    with patch("metricflow.cli.main.path_to_models", return_value=""):
        with patch.object(ConfigLinter, "lint_dir", return_value=ModelValidationResults(errors=lint_issues)):
            resp = cli_runner.run(validate_configs)

    assert "YAML Lint Error" in resp.output
    assert resp.exit_code == 0


def test_version(cli_runner: MetricFlowCliRunner) -> None:  # noqa: D
    resp = cli_runner.run(version)

    assert resp.output
    assert resp.exit_code == 0


def test_health_checks(cli_runner: MetricFlowCliRunner) -> None:  # noqa: D
    resp = cli_runner.run(health_checks)

    assert "SELECT 1: Success!" in resp.output
    assert resp.exit_code == 0
