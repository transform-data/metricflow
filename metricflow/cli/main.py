import logging
import signal
import sys

import click
import datetime as dt
import jinja2
import os
import pandas as pd
import pathlib
import textwrap
import time

from halo import Halo
from importlib.metadata import version as pkg_version
from packaging.version import parse
from typing import Callable, List, Optional
from update_checker import UpdateChecker

from metricflow.cli import PACKAGE_NAME
from metricflow.cli.constants import DEFAULT_RESULT_DECIMAL_PLACES, MAX_LIST_OBJECT_ELEMENTS
from metricflow.cli.cli_context import CLIContext
from metricflow.cli.tutorial import create_sample_data, gen_sample_model_configs, remove_sample_tables
from metricflow.cli.utils import (
    MF_BIGQUERY_KEYS,
    MF_CONFIG_KEYS,
    MF_REDSHIFT_KEYS,
    MF_SNOWFLAKE_KEYS,
    exception_handler,
    query_options,
    separated_by_comma_option,
    start_end_time_options,
    generate_duckdb_demo_keys,
    MF_POSTGRESQL_KEYS,
)
from metricflow.configuration.config_builder import YamlTemplateBuilder
from metricflow.dataflow.dataflow_plan_to_text import dataflow_plan_as_text
from metricflow.engine.metricflow_engine import MetricFlowQueryRequest, MetricFlowExplainResult, MetricFlowQueryResult
from metricflow.engine.utils import model_build_result_from_config, path_to_models
from metricflow.model.data_warehouse_model_validator import DataWarehouseModelValidator
from metricflow.model.model_validator import ModelValidator
from metricflow.model.objects.user_configured_model import UserConfiguredModel
from metricflow.model.parsing.config_linter import ConfigLinter
from metricflow.model.validations.validator_helpers import ModelValidationResults
from metricflow.sql_clients.common_client import SqlDialect
from metricflow.telemetry.models import TelemetryLevel
from metricflow.telemetry.reporter import TelemetryReporter, log_call
from metricflow.dag.dag_visualization import display_dag_as_svg

logger = logging.getLogger(__name__)

pass_config = click.make_pass_decorator(CLIContext, ensure=True)
_telemetry_reporter = TelemetryReporter(report_levels_higher_or_equal_to=TelemetryLevel.USAGE)
_telemetry_reporter.add_python_log_handler()
_telemetry_reporter.add_rudderstack_handler()


@click.group()
@click.option("-v", "--verbose", is_flag=True)
@pass_config
@log_call(module_name=__name__, telemetry_reporter=_telemetry_reporter)
def cli(cfg: CLIContext, verbose: bool) -> None:  # noqa: D
    cfg.verbose = verbose

    checker = UpdateChecker()
    result = checker.check(PACKAGE_NAME, pkg_version(PACKAGE_NAME))
    # result is None when an update was not found or a failure occurred
    if result:
        click.secho(
            "‼️ Warning: A new version of the MetricFlow CLI is available.",
            bold=True,
            fg="red",
        )

        click.echo(
            f"💡 Please update to version {result.available_version}, released {result.release_date} by running:\n"
            f"\t$ pip install --upgrade {PACKAGE_NAME}\n",
        )

    # Cancel queries submitted to the DW if the user precess CTRL + c / process is terminated.
    # Note: docs unclear on the type for the 'frame' argument.
    def exit_signal_handler(signal_type: int, frame) -> None:  # type: ignore
        if signal_type == signal.SIGINT:
            click.echo("Got SIGINT")
        elif signal_type == signal.SIGTERM:
            click.echo("Got SIGTERM")
        else:
            # Shouldn't happen since this should ony be registered for SIGINT / SIGTERM.
            click.echo(f"Got unhandled signal {signal_type}")
            return

        try:
            if cfg.sql_client.sql_engine_attributes.cancel_submitted_queries_supported:
                logger.info("Cancelling submitted queries")
                cfg.sql_client.cancel_submitted_queries()
                cfg.sql_client.close()
        finally:
            sys.exit(-1)

    signal.signal(signal.SIGINT, exit_signal_handler)
    signal.signal(signal.SIGTERM, exit_signal_handler)


@cli.command()
def version() -> None:
    """Print the current version of the MetricFlow CLI."""
    click.echo(pkg_version(PACKAGE_NAME))


@cli.command()
@click.option("--restart", is_flag=True, help="Wipe the config file and start over")
@pass_config
@log_call(module_name=__name__, telemetry_reporter=_telemetry_reporter)
def setup(cfg: CLIContext, restart: bool) -> None:
    """Setup MetricFlow."""

    click.echo(
        textwrap.dedent(
            """\
            🎉 Welcome to MetricFlow! 🎉
            """
        )
    )

    path = pathlib.Path(cfg.config.file_path)
    abs_path = path.absolute()
    to_create = not path.exists() or restart

    # Seed the config template to the config file
    if to_create:
        dialect_map = {
            SqlDialect.SNOWFLAKE.value: MF_SNOWFLAKE_KEYS,
            SqlDialect.BIGQUERY.value: MF_BIGQUERY_KEYS,
            SqlDialect.REDSHIFT.value: MF_REDSHIFT_KEYS,
            SqlDialect.POSTGRESQL.value: MF_POSTGRESQL_KEYS,
            SqlDialect.DUCKDB.value: generate_duckdb_demo_keys(config_dir=cfg.config.dir_path),
        }

        click.echo("Please enter your data warehouse dialect.")
        click.echo("Use 'duckdb' for a standalone demo.")
        click.echo("")
        dialect = click.prompt(
            "Dialect",
            type=click.Choice(sorted([x for x in dialect_map.keys()])),
            show_choices=True,
        )

        # If there is a collision, prefer to use the key in the dialect.
        config_keys = list(dialect_map[dialect])
        for mf_config_key in MF_CONFIG_KEYS:
            if not any(x.key == mf_config_key.key for x in config_keys):
                config_keys.append(mf_config_key)

        with open(abs_path, "w") as file:
            YamlTemplateBuilder.write_yaml(config_keys, file)

    template_description = (
        f"A template config file has been created in {abs_path}.\n"
        if to_create
        else f"A template config file already exists in {abs_path}, so it was left alone.\n"
    )
    click.echo(
        textwrap.dedent(
            f"""\
            💻 {template_description}
            Next steps:
              1. Review and fill out relevant fields.
              2. Run `mf health-checks` to validate the data warehouse connection.
              3. Run `mf validate-configs` to validate the model configurations.
            """
        )
    )


@cli.command()
@click.option("-m", "--msg", is_flag=True, help="Output the final steps dialogue")
@click.option("--skip-dw", is_flag=True, help="Skip the data warehouse health checks")
@click.option("--drop-tables", is_flag=True, help="Drop all the dummy tables created via tutorial")
@pass_config
@click.pass_context
@log_call(module_name=__name__, telemetry_reporter=_telemetry_reporter)
def tutorial(ctx: click.core.Context, cfg: CLIContext, msg: bool, skip_dw: bool, drop_tables: bool) -> None:
    """Run user through a tutorial."""

    # This text is also located in the projects top-level README.md
    help_msg = textwrap.dedent(
        """\
        🤓 Please run the following steps,

            1.  In '{$HOME}/.metricflow/config.yml', `model_path` should be '{$HOME}/.metricflow/sample_models'.
            2.  Try validating your data model: `mf validate-configs`
            3.  Check out your metrics: `mf list-metrics`
            4.  Check out dimensions for your metric `mf list-dimensions --metric-names transactions`
            5.  Query your first metric: `mf query --metrics transactions --dimensions ds --order ds`
            6.  Show the SQL MetricFlow generates: `mf query --metrics transactions --dimensions ds --order ds --explain`
            7.  Visualize the plan: `mf query --metrics transactions --dimensions ds --order ds --explain --display-plans`
                * This only works if you have graphviz installed - see README.
                * Aesthetic improvements to the visualization are TBD.
            8.  Add another dimension: `mf query --metrics transactions --dimensions ds,customer__country --order ds`
            9.  Add a higher date granularity: `mf query --metrics transactions --dimensions ds__week --order ds__week`
            10. Try a more complicated query: `mf query --metrics transactions,transaction_usd_na,transaction_usd_na_l7d --dimensions ds,is_large --order ds --start-time 2022-03-20 --end-time 2022-04-01`
                * You can also add `--explain --display-plans`.
            11. For more ways to interact with the sample models, go to ‘https://docs.transform.co/docs/metricflow/metricflow-tutorial’.
            12. Once you’re done, run `mf tutorial --skip-dw --drop-tables` to drop the sample tables.
        """
    )

    if msg:
        click.echo(help_msg)
        exit()

    # Check if the MetricFlow configuration file exists
    path = pathlib.Path(cfg.config.file_path)
    if not path.absolute().exists():
        click.echo("💡 Please run `mf setup` to get your configs set up before going through the tutorial.")
        exit()

    # Validate that the data warehouse connection is successful
    if not skip_dw:
        ctx.invoke(health_checks)
        click.confirm("❓ Are the health-checks all passing? Please fix them before continuing", abort=True)
        click.echo("💡 For future reference, you can continue with the tutorial by adding `--skip-dw`\n")

    if drop_tables:
        spinner = Halo(text="Dropping tables...", spinner="dots")
        spinner.start()
        remove_sample_tables(sql_client=cfg.sql_client, system_schema=cfg.mf_system_schema)
        spinner.succeed("Tables dropped")
        exit()

    # Seed sample data into data warehouse
    spinner = Halo(text=f"🤖 Generating sample data into schema {cfg.mf_system_schema}...", spinner="dots")
    spinner.start()
    created = create_sample_data(sql_client=cfg.sql_client, system_schema=cfg.mf_system_schema)
    if not created:
        spinner.warn("🙊 Skipped creating sample tables since they already exist.")
    else:
        spinner.succeed("📀 Sample tables have been successfully created into your data warehouse.")

    # Seed sample model file
    model_path = os.path.join(cfg.config.dir_path, "sample_models")
    if not os.path.exists(model_path):
        pathlib.Path(model_path).mkdir(parents=True)
    click.echo(f"🤖 Attempting to generate model configs to your local filesystem in '{str(model_path)}'.")
    spinner = Halo(text="Dropping tables...", spinner="dots")
    spinner.start()
    gen_sample_model_configs(dir_path=str(model_path), system_schema=cfg.mf_system_schema)
    spinner.succeed(f"📜 Model configs has been generated into '{model_path}'")

    click.echo(help_msg)
    click.echo("💡 Run `mf tutorial --msg` to see this message again without executing everything else")
    exit()


@cli.command()
@query_options
@click.option(
    "--as-table",
    required=False,
    type=str,
    help="Output the data to a specified SQL table in the form of '<schema>.<table>'",
)
@click.option(
    "--csv",
    type=click.File("wb"),
    required=False,
    help="Provide filepath for dataframe output to csv",
)
@click.option(
    "--explain",
    is_flag=True,
    required=False,
    default=False,
    help="In the query output, show the query that was executed against the data warehouse",
)
@click.option(
    "--display-plans",
    is_flag=True,
    required=False,
    help="Display plans (e.g. metric dataflow) in the browser",
)
@click.option(
    "--decimals",
    required=False,
    default=2,
    help="Choose the number of decimal places to round for the numerical values",
)
@click.option(
    "--show-sql-descriptions",
    is_flag=True,
    default=False,
    help="Shows inline descriptions of nodes in displayed SQL",
)
@pass_config
@exception_handler
@log_call(module_name=__name__, telemetry_reporter=_telemetry_reporter)
def query(
    cfg: CLIContext,
    metrics: List[str],
    dimensions: List[str],
    where: Optional[str] = None,
    start_time: Optional[dt.datetime] = None,
    end_time: Optional[dt.datetime] = None,
    order: Optional[List[str]] = None,
    limit: Optional[int] = None,
    as_table: Optional[str] = None,
    csv: Optional[click.utils.LazyFile] = None,
    explain: bool = False,
    display_plans: bool = False,
    decimals: int = DEFAULT_RESULT_DECIMAL_PLACES,
    show_sql_descriptions: bool = False,
) -> None:
    """Create a new query with MetricFlow and assembles a MetricFlowQueryResult."""
    start = time.time()
    spinner = Halo(text="Initiating query…", spinner="dots")
    spinner.start()

    mf_request = MetricFlowQueryRequest.create_with_random_request_id(
        metric_names=metrics,
        group_by_names=dimensions,
        limit=limit,
        time_constraint_start=start_time,
        time_constraint_end=end_time,
        where_constraint=where,
        order_by_names=order,
        output_table=as_table,
    )

    explain_result: Optional[MetricFlowExplainResult] = None
    query_result: Optional[MetricFlowQueryResult] = None

    if explain:
        explain_result = cfg.mf.explain(mf_request=mf_request)
    else:
        query_result = cfg.mf.query(mf_request=mf_request)

    spinner.succeed(f"Success 🦄 - query completed after {time.time() - start:.2f} seconds")

    if explain:
        assert explain_result
        sql = (
            explain_result.rendered_sql_without_descriptions.sql_query
            if not show_sql_descriptions
            else explain_result.rendered_sql.sql_query
        )
        click.echo("🔎 Generated Dataflow Plan + SQL (remove --explain to see data):")
        click.echo(
            textwrap.indent(
                jinja2.Template(
                    textwrap.dedent(
                        """\
                        Metric Dataflow Plan:
                            {{ plan_text | indent(4) }}
                        """
                    ),
                    undefined=jinja2.StrictUndefined,
                ).render(plan_text=dataflow_plan_as_text(explain_result.dataflow_plan)),
                prefix="-- ",
            )
        )
        click.echo("")
        click.echo(sql)
        if display_plans:
            svg_path = display_dag_as_svg(explain_result.dataflow_plan, cfg.config.dir_path)
            click.echo("")
            click.echo(f"Plan SVG saved to: {svg_path}")
        exit()

    assert query_result
    df = query_result.result_df
    # Show the data if returned successfully
    if df is not None:
        if df.empty:
            click.echo("🕳 Successful MQL query returned an empty result set.")
        elif csv is not None:
            # csv is a LazyFile that is file-like that works in this case.
            df.to_csv(csv, index=False)  # type: ignore
            click.echo(f"🖨 Successfully written query output to {csv.name}")
        else:
            # NOTE: remove `to_string` if no pandas dependency is < 1.1.0
            if parse(pd.__version__) >= parse("1.1.0"):
                click.echo(df.to_markdown(index=False, floatfmt=f".{decimals}f"))
            else:
                click.echo(df.to_string(index=False, float_format=lambda x: format(x, f".{decimals}f")))

        if display_plans:
            svg_path = display_dag_as_svg(query_result.dataflow_plan, cfg.config.dir_path)
            click.echo(f"Plan SVG saved to: {svg_path}")


@cli.command()
@click.option("--search", required=False, type=str, help="Filter available metrics by this search term")
@click.option("--show-all-dims", is_flag=True, default=False, help="Show all dimensions associated with a metric.")
@pass_config
@exception_handler
@log_call(module_name=__name__, telemetry_reporter=_telemetry_reporter)
def list_metrics(cfg: CLIContext, show_all_dims: bool = False, search: Optional[str] = None) -> None:
    """List the metrics with their available dimensions.

    Automatically truncates long lists of dimensions, pass --show-all-dims to see all.
    """

    spinner = Halo(text="🔍 Looking for all available metrics...", spinner="dots")
    spinner.start()

    metrics = cfg.mf.list_metrics()

    if not metrics:
        spinner.fail("List of metrics unavailable.")

    filter_msg = ""
    if search is not None:
        num_metrics = len(metrics)
        metrics = [m for m in metrics if search.lower() in m.name.lower()]
        filter_msg = f" matching `{search}`, of a total of {num_metrics} available"

    spinner.succeed(f"🌱 We've found {len(metrics)} metrics{filter_msg}.")
    click.echo('The list below shows metrics in the format of "metric_name: list of available dimensions"')
    num_dims_to_show = MAX_LIST_OBJECT_ELEMENTS
    for m in metrics:
        # sort dimensions by whether they're local first(if / then global else local) then the dim name
        dimensions = sorted(map(lambda d: d.name, filter(lambda d: "/" not in d.name, m.dimensions))) + sorted(
            map(lambda d: d.name, filter(lambda d: "/" in d.name, m.dimensions))
        )
        if show_all_dims:
            num_dims_to_show = len(dimensions)
        click.echo(
            f"• {click.style(m.name, bold=True, fg='green')}: {', '.join(dimensions[:num_dims_to_show])}"
            + (f" and {len(dimensions) - num_dims_to_show} more" if len(dimensions) > num_dims_to_show else "")
        )


@cli.command()
@separated_by_comma_option(
    "--metric-names", help_msg="List dimensions by given metrics (intersection). Ex. --metric-names bookings,messages"
)
@pass_config
@exception_handler
@log_call(module_name=__name__, telemetry_reporter=_telemetry_reporter)
def list_dimensions(cfg: CLIContext, metric_names: List[str]) -> None:
    """List all unique dimensions."""
    spinner = Halo(
        text="🔍 Looking for all available dimensions...",
        spinner="dots",
    )
    spinner.start()

    dimensions = cfg.mf.simple_dimensions_for_metrics(metric_names)
    if not dimensions:
        spinner.fail("List of dimensions unavailable.")

    spinner.succeed(f"🌱 We've found {len(dimensions)} common dimensions for metrics {metric_names}.")
    for d in dimensions:
        click.echo(f"• {click.style(d.name, bold=True, fg='green')}")


@cli.command()
@pass_config
@exception_handler
@log_call(module_name=__name__, telemetry_reporter=_telemetry_reporter)
def health_checks(cfg: CLIContext) -> None:
    """Performs a health check against the DW provided in the configs."""
    spinner = Halo(
        text="🏥 Running health checks against your data warehouse... (This should not take longer than 30s for a successful connection)",
        spinner="dots",
    )
    spinner.start()
    res = cfg.run_health_checks()
    spinner.succeed("Health checks completed.")
    for test in res:
        test_res = res[test]
        if test_res["status"] != "SUCCESS":
            click.echo(f"• ❌ {click.style(test, bold=True, fg=('red'))}:  Failed with - {test_res['error_message']}.")
        else:
            click.echo(f"• ✅ {click.style(test, bold=True, fg=('green'))}: Success!")


@cli.command()
@click.option("--dimension-name", required=True, type=str, help="Dimension to query values from")
@click.option("--metric-name", required=True, type=str, help="Metric that is associated with the dimension")
@start_end_time_options
@pass_config
@exception_handler
@log_call(module_name=__name__, telemetry_reporter=_telemetry_reporter)
def get_dimension_values(
    cfg: CLIContext,
    metric_name: str,
    dimension_name: str,
    start_time: Optional[dt.datetime] = None,
    end_time: Optional[dt.datetime] = None,
) -> None:
    """List all dimension values with the corresponding metric."""
    spinner = Halo(
        text=f"🔍 Retrieving dimension values for dimension '{dimension_name}' of metric '{metric_name}'...",
        spinner="dots",
    )
    spinner.start()

    dim_vals: Optional[List[str]] = None

    try:
        dim_vals = cfg.mf.get_dimension_values(
            metric_name=metric_name,
            get_group_by_values=dimension_name,
            time_constraint_start=start_time,
            time_constraint_end=end_time,
        )
    except Exception as e:
        spinner.fail()
        click.echo(
            textwrap.dedent(
                f"""\
                ❌ Failed to query dimension values for dimension {dimension_name} of metric {metric_name}.
                    ERROR: {str(e)}
                """
            )
        )
        exit(1)

    assert dim_vals
    spinner.succeed(
        f"🌱 We've found {len(dim_vals)} dimension values for dimension {dimension_name} of metric {metric_name}."
    )
    for dim_val in dim_vals:
        click.echo(f"• {click.style(dim_val, bold=True, fg='green')}")


@cli.command()
@click.option("--search", required=False, type=str, help="Filter available materializations by this search term")
@pass_config
@exception_handler
@log_call(module_name=__name__, telemetry_reporter=_telemetry_reporter)
def list_materializations(cfg: CLIContext, search: Optional[str] = None) -> None:
    """List the materializations with their available metrics and dimensions."""

    spinner = Halo(text="🔍 Looking for all available materializations...", spinner="dots")
    spinner.start()

    materializations = cfg.mf.list_materializations()
    if not materializations:
        spinner.fail("List of materializations unavailable.")

    filter_msg = ""
    if search is not None:
        count = len(materializations)
        materializations = [m for m in materializations if search.lower() in m.name.lower()]
        filter_msg = f" matching `{search}`, of a total of {count} available"

    spinner.succeed(f"🌱 We've found {len(materializations)} materializations{filter_msg}.")
    click.echo(
        'The list below shows materializations in the format of "materialization: details related to materialization"'
    )
    for m in materializations:
        dimensions = sorted(m.dimensions)
        metrics = sorted(m.metrics)
        # Materialization name
        click.echo(f"• {click.style(m.name, bold=True, fg='green')}:")
        # Metrics related to this materalization
        click.echo(
            f"Metrics: {', '.join(metrics[:MAX_LIST_OBJECT_ELEMENTS])}"
            + (
                f" and {len(metrics) - MAX_LIST_OBJECT_ELEMENTS} more"
                if len(metrics) > MAX_LIST_OBJECT_ELEMENTS
                else ""
            )
        )
        # Dimensions related to this materalization
        click.echo(
            f"Dimensions: {', '.join(dimensions[:MAX_LIST_OBJECT_ELEMENTS])}"
            + (
                f" and {len(dimensions) - MAX_LIST_OBJECT_ELEMENTS} more"
                if len(dimensions) > MAX_LIST_OBJECT_ELEMENTS
                else ""
            )
        )
        # Misc related to this materialization
        click.echo(f"destination table: {m.destination_table or m.name}")


@cli.command()
@click.option(
    "--materialization-name",
    required=True,
    type=str,
    help="Name of materialization to materialize",
)
@start_end_time_options
@pass_config
@exception_handler
@log_call(module_name=__name__, telemetry_reporter=_telemetry_reporter)
def materialize(
    cfg: CLIContext,
    materialization_name: str,
    start_time: Optional[dt.datetime] = None,
    end_time: Optional[dt.datetime] = None,
) -> None:
    """Create a new materialization query and returns materialized table"""
    if start_time is None and not click.confirm(
        "You haven't provided a start_time. This means we will materialize from the beginning of time. This may be expensive. Are you sure you want to continue?"
    ):
        click.echo("Exiting")
        exit()

    start = time.time()
    spinner = Halo(text="Initiating materialization query…", spinner="dots")
    spinner.start()

    result_table = cfg.mf.materialize(
        materialization_name,
        time_constraint_start=start_time,
        time_constraint_end=end_time,
    )

    spinner.succeed(f"Success 🦄 - materialize query completed after {time.time() - start:.2f} seconds.")
    click.echo(f"Materialized table created at: {result_table.sql}")


@cli.command()
@click.option(
    "--materialization-name",
    required=True,
    type=str,
    help="Name of materialization to drop",
)
@pass_config
@exception_handler
@log_call(module_name=__name__, telemetry_reporter=_telemetry_reporter)
def drop_materialization(cfg: CLIContext, materialization_name: str) -> None:
    """Drops a given materialized table."""

    start = time.time()
    spinner = Halo(text="Initiating drop materialization query…", spinner="dots")
    spinner.start()

    result = cfg.mf.drop_materialization(materialization_name=materialization_name)

    if result:
        spinner.succeed(f"Success 🦄 - drop materialization query completed after {time.time() - start:.2f} seconds.")
    else:
        spinner.warn(f"Materialized table for `{materialization_name}` did not exist, no table was dropped")


def _print_issues(
    issues: ModelValidationResults, show_non_blocking: bool = False, verbose: bool = False
) -> None:  # noqa: D
    for issue in issues.fatals:
        print(f"• {issue.as_readable_str(verbose=verbose)}")
    for issue in issues.errors:
        print(f"• {issue.as_readable_str(verbose=verbose)}")
    if show_non_blocking:
        for issue in issues.future_errors:
            print(f"• {issue.as_readable_str(verbose=verbose)}")
        for issue in issues.warnings:
            print(f"• {issue.as_readable_str(verbose=verbose)}")


def _run_dw_validations(
    validation_func: Callable[[UserConfiguredModel, Optional[int]], ModelValidationResults],
    validation_type: str,
    model: UserConfiguredModel,
    timeout: Optional[int],
) -> ModelValidationResults:
    """Helper handles the calling of data warehouse issue generating functions"""

    spinner = Halo(text=f"Validating {validation_type} against data warehouse...", spinner="dots")
    spinner.start()

    results = validation_func(model, timeout)
    if not results.has_blocking_issues:
        spinner.succeed(f"🎉 Successfully validated {validation_type} against data warehouse ({results.summary()})")
    else:
        spinner.fail(
            f"Breaking issues found when validating {validation_type} against data warehouse ({results.summary()})"
        )
    return results


def _data_warehouse_validations_runner(
    dw_validator: DataWarehouseModelValidator, model: UserConfiguredModel, timeout: Optional[int]
) -> ModelValidationResults:
    """Helper which calls the individual data warehouse validations to run and prints collected issues"""

    data_source_results = _run_dw_validations(
        dw_validator.validate_data_sources, model=model, validation_type="data sources", timeout=timeout
    )
    dimension_results = _run_dw_validations(
        dw_validator.validate_dimensions, model=model, validation_type="dimensions", timeout=timeout
    )
    metric_results = _run_dw_validations(
        dw_validator.validate_metrics, model=model, validation_type="metrics", timeout=timeout
    )

    return ModelValidationResults.merge([data_source_results, dimension_results, metric_results])


@cli.command()
@click.option(
    "--dw-timeout", required=False, type=int, help="Optional timeout for data warehouse validation steps. Default None."
)
@click.option(
    "--skip-dw",
    is_flag=True,
    default=False,
    help="If specified, skips the data warehouse validations",
)
@click.option("--show-all", is_flag=True, default=False, help="If specified, prints warnings and future-errors")
@click.option(
    "--verbose-issues", is_flag=True, default=False, help="If specified, prints any extra details issues might have"
)
@pass_config
@exception_handler
@log_call(module_name=__name__, telemetry_reporter=_telemetry_reporter)
def validate_configs(
    cfg: CLIContext,
    dw_timeout: Optional[int] = None,
    skip_dw: bool = False,
    show_all: bool = False,
    verbose_issues: bool = False,
) -> None:
    """Perform validations against the defined model configurations."""
    cfg.verbose = True

    if not show_all:
        print("(To see warnings and future-errors, run again with flag `--show-all`)")

    # Lint Validation
    lint_spinner = Halo(text="Checking for YAML format issues", spinner="dots")
    lint_spinner.start()

    lint_results = ConfigLinter().lint_dir(path_to_models(handler=cfg.config))
    if not lint_results.has_blocking_issues:
        lint_spinner.succeed(f"🎉 Successfully linted config YAML files ({lint_results.summary()})")
    else:
        lint_spinner.fail(f"Breaking issues found in config YAML files ({lint_results.summary()})")
        _print_issues(lint_results, show_non_blocking=show_all, verbose=verbose_issues)
        return

    # Parsing Validation
    parsing_spinner = Halo(text="Building model from configs", spinner="dots")
    parsing_spinner.start()
    parsing_result = model_build_result_from_config(handler=cfg.config, raise_issues_as_exceptions=False)

    if not parsing_result.issues.has_blocking_issues:
        parsing_spinner.succeed(f"🎉 Successfully built model from configs ({parsing_result.issues.summary()})")
    else:
        parsing_spinner.fail(
            f"Breaking issues found when building model from configs ({parsing_result.issues.summary()})"
        )
        _print_issues(parsing_result.issues, show_non_blocking=show_all, verbose=verbose_issues)
        return

    user_model = parsing_result.model

    # Semantic validation
    semantic_spinner = Halo(text="Validating semantics of built model", spinner="dots")
    semantic_spinner.start()
    semantic_result = ModelValidator().validate_model(user_model)

    if not semantic_result.issues.has_blocking_issues:
        semantic_spinner.succeed(
            f"🎉 Successfully validated the semantics of built model ({semantic_result.issues.summary()})"
        )
    else:
        semantic_spinner.fail(
            f"Breaking issues found when checking semantics of built model ({semantic_result.issues.summary()})"
        )
        _print_issues(semantic_result.issues, show_non_blocking=show_all, verbose=verbose_issues)
        return

    dw_results = ModelValidationResults()
    if not skip_dw:
        dw_validator = DataWarehouseModelValidator(sql_client=cfg.sql_client, mf_engine=cfg.mf)
        dw_results = _data_warehouse_validations_runner(dw_validator=dw_validator, model=user_model, timeout=dw_timeout)

    merged_results = ModelValidationResults.merge(
        [lint_results, parsing_result.issues, semantic_result.issues, dw_results]
    )
    _print_issues(merged_results, show_non_blocking=show_all, verbose=verbose_issues)


if __name__ == "__main__":
    cli()
