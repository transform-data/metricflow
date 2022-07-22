from copy import deepcopy
from typing import Tuple

from _pytest.fixtures import FixtureRequest

from metricflow.model.data_warehouse_model_validator import (
    DataWarehouseModelValidator,
    DataWarehouseTaskBuilder,
    DataWarehouseValidationTask,
)
from metricflow.model.model_transformer import ModelTransformer
from metricflow.model.objects.data_source import Mutability, MutabilityType
from metricflow.model.objects.elements.dimension import Dimension, DimensionType
from metricflow.model.objects.elements.identifier import Identifier, IdentifierType
from metricflow.model.objects.elements.measure import AggregationType, Measure
from metricflow.model.objects.user_configured_model import UserConfiguredModel
from metricflow.protocols.sql_client import SqlClient
from metricflow.sql.sql_bind_parameters import SqlBindParameters
from metricflow.test.fixtures.setup_fixtures import MetricFlowTestSessionState
from metricflow.test.model.validations.helpers import data_source_with_guaranteed_meta
from metricflow.test.plan_utils import assert_snapshot_text_equal, make_schema_replacement_function


def test_build_data_source_tasks(
    mf_test_session_state: MetricFlowTestSessionState,
    data_warehouse_validation_model: UserConfiguredModel,
    sql_client: SqlClient,
) -> None:  # noqa:D
    tasks = DataWarehouseTaskBuilder.gen_data_source_tasks(
        model=data_warehouse_validation_model,
        sql_client=sql_client,
        system_schema=mf_test_session_state.mf_system_schema,
    )
    assert len(tasks) == len(data_warehouse_validation_model.data_sources)


def test_task_runner(sql_client: SqlClient, mf_test_session_state: MetricFlowTestSessionState) -> None:  # noqa: D
    dw_validator = DataWarehouseModelValidator(
        sql_client=sql_client, system_schema=mf_test_session_state.mf_system_schema
    )

    def good_query() -> Tuple[str, SqlBindParameters]:
        return ("SELECT 'foo' AS foo", SqlBindParameters())

    tasks = [
        DataWarehouseValidationTask(query_and_params_callable=good_query, error_message="Could not select foo"),
    ]

    issues = dw_validator.run_tasks(tasks=tasks)
    assert len(issues.all_issues) == 0

    def bad_query() -> Tuple[str, SqlBindParameters]:
        return ("SELECT (true) AS col1 FROM doesnt_exist", SqlBindParameters())

    err_msg_bad = "Could not access table 'doesnt_exist' in data warehouse"
    bad_task = DataWarehouseValidationTask(query_and_params_callable=bad_query, error_message=err_msg_bad)

    tasks.append(bad_task)
    issues = dw_validator.run_tasks(tasks=tasks)
    assert len(issues.all_issues) == 1
    assert len(issues.errors) == 1
    assert err_msg_bad in issues.errors[0].message


def test_validate_data_sources(  # noqa: D
    data_warehouse_validation_model: UserConfiguredModel,
    create_data_warehouse_validation_model_tables: bool,
    sql_client: SqlClient,
    mf_test_session_state: MetricFlowTestSessionState,
) -> None:
    model = deepcopy(data_warehouse_validation_model)

    dw_validator = DataWarehouseModelValidator(
        sql_client=sql_client, system_schema=mf_test_session_state.mf_system_schema
    )

    issues = dw_validator.validate_data_sources(model)
    assert len(issues.all_issues) == 0

    model.data_sources.append(
        data_source_with_guaranteed_meta(
            name="test_data_source2",
            sql_table="doesnt.exist",
            dimensions=[],
            mutability=Mutability(type=MutabilityType.IMMUTABLE),
        )
    )

    issues = dw_validator.validate_data_sources(model)
    assert len(issues.all_issues) == 1
    assert len(issues.errors) == 1
    assert "Unable to access data source `test_data_source2`" in issues.all_issues[0].message


def test_build_dimension_tasks(  # noqa: D
    data_warehouse_validation_model: UserConfiguredModel,
    sql_client: SqlClient,
    mf_test_session_state: MetricFlowTestSessionState,
) -> None:
    tasks = DataWarehouseTaskBuilder.gen_dimension_tasks(
        model=data_warehouse_validation_model,
        sql_client=sql_client,
        system_schema=mf_test_session_state.mf_system_schema,
    )
    assert len(tasks) == 1  # on data source query with all dimensions
    assert len(tasks[0].on_fail_subtasks) == 2  # a sub task for each dimension on the data source


def test_validate_dimensions(  # noqa: D
    data_warehouse_validation_model: UserConfiguredModel,
    sql_client: SqlClient,
    mf_test_session_state: MetricFlowTestSessionState,
) -> None:
    model = deepcopy(data_warehouse_validation_model)

    dw_validator = DataWarehouseModelValidator(
        sql_client=sql_client, system_schema=mf_test_session_state.mf_system_schema
    )

    issues = dw_validator.validate_dimensions(model)
    assert len(issues.all_issues) == 0

    dimensions = list(model.data_sources[0].dimensions)
    dimensions.append(Dimension(name="doesnt_exist", type=DimensionType.CATEGORICAL))
    model.data_sources[0].dimensions = dimensions

    issues = dw_validator.validate_dimensions(model)
    # One isssue is created for the short circuit query failure, and another is
    # created for the sub task checking the specific dimension
    assert len(issues.all_issues) == 2


def test_build_identifiers_tasks(  # noqa: D
    data_warehouse_validation_model: UserConfiguredModel,
    sql_client: SqlClient,
    mf_test_session_state: MetricFlowTestSessionState,
) -> None:
    tasks = DataWarehouseTaskBuilder.gen_identifier_tasks(
        model=data_warehouse_validation_model,
        sql_client=sql_client,
        system_schema=mf_test_session_state.mf_system_schema,
    )
    assert len(tasks) == 1  # on data source query with all identifiers
    assert len(tasks[0].on_fail_subtasks) == 1  # a sub task for each identifier on the data source


def test_validate_identifiers(  # noqa: D
    data_warehouse_validation_model: UserConfiguredModel,
    sql_client: SqlClient,
    mf_test_session_state: MetricFlowTestSessionState,
) -> None:
    model = deepcopy(data_warehouse_validation_model)

    dw_validator = DataWarehouseModelValidator(
        sql_client=sql_client, system_schema=mf_test_session_state.mf_system_schema
    )

    issues = dw_validator.validate_identifiers(model)
    assert len(issues.all_issues) == 0

    identifiers = list(model.data_sources[0].identifiers)
    identifiers.append(Identifier(name="doesnt_exist", type=IdentifierType.UNIQUE))
    model.data_sources[0].identifiers = identifiers

    issues = dw_validator.validate_identifiers(model)
    # One isssue is created for the short circuit query failure, and another is
    # created for the sub task checking the specific identifier
    assert len(issues.all_issues) == 2


def test_build_measure_tasks(  # noqa: D
    data_warehouse_validation_model: UserConfiguredModel,
    sql_client: SqlClient,
    mf_test_session_state: MetricFlowTestSessionState,
) -> None:
    tasks = DataWarehouseTaskBuilder.gen_measure_tasks(
        model=data_warehouse_validation_model,
        sql_client=sql_client,
        system_schema=mf_test_session_state.mf_system_schema,
    )
    assert len(tasks) == 1  # on data source query with all measures
    assert len(tasks[0].on_fail_subtasks) == 1  # a sub task for each measure on the data source


def test_validate_measures(  # noqa: D
    data_warehouse_validation_model: UserConfiguredModel,
    sql_client: SqlClient,
    mf_test_session_state: MetricFlowTestSessionState,
) -> None:
    model = deepcopy(data_warehouse_validation_model)

    dw_validator = DataWarehouseModelValidator(
        sql_client=sql_client, system_schema=mf_test_session_state.mf_system_schema
    )

    issues = dw_validator.validate_measures(model)
    assert len(issues.all_issues) == 0

    measures = list(model.data_sources[0].measures)
    measures.append(Measure(name="doesnt_exist", agg=AggregationType.SUM, agg_time_dimension="ds"))
    model.data_sources[0].measures = measures

    issues = dw_validator.validate_measures(model)
    # One isssue is created for the short circuit query failure, and another is
    # created for the sub task checking the specific measure
    assert len(issues.all_issues) == 2


def test_build_metric_tasks(  # noqa: D
    request: FixtureRequest,
    data_warehouse_validation_model: UserConfiguredModel,
    sql_client: SqlClient,
    mf_test_session_state: MetricFlowTestSessionState,
) -> None:
    tasks = DataWarehouseTaskBuilder.gen_metric_tasks(
        model=data_warehouse_validation_model,
        system_schema=mf_test_session_state.mf_system_schema,
        sql_client=sql_client,
    )
    assert len(tasks) == 1
    (query_string, _params) = tasks[0].query_and_params_callable()
    assert_snapshot_text_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        group_id="data_warehouse_validation_model",
        snapshot_id="query0",
        snapshot_text=query_string,
        snapshot_file_extension=".sql",
        incomparable_strings_replacement_function=make_schema_replacement_function(
            system_schema=mf_test_session_state.mf_system_schema, source_schema=mf_test_session_state.mf_source_schema
        ),
        additional_sub_directories_for_snapshots=(sql_client.__class__.__name__,),
    )


def test_validate_metrics(  # noqa: D
    data_warehouse_validation_model: UserConfiguredModel,
    sql_client: SqlClient,
    mf_test_session_state: MetricFlowTestSessionState,
) -> None:
    model = deepcopy(data_warehouse_validation_model)
    dw_validator = DataWarehouseModelValidator(
        sql_client=sql_client, system_schema=mf_test_session_state.mf_system_schema
    )

    issues = dw_validator.validate_metrics(model)
    assert len(issues.all_issues) == 0

    # Update model to have a new measure which creates a new metric by proxy
    new_measures = list(model.data_sources[0].measures)
    new_measures.append(
        Measure(
            name="count_cats",
            agg=AggregationType.SUM,
            expr="is_cat",  # doesn't exist as column
            create_metric=True,
        )
    )
    model.data_sources[0].measures = new_measures
    model.metrics = []
    model = ModelTransformer.pre_validation_transform_model(model)
    model = ModelTransformer.post_validation_transform_model(model)

    # Validate new metric created by proxy causes an issue (because the column used doesn't exist)
    dw_validator = DataWarehouseModelValidator(
        sql_client=sql_client, system_schema=mf_test_session_state.mf_system_schema
    )
    issues = dw_validator.validate_metrics(model)
    assert len(issues.all_issues) == 1
    assert len(issues.errors) == 1
    assert "Unable to query metric `count_cats`" in issues.errors[0].message
