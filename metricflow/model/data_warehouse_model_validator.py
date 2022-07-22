from __future__ import annotations
from copy import deepcopy

from dataclasses import dataclass, field
from functools import partial
from math import floor
from time import perf_counter
from typing import Callable, List, Optional, Tuple
from metricflow.dataflow.builder.source_node import SourceNodeBuilder
from metricflow.dataflow.dataflow_plan import BaseOutput, FilterElementsNode
from metricflow.dataset.convert_data_source import DataSourceToDataSetConverter
from metricflow.dataset.data_source_adapter import DataSourceDataSet

from metricflow.dataset.dataset import DataSet
from metricflow.engine.metricflow_engine import MetricFlowEngine, MetricFlowExplainResult, MetricFlowQueryRequest
from metricflow.instances import DataSourceElementReference, DataSourceReference, MetricModelReference
from metricflow.model.objects.data_source import DataSource
from metricflow.model.objects.elements.dimension import Dimension, DimensionType
from metricflow.model.objects.metric import Metric
from metricflow.model.objects.user_configured_model import UserConfiguredModel
from metricflow.model.semantic_model import SemanticModel
from metricflow.model.validations.validator_helpers import (
    DataSourceContext,
    DataSourceElementContext,
    DataSourceElementType,
    FileContext,
    MetricContext,
    ModelValidationResults,
    ValidationContext,
    ValidationError,
    ValidationIssue,
    ValidationWarning,
)
from metricflow.plan_conversion.column_resolver import DefaultColumnAssociationResolver
from metricflow.plan_conversion.dataflow_to_sql import DataflowToSqlQueryPlanConverter
from metricflow.plan_conversion.time_spine import TimeSpineSource
from metricflow.protocols.sql_client import SqlClient
from metricflow.specs import DimensionSpec, TimeDimensionSpec
from metricflow.sql.sql_bind_parameters import SqlBindParameters


@dataclass
class QueryRenderingTools:
    """General tools that data warehosue validations use for rendering the validation queries"""

    semantic_model: SemanticModel
    source_node_builder: SourceNodeBuilder
    converter: DataSourceToDataSetConverter
    time_spine_source: TimeSpineSource
    plan_converter: DataflowToSqlQueryPlanConverter

    def __init__(self, model: UserConfiguredModel, system_schema: str) -> None:  # noqa: D
        self.semantic_model = SemanticModel(user_configured_model=model)
        self.source_node_builder = SourceNodeBuilder(semantic_model=self.semantic_model)
        self.time_spine_source = TimeSpineSource(schema_name=system_schema)
        self.converter = DataSourceToDataSetConverter(
            column_association_resolver=DefaultColumnAssociationResolver(semantic_model=self.semantic_model)
        )
        self.plan_converter = DataflowToSqlQueryPlanConverter(
            column_association_resolver=DefaultColumnAssociationResolver(self.semantic_model),
            semantic_model=self.semantic_model,
            time_spine_source=self.time_spine_source,
        )


@dataclass
class DataWarehouseValidationTask:
    """Dataclass for defining a task to be used in the DataWarehouseModelValidator"""

    query_and_params_callable: Callable[[], Tuple[str, SqlBindParameters]]
    error_message: str
    context: Optional[ValidationContext] = None
    on_fail_subtasks: List[DataWarehouseValidationTask] = field(default_factory=lambda: [])


class DataWarehouseTaskBuilder:
    """Task builder for standard data warehouse validation tasks"""

    @staticmethod
    def _data_source_node(render_tools: QueryRenderingTools, data_source: DataSource) -> BaseOutput[DataSourceDataSet]:
        """Builds and returns the DataSourceDataSet node for the given data source"""
        data_source_semantics = render_tools.semantic_model.data_source_semantics.get_by_reference(
            DataSourceReference(data_source_name=data_source.name)
        )
        assert data_source_semantics

        source_nodes = render_tools.source_node_builder.create_from_data_sets(
            (render_tools.converter.create_sql_source_data_set(data_source_semantics),)
        )

        assert len(source_nodes) == 1
        return source_nodes[0]

    @staticmethod
    def renderize(
        sql_client: SqlClient, plan_converter: DataflowToSqlQueryPlanConverter, plan_id: str, nodes: FilterElementsNode
    ):
        """Generates a sql query plan and returns the rendered sql and execution_parameters"""
        sql_plan = plan_converter.convert_to_sql_query_plan(
            sql_engine_attributes=sql_client.sql_engine_attributes,
            sql_query_plan_id=plan_id,
            dataflow_plan_node=nodes,
        )

        rendered_plan = sql_client.sql_engine_attributes.sql_query_plan_renderer.render_sql_query_plan(sql_plan)
        return (rendered_plan.sql, rendered_plan.execution_parameters)

    @classmethod
    def gen_data_source_tasks(
        cls, model: UserConfiguredModel, sql_client: SqlClient, system_schema: str
    ) -> List[DataWarehouseValidationTask]:
        """Generates a list of tasks for validating the data sources of the model"""

        # we need a dimension to query that we know exists (i.e. the dimension
        # is guaranteed to not cause a problem) on each data source.
        # Additionally, we don't want to modify the original model, so we
        # first make a deep copy of it
        model = deepcopy(model)
        for data_source in model.data_sources:
            data_source.dimensions = list(data_source.dimensions) + [
                Dimension(name=f"validation_dim_for_{data_source.name}", type=DimensionType.CATEGORICAL, expr="1")
            ]

        render_tools = QueryRenderingTools(model=model, system_schema=system_schema)

        tasks: List[DataWarehouseValidationTask] = []
        for data_source in model.data_sources:
            source_node = cls._data_source_node(render_tools=render_tools, data_source=data_source)
            spec = DimensionSpec.from_name(name=f"validation_dim_for_{data_source.name}")
            filter_elements_node = FilterElementsNode(parent_node=source_node, include_specs=[spec])

            tasks.append(
                DataWarehouseValidationTask(
                    query_and_params_callable=partial(
                        cls.renderize,
                        sql_client=sql_client,
                        plan_converter=render_tools.plan_converter,
                        plan_id=f"{data_source.name}_validation",
                        nodes=filter_elements_node,
                    ),
                    context=DataSourceContext(
                        file_context=FileContext.from_metadata(metadata=data_source.metadata),
                        data_source=DataSourceReference(data_source_name=data_source.name),
                    ),
                    error_message=f"Unable to access data source `{data_source.name}` in data warehouse",
                )
            )

        return tasks

    @classmethod
    def gen_dimension_tasks(
        cls, model: UserConfiguredModel, sql_client: SqlClient, system_schema: str
    ) -> List[DataWarehouseValidationTask]:
        """Generates a list of tasks for validating the dimensions of the model

        The high level tasks returned are "short cut" queries which try to
        query all the dimensions for a given data source. If that query fails,
        one or more of the dimensions is incorrectly specified. Thus if the
        query fails, there are subtasks which query the individual dimensions
        on the data source to identify which have issues.
        """

        render_tools = QueryRenderingTools(model=model, system_schema=system_schema)

        tasks: List[DataWarehouseValidationTask] = []
        for data_source in model.data_sources:
            if len(data_source.dimensions) > 0:
                source_node = cls._data_source_node(render_tools=render_tools, data_source=data_source)

                data_source_sub_tasks: List[DataWarehouseValidationTask] = []
                data_source_specs: List[DimensionSpec] = []
                for dimension in data_source.dimensions:
                    if dimension.type == DimensionType.TIME:
                        spec = TimeDimensionSpec.from_name(name=dimension.name)
                    else:
                        spec = DimensionSpec.from_name(name=dimension.name)
                    data_source_specs.append(spec)
                    filter_elements_node = FilterElementsNode(parent_node=source_node, include_specs=[spec])
                    data_source_sub_tasks.append(
                        DataWarehouseValidationTask(
                            query_and_params_callable=partial(
                                cls.renderize,
                                sql_client=sql_client,
                                plan_converter=render_tools.plan_converter,
                                plan_id=f"{data_source.name}_dim_{dimension.name}_validation",
                                nodes=filter_elements_node,
                            ),
                            context=DataSourceElementContext(
                                file_context=FileContext.from_metadata(metadata=data_source.metadata),
                                data_source_element=DataSourceElementReference(
                                    data_source_name=data_source.name, element_name=dimension.name
                                ),
                                element_type=DataSourceElementType.DIMENSION,
                            ),
                            error_message=f"Unable to query dimension `{dimension.name}` on data source `{data_source.name}` in data warehouse",
                        )
                    )

                filter_elements_node = FilterElementsNode(parent_node=source_node, include_specs=data_source_specs)
                tasks.append(
                    DataWarehouseValidationTask(
                        query_and_params_callable=partial(
                            cls.renderize,
                            sql_client=sql_client,
                            plan_converter=render_tools.plan_converter,
                            plan_id=f"{data_source.name}_all_dimensions_validation",
                            nodes=filter_elements_node,
                        ),
                        context=DataSourceContext(
                            file_context=FileContext.from_metadata(metadata=data_source.metadata),
                            data_source=DataSourceReference(data_source_name=data_source.name),
                        ),
                        error_message=f"Failed to query dimensions in data warehouse for data source `{data_source.name}`",
                        on_fail_subtasks=data_source_sub_tasks,
                    )
                )
        return tasks

    @staticmethod
    def _gen_metric_task_query_and_params(
        metric: Metric, mf_engine: MetricFlowEngine
    ) -> Tuple[str, SqlBindParameters]:  # noqa: D
        mf_query = MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=[metric.name], group_by_names=[DataSet.metric_time_dimension_name()]
        )
        explain_result: MetricFlowExplainResult = mf_engine.explain(mf_request=mf_query)
        return (explain_result.rendered_sql.sql_query, explain_result.rendered_sql.bind_parameters)

    @classmethod
    def gen_metric_tasks(
        cls, model: UserConfiguredModel, sql_client: SqlClient, system_schema: str
    ) -> List[DataWarehouseValidationTask]:
        """Generates a list of tasks for validating the metrics of the model"""
        mf_engine = MetricFlowEngine(
            semantic_model=SemanticModel(user_configured_model=model),
            sql_client=sql_client,
            system_schema=system_schema,
        )
        tasks: List[DataWarehouseValidationTask] = []
        for metric in model.metrics:
            tasks.append(
                DataWarehouseValidationTask(
                    query_and_params_callable=partial(
                        cls._gen_metric_task_query_and_params,
                        metric=metric,
                        mf_engine=mf_engine,
                    ),
                    context=MetricContext(
                        file_context=FileContext.from_metadata(metadata=metric.metadata),
                        metric=MetricModelReference(metric_name=metric.name),
                    ),
                    error_message=f"Unable to query metric `{metric.name}`.",
                )
            )
        return tasks


class DataWarehouseModelValidator:
    """A Validator for checking specific tasks for the model against the Data Warehouse

    Data Warehouse Validations are validations that are done against the data
    warehouse based on the model configured by the user. Their purpose is to
    ensure that queries generated by MetricFlow won't fail when you go to use
    them (assuming the model has passed these validations before use).
    """

    def __init__(self, sql_client: SqlClient, system_schema: str) -> None:  # noqa: D
        self._sql_client = sql_client
        self._sql_schema = system_schema

    def run_tasks(
        self, tasks: List[DataWarehouseValidationTask], timeout: Optional[int] = None
    ) -> ModelValidationResults:
        """Runs the list of tasks as queries agains the data warehouse, returning any found issues

        Args:
            tasks: A list of tasks to run against the data warehouse
            timeout: An optional timeout. Default is None. When the timeout is hit, function will return early.

        Returns:
            A list of validation issues discovered when running the passed in tasks against the data warehosue
        """

        # Used for keeping track if we go past the max time
        start_time = perf_counter()

        issues: List[ValidationIssue] = []
        # TODO: Asyncio implementation
        for index, task in enumerate(tasks):
            if timeout is not None and perf_counter() - start_time > timeout:
                issues.append(
                    ValidationWarning(
                        context=None,
                        message=f"Hit timeout before completing all tasks. Completed {index}/{len(tasks)} tasks.",
                    )
                )
                break
            try:
                (query_string, query_params) = task.query_and_params_callable()
                self._sql_client.dry_run(stmt=query_string, sql_bind_parameters=query_params)
            except Exception as e:
                issues.append(
                    ValidationError(
                        context=task.context,
                        message=task.error_message + f"\nRecieved following error from data warehouse:\n{e}",
                    )
                )
                if task.on_fail_subtasks:
                    sub_task_timeout = floor(timeout - (perf_counter() - start_time)) if timeout else None
                    issues += self.run_tasks(tasks=task.on_fail_subtasks, timeout=sub_task_timeout).all_issues

        return ModelValidationResults.from_issues_sequence(issues)

    def validate_data_sources(
        self, model: UserConfiguredModel, timeout: Optional[int] = None
    ) -> ModelValidationResults:
        """Generates a list of tasks for validating the data sources of the model and then runs them

        Args:
            model: Model which to run data warehouse validations on
            timeout: An optional timeout. Default is None. When the timeout is hit, function will return early.

        Returns:
            A list of validation issues discovered when running the passed in tasks against the data warehosue
        """
        tasks = DataWarehouseTaskBuilder.gen_data_source_tasks(
            model=model, sql_client=self._sql_client, system_schema=self._sql_schema
        )
        return self.run_tasks(tasks=tasks, timeout=timeout)

    def validate_dimensions(self, model: UserConfiguredModel, timeout: Optional[int] = None) -> ModelValidationResults:
        """Generates a list of tasks for validating the dimensions of the model and then runs them

        Args:
            model: Model which to run data warehouse validations on
            timeout: An optional timeout. Default is None. When the timeout is hit, function will return early.

        Returns:
            A list of validation issues. If there are no validation issues, an empty list is returned.
        """
        tasks = DataWarehouseTaskBuilder.gen_dimension_tasks(
            model=model, sql_client=self._sql_client, system_schema=self._sql_schema
        )
        return self.run_tasks(tasks=tasks, timeout=timeout)

    def validate_metrics(self, model: UserConfiguredModel, timeout: Optional[int] = None) -> ModelValidationResults:
        """Generates a list of tasks for validating the metrics of the model and then runs them

        Args:
            model: Model which to run data warehouse validations on
            timeout: An optional timeout. Default is None. When the timeout is hit, function will return early.

        Returns:
            A list of validation issues. If there are no validation issues, an empty list is returned.
        """

        tasks = DataWarehouseTaskBuilder.gen_metric_tasks(
            model=model, sql_client=self._sql_client, system_schema=self._sql_schema
        )
        return self.run_tasks(tasks=tasks, timeout=timeout)
