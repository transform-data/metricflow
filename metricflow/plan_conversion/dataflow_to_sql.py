from __future__ import annotations

import logging
from collections import OrderedDict
from dataclasses import dataclass
from typing import TypeVar, Generic, Union, List, Sequence, Optional

from metricflow.column_assoc import ColumnAssociation, SingleColumnCorrelationKey
from metricflow.constraints.time_constraint import TimeRangeConstraint
from metricflow.dag.id_generation import IdGeneratorRegistry
from metricflow.dataflow.dataflow_plan import (
    DataflowPlanNodeVisitor,
    FilterElementsNode,
    WriteToResultDataframeNode,
    OrderByLimitNode,
    ComputeMetricsNode,
    AggregateMeasuresNode,
    JoinAggregatedMeasuresByGroupByColumnsNode,
    JoinToBaseOutputNode,
    ReadSqlSourceNode,
    BaseOutput,
    ComputedMetricsOutput,
    WhereConstraintNode,
    CombineMetricsNode,
    SourceDataSetT,
    ConstrainTimeRangeNode,
    WriteToResultTableNode,
    JoinOverTimeRangeNode,
    MetricTimeDimensionTransformNode,
)
from metricflow.dataset.dataset import DataSet
from metricflow.instances import (
    AggregationState,
    InstanceSet,
    MetricInstance,
    MetricModelReference,
    TimeDimensionInstance,
)
from metricflow.model.objects.metric import CumulativeMetricWindow, MetricType
from metricflow.model.semantic_model import SemanticModel
from metricflow.object_utils import assert_values_exhausted
from metricflow.plan_conversion.instance_converters import (
    AliasAggregatedMeasures,
    RemoveMeasures,
    AddMetrics,
    CreateSelectColumnsForInstances,
    CreateSelectColumnsWithMeasuresAggregated,
    create_select_columns_for_instance_sets,
    AddLinkToLinkableElements,
    FilterElements,
    ChangeAssociatedColumns,
    ChangeMeasureAggregationState,
    FilterLinkableInstancesWithLeadingLink,
)
from metricflow.plan_conversion.select_column_gen import (
    SelectColumnSet,
)
from metricflow.plan_conversion.spec_transforms import (
    CreateOnConditionForCombiningMetrics,
    CreateSelectCoalescedColumnsForLinkableSpecs,
    SelectOnlyLinkableSpecs,
)
from metricflow.plan_conversion.sql_dataset import SqlDataSet
from metricflow.plan_conversion.time_spine import TimeSpineSource
from metricflow.protocols.sql_client import SqlEngineAttributes, SupportedSqlEngine
from metricflow.specs import (
    ColumnAssociationResolver,
    MetricSpec,
    TimeDimensionSpec,
    MeasureSpec,
)
from metricflow.sql.optimizer.optimization_levels import (
    SqlQueryOptimizationLevel,
    SqlQueryOptimizerConfiguration,
)
from metricflow.sql.sql_exprs import (
    SqlExpressionNode,
    SqlComparisonExpression,
    SqlColumnReferenceExpression,
    SqlColumnReference,
    SqlComparison,
    SqlIsNullExpression,
    SqlLogicalExpression,
    SqlLogicalOperator,
    SqlStringExpression,
    SqlCastToTimestampExpression,
    SqlTimeDeltaExpression,
    SqlRatioComputationExpression,
    SqlDateTruncExpression,
    SqlStringLiteralExpression,
)
from metricflow.sql.sql_plan import (
    SqlQueryPlan,
    SqlSelectStatementNode,
    SqlJoinDescription,
    SqlSelectColumn,
    SqlOrderByDescription,
    SqlJoinType,
    SqlQueryPlanNode,
    SqlTableFromClauseNode,
)
from metricflow.time.time_constants import ISO8601_PYTHON_FORMAT
from metricflow.time.time_granularity import TimeGranularity

logger = logging.getLogger(__name__)


# The type of data set that present at a source node.
SqlDataSetT = TypeVar("SqlDataSetT", bound=SqlDataSet)


@dataclass(frozen=True)
class ColumnEqualityDescription:
    """Helper class to enumerate columns that should be equal between sources in a join."""

    left_column_alias: str
    right_column_alias: str


def make_equijoin_description(
    right_data_set: SqlDataSet,
    left_source_alias: str,
    right_source_alias: str,
    column_equality_descriptions: Sequence[ColumnEqualityDescription],
) -> SqlJoinDescription:
    """Make a join description where the condition is an equals between two columns in the left and right sources.

    A source is defined as either a table or a subquery.
    """
    assert len(column_equality_descriptions) > 0

    and_conditions = []
    for column_equality_description in column_equality_descriptions:
        and_conditions.append(
            SqlComparisonExpression(
                left_expr=SqlColumnReferenceExpression(
                    SqlColumnReference(
                        table_alias=left_source_alias,
                        column_name=column_equality_description.left_column_alias,
                    )
                ),
                comparison=SqlComparison.EQUALS,
                right_expr=SqlColumnReferenceExpression(
                    SqlColumnReference(
                        table_alias=right_source_alias,
                        column_name=column_equality_description.right_column_alias,
                    )
                ),
            )
        )
    on_condition: SqlExpressionNode
    if len(and_conditions) == 1:
        on_condition = and_conditions[0]
    else:
        on_condition = SqlLogicalExpression(operator=SqlLogicalOperator.AND, args=tuple(and_conditions))

    return SqlJoinDescription(
        right_source=right_data_set.sql_select_node,
        right_source_alias=right_source_alias,
        on_condition=on_condition,
        join_type=SqlJoinType.LEFT_OUTER,
    )


def _make_aggregate_measures_join_condition(
    column_aliases: Sequence[str],
    left_source_alias: str,
    right_source_alias: str,
) -> SqlLogicalExpression:
    """Make the ON condition particular to the aggregate measures join scenario

    In most cases equality will suffice, but in the event of NULL values on both sides we include
    those in the join. This is questionable, so maybe we shouldn't do it.
    """

    join_conditions = []
    for column_alias in column_aliases:
        left_column = SqlColumnReferenceExpression(
            SqlColumnReference(table_alias=left_source_alias, column_name=column_alias)
        )
        right_column = SqlColumnReferenceExpression(
            SqlColumnReference(table_alias=right_source_alias, column_name=column_alias)
        )
        equality_condition = SqlComparisonExpression(
            left_expr=left_column, right_expr=right_column, comparison=SqlComparison.EQUALS
        )

        left_is_null = SqlIsNullExpression(arg=left_column)
        right_is_null = SqlIsNullExpression(arg=right_column)
        both_null_condition = SqlLogicalExpression(operator=SqlLogicalOperator.AND, args=(left_is_null, right_is_null))

        join_conditions.append(
            SqlLogicalExpression(operator=SqlLogicalOperator.OR, args=(equality_condition, both_null_condition))
        )

    return SqlLogicalExpression(
        operator=SqlLogicalOperator.AND,
        args=tuple(join_conditions),
    )


def _make_cumulative_metric_join_condition(
    from_data_set_alias: str,
    from_data_set_identifier_col: str,
    right_data_set_alias: str,
    right_data_set_identifier_col: str,
    window: Optional[CumulativeMetricWindow],
) -> SqlExpressionNode:
    """Creates SqlLogicalExpression representing a cumulative metric self-join condition"""
    if window is None:  # accumulate over all time
        return SqlComparisonExpression(
            left_expr=SqlColumnReferenceExpression(
                SqlColumnReference(
                    table_alias=from_data_set_alias,
                    column_name=from_data_set_identifier_col,
                )
            ),
            comparison=SqlComparison.LESS_THAN_OR_EQUALS,
            right_expr=SqlColumnReferenceExpression(
                SqlColumnReference(
                    table_alias=right_data_set_alias,
                    column_name=right_data_set_identifier_col,
                )
            ),
        )

    # ds <= <date> and  ds > <date> - <window>
    return SqlLogicalExpression(
        operator=SqlLogicalOperator.AND,
        args=(
            SqlComparisonExpression(
                left_expr=SqlColumnReferenceExpression(
                    SqlColumnReference(
                        table_alias=from_data_set_alias,
                        column_name=from_data_set_identifier_col,
                    )
                ),
                comparison=SqlComparison.LESS_THAN_OR_EQUALS,
                right_expr=SqlColumnReferenceExpression(
                    SqlColumnReference(
                        table_alias=right_data_set_alias,
                        column_name=right_data_set_identifier_col,
                    )
                ),
            ),
            SqlComparisonExpression(
                left_expr=SqlColumnReferenceExpression(
                    SqlColumnReference(
                        table_alias=from_data_set_alias,
                        column_name=from_data_set_identifier_col,
                    )
                ),
                comparison=SqlComparison.GREATER_THAN,
                right_expr=SqlTimeDeltaExpression(
                    SqlColumnReferenceExpression(
                        SqlColumnReference(
                            table_alias=right_data_set_alias,
                            column_name=right_data_set_identifier_col,
                        )
                    ),
                    count=window.count,
                    granularity=window.granularity,
                    grain_to_date=None,
                ),
            ),
        ),
    )


def _make_grain_to_date_cumulative_metric_join_condition(
    from_data_set_alias: str,
    from_data_set_identifier_col: str,
    right_data_set_alias: str,
    right_data_set_identifier_col: str,
    grain_to_date: TimeGranularity,
) -> SqlLogicalExpression:
    """Creates SqlLogicalExpression representing a grain_to_date cumulative metric self-join condition"""
    return SqlLogicalExpression(
        operator=SqlLogicalOperator.AND,
        args=(
            SqlComparisonExpression(
                left_expr=SqlColumnReferenceExpression(
                    SqlColumnReference(
                        table_alias=from_data_set_alias,
                        column_name=from_data_set_identifier_col,
                    )
                ),
                comparison=SqlComparison.LESS_THAN_OR_EQUALS,
                right_expr=SqlColumnReferenceExpression(
                    SqlColumnReference(
                        table_alias=right_data_set_alias,
                        column_name=right_data_set_identifier_col,
                    )
                ),
            ),
            SqlComparisonExpression(
                left_expr=SqlColumnReferenceExpression(
                    SqlColumnReference(
                        table_alias=from_data_set_alias,
                        column_name=from_data_set_identifier_col,
                    )
                ),
                comparison=SqlComparison.GREATER_THAN_OR_EQUALS,
                right_expr=SqlTimeDeltaExpression(
                    SqlColumnReferenceExpression(
                        SqlColumnReference(
                            table_alias=right_data_set_alias,
                            column_name=right_data_set_identifier_col,
                        )
                    ),
                    count=0,
                    granularity=grain_to_date,
                    grain_to_date=grain_to_date,
                ),
            ),
        ),
    )


def _make_time_range_comparison_expr(
    table_alias: str, column_alias: str, time_range_constraint: TimeRangeConstraint
) -> SqlExpressionNode:
    """Build an expression like "ds >= CAST('2020-01-01' AS TIMESTAMP) AND ds <= CAST('2020-01-02' AS TIMESTAMP)"""
    return SqlLogicalExpression(
        operator=SqlLogicalOperator.AND,
        args=(
            SqlComparisonExpression(
                left_expr=SqlColumnReferenceExpression(
                    SqlColumnReference(
                        table_alias=table_alias,
                        column_name=column_alias,
                    )
                ),
                comparison=SqlComparison.GREATER_THAN_OR_EQUALS,
                right_expr=SqlCastToTimestampExpression(
                    # TODO: Update when adding < day granularity support.
                    arg=SqlStringLiteralExpression(
                        literal_value=time_range_constraint.start_time.strftime(ISO8601_PYTHON_FORMAT),
                    )
                ),
            ),
            SqlComparisonExpression(
                left_expr=SqlColumnReferenceExpression(
                    SqlColumnReference(
                        table_alias=table_alias,
                        column_name=column_alias,
                    )
                ),
                comparison=SqlComparison.LESS_THAN_OR_EQUALS,
                right_expr=SqlCastToTimestampExpression(
                    # TODO: Update when adding < day granularity support.
                    arg=SqlStringLiteralExpression(
                        literal_value=time_range_constraint.end_time.strftime(ISO8601_PYTHON_FORMAT),
                    )
                ),
            ),
        ),
    )


def _make_time_spine_data_set(
    metric_time_dimension_instance: TimeDimensionInstance,
    metric_time_dimension_column_name: str,
    time_spine_source: TimeSpineSource,
    time_spine_table_alias: str,
    time_range_constraint: Optional[TimeRangeConstraint] = None,
) -> SqlDataSet:
    """Make a time spine data set, which contains all date values like '2020-01-01', '2020-01-02'...

    This is useful in computing cumulative metrics. This will need to be updated to support granularities finer than a
    day.
    """
    time_spine_instance = (
        TimeDimensionInstance(
            defined_from=metric_time_dimension_instance.defined_from,
            associated_columns=(
                ColumnAssociation(
                    column_name=metric_time_dimension_column_name,
                    column_correlation_key=SingleColumnCorrelationKey(),
                ),
            ),
            spec=metric_time_dimension_instance.spec,
        ),
    )
    time_spine_instance_set = InstanceSet(
        time_dimension_instances=time_spine_instance,
    )
    description = "Date Spine"

    # If the requested granularity is the same as the granularity of the spine, do a direct select.
    if metric_time_dimension_instance.spec.time_granularity == time_spine_source.time_column_granularity:
        return SqlDataSet(
            instance_set=time_spine_instance_set,
            sql_select_node=SqlSelectStatementNode(
                description=description,
                # This creates select expressions for all columns referenced in the instance set.
                select_columns=(
                    SqlSelectColumn(
                        expr=SqlColumnReferenceExpression(
                            SqlColumnReference(
                                table_alias=time_spine_table_alias,
                                column_name=time_spine_source.time_column_name,
                            ),
                        ),
                        column_alias=metric_time_dimension_column_name,
                    ),
                ),
                from_source=SqlTableFromClauseNode(sql_table=time_spine_source.spine_table),
                from_source_alias=time_spine_table_alias,
                joins_descs=(),
                group_bys=(),
                where=_make_time_range_comparison_expr(
                    table_alias=time_spine_table_alias,
                    column_alias=time_spine_source.time_column_name,
                    time_range_constraint=time_range_constraint,
                )
                if time_range_constraint
                else None,
                order_bys=(),
            ),
        )
    # If the granularity is different, apply a DATE_TRUNC() and aggregate.
    else:
        select_columns = (
            SqlSelectColumn(
                expr=SqlDateTruncExpression(
                    time_granularity=metric_time_dimension_instance.spec.time_granularity,
                    arg=SqlColumnReferenceExpression(
                        SqlColumnReference(
                            table_alias=time_spine_table_alias,
                            column_name=time_spine_source.time_column_name,
                        ),
                    ),
                ),
                column_alias=metric_time_dimension_column_name,
            ),
        )
        return SqlDataSet(
            instance_set=time_spine_instance_set,
            sql_select_node=SqlSelectStatementNode(
                description=description,
                # This creates select expressions for all columns referenced in the instance set.
                select_columns=select_columns,
                from_source=SqlTableFromClauseNode(sql_table=time_spine_source.spine_table),
                from_source_alias=time_spine_table_alias,
                joins_descs=(),
                group_bys=select_columns,
                where=_make_time_range_comparison_expr(
                    table_alias=time_spine_table_alias,
                    column_alias=time_spine_source.time_column_name,
                    time_range_constraint=time_range_constraint,
                )
                if time_range_constraint
                else None,
                order_bys=(),
            ),
        )


class DataflowToSqlQueryPlanConverter(Generic[SqlDataSetT], DataflowPlanNodeVisitor[SqlDataSetT, SqlDataSet]):
    """Generates an SQL query plan from a node in the a metric dataflow plan."""

    def __init__(
        self,
        column_association_resolver: ColumnAssociationResolver,
        semantic_model: SemanticModel,
        time_spine_source: TimeSpineSource,
    ) -> None:
        """Constructor.

        Args:
            column_association_resolver: controls how columns for instances are generated and used between nested
            queries.
            semantic_model: Self-explanatory.
            time_spine_source: Allows getting dates for use in cumulative joins
        """
        self._column_association_resolver = column_association_resolver
        self._metric_semantics = semantic_model.metric_semantics
        self._data_source_semantics = semantic_model.data_source_semantics
        self._time_spine_source = time_spine_source

    @property
    def column_association_resolver(self) -> ColumnAssociationResolver:  # noqa: D
        return self._column_association_resolver

    def _next_unique_table_alias(self) -> str:
        """Return the next unique table alias to use in generating queries."""
        return IdGeneratorRegistry.for_class(self.__class__).create_id(prefix="subq")

    def visit_source_node(self, node: ReadSqlSourceNode[SqlDataSetT]) -> SqlDataSet:
        """Generate the SQL to read from the source."""
        return SqlDataSet(
            sql_select_node=node.data_set.sql_select_node,
            instance_set=node.data_set.instance_set,
        )

    def visit_join_over_time_range_node(self, node: JoinOverTimeRangeNode[SqlDataSetT]) -> SqlDataSet:
        """Generate time range join SQL"""
        table_alias_to_instance_set: OrderedDict[str, InstanceSet] = OrderedDict()

        input_data_set = node.parent_node.accept(self)
        input_data_set_alias = self._next_unique_table_alias()

        sql_join_descs: List[SqlJoinDescription] = []

        metric_time_dimension_spec: Optional[TimeDimensionSpec] = None
        metric_time_dimension_instance: Optional[TimeDimensionInstance] = None
        for instance in input_data_set.instance_set.time_dimension_instances:
            if (
                instance.spec.element_name == node.metric_time_dimension_reference.element_name
                and len(instance.spec.identifier_links) == 0
            ):
                metric_time_dimension_instance = instance
                metric_time_dimension_spec = instance.spec
                break

        # If the metric time dimension isn't present in the parent node it's because it wasn't requested
        # and therefore we don't need the time range join because we can just let the metric sum over all time
        if metric_time_dimension_spec is None:
            return input_data_set

        input_data_set_identifier_column_association = input_data_set.column_association_for_time_dimension(
            metric_time_dimension_spec
        )
        input_data_set_identifier_col = input_data_set_identifier_column_association.column_name

        time_spine_data_set_alias = self._next_unique_table_alias()

        metric_time_dimension_column_name = self.column_association_resolver.resolve_time_dimension_spec(
            metric_time_dimension_spec
        ).column_name

        # assemble dataset with metric_time_dimension to join
        assert metric_time_dimension_instance
        time_spine_data_set = _make_time_spine_data_set(
            metric_time_dimension_instance=metric_time_dimension_instance,
            metric_time_dimension_column_name=metric_time_dimension_column_name,
            time_spine_source=self._time_spine_source,
            time_spine_table_alias=self._next_unique_table_alias(),
            time_range_constraint=node.time_range_constraint,
        )
        table_alias_to_instance_set[time_spine_data_set_alias] = time_spine_data_set.instance_set

        # Figure out which columns in the "right" data set correspond to the time dimension that we want to join on.
        time_spine_data_set_column_associations = time_spine_data_set.column_association_for_time_dimension(
            metric_time_dimension_spec
        )
        time_spine_data_set_time_dimension_col = time_spine_data_set_column_associations.column_name

        # Build an expression like "a.ds <= b.ds AND a.ds >= b.ds - <window>
        # If no window is present we join across all time -> "a.ds <= b.ds"
        constrain_metric_time_column_condition_both: Optional[SqlExpressionNode] = None
        if node.window is not None:
            constrain_metric_time_column_condition_both = _make_cumulative_metric_join_condition(
                input_data_set_alias,
                input_data_set_identifier_col,
                time_spine_data_set_alias,
                time_spine_data_set_time_dimension_col,
                node.window,
            )
        elif node.grain_to_date is not None:
            constrain_metric_time_column_condition_both = _make_grain_to_date_cumulative_metric_join_condition(
                input_data_set_alias,
                input_data_set_identifier_col,
                time_spine_data_set_alias,
                time_spine_data_set_time_dimension_col,
                node.grain_to_date,
            )
        elif node.window is None:  # `window is None` for clarity (could be else since we have window and !window)
            constrain_metric_time_column_condition_both = _make_cumulative_metric_join_condition(
                input_data_set_alias,
                input_data_set_identifier_col,
                time_spine_data_set_alias,
                time_spine_data_set_time_dimension_col,
                None,
            )

        sql_join_descs.append(
            SqlJoinDescription(
                right_source=input_data_set.sql_select_node,
                right_source_alias=input_data_set_alias,
                on_condition=constrain_metric_time_column_condition_both,
                join_type=SqlJoinType.INNER,
            )
        )

        modified_input_instance_set = InstanceSet(
            measure_instances=input_data_set.instance_set.measure_instances,
            dimension_instances=input_data_set.instance_set.dimension_instances,
            identifier_instances=input_data_set.instance_set.identifier_instances,
            metric_instances=input_data_set.instance_set.metric_instances,
            # we omit the metric time dimension from the right side of the self-join because we need to use
            # the metric time dimension from the right side
            time_dimension_instances=tuple(
                [
                    time_dimension_instance
                    for time_dimension_instance in input_data_set.instance_set.time_dimension_instances
                    if time_dimension_instance.spec != metric_time_dimension_spec
                ]
            ),
        )
        table_alias_to_instance_set[input_data_set_alias] = modified_input_instance_set

        # The output instances are the same as the input instances.
        output_instance_set = ChangeAssociatedColumns(self._column_association_resolver).transform(
            input_data_set.instance_set
        )

        return SqlDataSet(
            instance_set=output_instance_set,
            sql_select_node=SqlSelectStatementNode(
                description=node.description,
                select_columns=create_select_columns_for_instance_sets(
                    self._column_association_resolver, table_alias_to_instance_set
                ),
                from_source=time_spine_data_set.sql_select_node,
                from_source_alias=time_spine_data_set_alias,
                joins_descs=tuple(sql_join_descs),
                group_bys=(),
                where=None,
                order_bys=(),
            ),
        )

    def visit_join_to_base_output_node(self, node: JoinToBaseOutputNode[SqlDataSetT]) -> SqlDataSet:
        """Generates the query that realizes the behavior of the JoinToStandardOutputNode."""

        # Keep a mapping between the table aliases that would be used in the query and the MDO instances in that source.
        # e.g. when building "FROM from_table a JOIN right_table b", the value for key "a" would be the instances in
        # "from_table"
        table_alias_to_instance_set: OrderedDict[str, InstanceSet] = OrderedDict()

        # Convert the dataflow from the left node to a DataSet and add context for it to table_alias_to_instance_set
        # A DataSet is a bundle of the SQL query (in object form) and the MDO instances that the SQL query contains.
        from_data_set = node.left_node.accept(self)
        from_data_set_alias = self._next_unique_table_alias()
        table_alias_to_instance_set[from_data_set_alias] = from_data_set.instance_set

        # Build the join descriptions for the SqlQueryPlan - different from node.join_descriptions which are the join
        # descriptions from the dataflow plan.
        sql_join_descs: List[SqlJoinDescription] = []

        # The dataflow plan describes how the data sets coming from the parent nodes should be joined together. Use
        # those descriptions to convert them to join descriptions for the SQL query plan.
        for join_description in node.join_targets:
            join_on_identifier = join_description.join_on_identifier

            # Figure out which columns in the "from" data set correspond to the identifier that we want to join on.
            # The column associations tell us which columns correspond to which instances in the data set.
            from_data_set_identifier_column_associations = from_data_set.column_associations_for_identifier(
                join_on_identifier
            )
            from_data_set_identifier_cols = [c.column_name for c in from_data_set_identifier_column_associations]

            right_node_to_join: BaseOutput = join_description.join_node
            right_data_set: SqlDataSet = right_node_to_join.accept(self)
            right_data_set_alias = self._next_unique_table_alias()

            # Figure out which columns in the "right" data set correspond to the identifier that we want to join on.
            right_data_set_column_associations = right_data_set.column_associations_for_identifier(join_on_identifier)
            right_data_set_identifier_cols = [c.column_name for c in right_data_set_column_associations]

            assert len(from_data_set_identifier_cols) == len(
                right_data_set_identifier_cols
            ), f"Cannot construct join - the number of columns on the left ({from_data_set_identifier_cols}) side of the join does not match the right ({right_data_set_identifier_cols})"

            # We have the columns that we need to "join on" in the query, so add it to the list of join descriptions to
            # use later.
            column_equality_descriptions = []
            for idx in range(len(from_data_set_identifier_cols)):
                column_equality_descriptions.append(
                    ColumnEqualityDescription(
                        left_column_alias=from_data_set_identifier_cols[idx],
                        right_column_alias=right_data_set_identifier_cols[idx],
                    )
                )
            # Add the partition columns as well.
            for dimension_join_description in join_description.join_on_partition_dimensions:
                column_equality_descriptions.append(
                    ColumnEqualityDescription(
                        left_column_alias=from_data_set.column_association_for_dimension(
                            dimension_join_description.start_node_dimension_spec
                        ).column_name,
                        right_column_alias=right_data_set.column_association_for_dimension(
                            dimension_join_description.node_to_join_dimension_spec
                        ).column_name,
                    )
                )

            for time_dimension_join_description in join_description.join_on_partition_time_dimensions:
                column_equality_descriptions.append(
                    ColumnEqualityDescription(
                        left_column_alias=from_data_set.column_association_for_time_dimension(
                            time_dimension_join_description.start_node_time_dimension_spec
                        ).column_name,
                        right_column_alias=right_data_set.column_association_for_time_dimension(
                            time_dimension_join_description.node_to_join_time_dimension_spec
                        ).column_name,
                    )
                )

            sql_join_descs.append(
                make_equijoin_description(
                    right_data_set=right_data_set,
                    left_source_alias=from_data_set_alias,
                    right_source_alias=right_data_set_alias,
                    column_equality_descriptions=column_equality_descriptions,
                )
            )

            # Remove the linkable instances with the join_on_identifier as the leading link as the next step adds the
            # link. This is to avoid cases where there is a primary identifier and a dimension in the data set, and we
            # create an instance in the next step that has the same identifier link.
            # e.g. a data set has the dimension "listing__country_latest" and "listing" is a primary identifier in the
            # data set. The next step would create an instance like "listing__listing__country_latest" without this
            # filter.

            # logger.error(f"before filter is:\n{pformat_big_objects(right_data_set.instance_set.spec_set)}")
            right_data_set_instance_set_filtered = FilterLinkableInstancesWithLeadingLink(
                identifier_link=join_on_identifier,
            ).transform(right_data_set.instance_set)
            # logger.error(f"after filter is:\n{pformat_big_objects(right_data_set_instance_set_filtered.spec_set)}")

            # After the right data set is joined to the "from" data set, we need to change the links for some of the
            # instances that represent the right data set. For example, if the "from" data set contains the "bookings"
            # measure instance and the right dataset contains the "country" dimension instance, then after the join,
            # the output data set should have the "country" dimension instance with the "user_id" identifier link
            # (if "user_id" equality was the join condition). "country" -> "user_id__country"
            right_data_set_instance_set_after_join = right_data_set_instance_set_filtered.transform(
                AddLinkToLinkableElements(join_on_identifier=join_on_identifier)
            )
            table_alias_to_instance_set[right_data_set_alias] = right_data_set_instance_set_after_join

        from_data_set_output_instance_set = from_data_set.instance_set.transform(
            FilterElements(include_specs=from_data_set.instance_set.spec_set.all_specs)
        )

        # Change the aggregation state for the measures to be partially aggregated if it was previously aggregated
        # since we removed the identifiers and added the dimensions. The dimensions could have the same value for
        # multiple rows, so we'll need to re-aggregate.
        from_data_set_output_instance_set = from_data_set_output_instance_set.transform(
            ChangeMeasureAggregationState(
                {
                    AggregationState.NON_AGGREGATED: AggregationState.NON_AGGREGATED,
                    AggregationState.COMPLETE: AggregationState.PARTIAL,
                    AggregationState.PARTIAL: AggregationState.PARTIAL,
                }
            )
        )

        table_alias_to_instance_set[from_data_set_alias] = from_data_set_output_instance_set

        # Construct the data set that contains the updated instances and the SQL nodes that should go in the various
        # clauses.
        return SqlDataSet(
            instance_set=InstanceSet.merge(list(table_alias_to_instance_set.values())),
            sql_select_node=SqlSelectStatementNode(
                description=node.description,
                select_columns=create_select_columns_for_instance_sets(
                    self._column_association_resolver, table_alias_to_instance_set
                ),
                from_source=from_data_set.sql_select_node,
                from_source_alias=from_data_set_alias,
                joins_descs=tuple(sql_join_descs),
                group_bys=(),
                where=None,
                order_bys=(),
            ),
        )

    def visit_join_aggregated_measures_by_groupby_columns_node(  # noqa: D
        self, node: JoinAggregatedMeasuresByGroupByColumnsNode
    ) -> SqlDataSet:
        """Generates the query that realizes the behavior of the JoinAggregatedMeasuresByGroupByColumnsNode.

        This node is a straight inner join against all of the columns used for grouping in the input
        aggregation steps. Every column should be used, and at this point all inputs are fully aggregated,
        meaning we can make assumptions about things like NULL handling and there being one row per value
        set in each data source.

        In addition, this is used in cases where we expect a final metric to be computed using these
        measures as input. Therefore, we make the assumption that any mismatch should be discarded, as
        the behavior of the metric will be undefined in that case. This is why the INNER JOIN type is
        appropriate - if a dimension value set exists in one aggregated set but not the other, there is
        no sensible metric value for that dimension set.
        """
        assert len(node.parent_nodes) > 1, "This cannot happen, the node initializer would have failed"

        table_alias_to_instance_set: OrderedDict[str, InstanceSet] = OrderedDict()

        from_data_set: SqlDataSet = node.parent_nodes[0].accept(self)
        from_data_set_alias = self._next_unique_table_alias()
        table_alias_to_instance_set[from_data_set_alias] = from_data_set.instance_set
        join_aliases = [column.column_name for column in from_data_set.groupable_column_associations]

        sql_join_descs: List[SqlJoinDescription] = []
        for aggregated_node in node.parent_nodes[1:]:
            right_data_set: SqlDataSet = aggregated_node.accept(self)
            right_data_set_alias = self._next_unique_table_alias()
            right_column_names = {column.column_name for column in right_data_set.groupable_column_associations}
            if right_column_names != set(join_aliases):
                # TODO test multi-hop dimensions and address any issues. For now, let's raise an exception.
                raise ValueError(
                    f"We only support joins where all dimensions have the same name, but we got {right_column_names} "
                    f"and {join_aliases}, which differ by {right_column_names.difference(set(join_aliases))}!"
                )
            sql_join_descs.append(
                SqlJoinDescription(
                    right_source=right_data_set.sql_select_node,
                    right_source_alias=right_data_set_alias,
                    on_condition=_make_aggregate_measures_join_condition(
                        column_aliases=join_aliases,
                        left_source_alias=from_data_set_alias,
                        right_source_alias=right_data_set_alias,
                    ),
                    join_type=SqlJoinType.INNER,
                )
            )
            # All groupby columns are shared by all inputs, so we only want the measure/metric columns
            # from the data sources on the right side of the join
            table_alias_to_instance_set[right_data_set_alias] = InstanceSet(
                measure_instances=right_data_set.instance_set.measure_instances,
                metric_instances=right_data_set.instance_set.metric_instances,
            )

        return SqlDataSet(
            instance_set=InstanceSet.merge(list(table_alias_to_instance_set.values())),
            sql_select_node=SqlSelectStatementNode(
                description=node.description,
                select_columns=create_select_columns_for_instance_sets(
                    self._column_association_resolver, table_alias_to_instance_set
                ),
                from_source=from_data_set.sql_select_node,
                from_source_alias=from_data_set_alias,
                joins_descs=tuple(sql_join_descs),
                group_bys=(),
                where=None,
                order_bys=(),
            ),
        )

    def visit_aggregate_measures_node(self, node: AggregateMeasuresNode) -> SqlDataSet:
        """Generates the query that realizes the behavior of AggregateMeasuresNode.

        This will produce a query that aggregates all measures from a given input data source per the
        measure spec

        In the event the input aggregations are applied to measures with aliases set, in case of, e.g.,
        a constraint applied to one instance of the measure but not another one, this method will
        apply the rename in the select statement for this node, and propagate that further along via an
        instance set transform to rename the measures.

        Any node operating on the output of this node will need to use the measure aliases instead of
        the measure names as references.

        """
        # Get the data from the parent, and change measure instances to the aggregated state.
        from_data_set: SqlDataSet = node.parent_node.accept(self)
        aggregated_instance_set = from_data_set.instance_set.transform(
            ChangeMeasureAggregationState(
                {
                    AggregationState.NON_AGGREGATED: AggregationState.COMPLETE,
                    AggregationState.COMPLETE: AggregationState.COMPLETE,
                    AggregationState.PARTIAL: AggregationState.COMPLETE,
                }
            )
        )
        # Also, the columns should always follow the resolver format.
        aggregated_instance_set = aggregated_instance_set.transform(
            ChangeAssociatedColumns(self._column_association_resolver)
        )

        from_data_set_alias = self._next_unique_table_alias()

        # Convert the instance set into a set of select column statements with updated aliases
        # Note any measure with an alias requirement will be recast at this point, and
        # downstream consumers of the resulting node must therefore request aggregated measures
        # by their appropriate aliases
        select_column_set: SelectColumnSet = aggregated_instance_set.transform(
            CreateSelectColumnsWithMeasuresAggregated(
                table_alias=from_data_set_alias,
                column_resolver=self._column_association_resolver,
                data_source_semantics=self._data_source_semantics,
                metric_input_measure_specs=node.metric_input_measure_specs,
            )
        )

        if any((spec.alias for spec in node.metric_input_measure_specs)):
            # This is a little silly, but we need to update the column instance set with the new aliases
            # There are a number of refactoring options - simplest is to consolidate this with
            # ChangeMeasureAggregationState, assuming there are no ordering dependencies up above
            aggregated_instance_set = aggregated_instance_set.transform(
                AliasAggregatedMeasures(metric_input_measure_specs=node.metric_input_measure_specs)
            )
            # and make sure we follow the resolver format for any newly aliased measures....
            aggregated_instance_set = aggregated_instance_set.transform(
                ChangeAssociatedColumns(self._column_association_resolver)
            )

        return SqlDataSet(
            instance_set=aggregated_instance_set,
            sql_select_node=SqlSelectStatementNode(
                description=node.description,
                # This will generate expressions with the appropriate aggregation functions e.g. SUM()
                select_columns=select_column_set.as_tuple(),
                from_source=from_data_set.sql_select_node,
                from_source_alias=from_data_set_alias,
                joins_descs=(),
                # This will generate expressions to group by the columns that don't correspond to a measure instance.
                group_bys=select_column_set.without_measure_columns().as_tuple(),
                where=None,
                order_bys=(),
            ),
        )

    def visit_compute_metrics_node(self, node: ComputeMetricsNode) -> SqlDataSet:
        """Generates the query that realizes the behavior of ComputeMetricsNode."""
        from_data_set: SqlDataSet = node.parent_node.accept(self)
        from_data_set_alias = self._next_unique_table_alias()

        # TODO: Check that all measures for the metrics are in the input instance set
        # The desired output instance set has no measures, so create a copy with those removed.
        output_instance_set: InstanceSet = from_data_set.instance_set.transform(RemoveMeasures())

        # Also, the output columns should always follow the resolver format.
        output_instance_set = output_instance_set.transform(ChangeAssociatedColumns(self._column_association_resolver))

        non_metric_select_column_set: SelectColumnSet = output_instance_set.transform(
            CreateSelectColumnsForInstances(
                table_alias=from_data_set_alias,
                column_resolver=self._column_association_resolver,
            )
        )

        # Add select columns that would compute the metrics to the select columns.
        metric_select_columns = []
        metric_instances = []
        for metric_spec in node.metric_specs:
            metric = self._metric_semantics.get_metric(metric_spec)

            metric_expr: Optional[SqlExpressionNode] = None
            if metric.type is MetricType.RATIO:
                numerator = metric.type_params.numerator
                denominator = metric.type_params.denominator
                assert (
                    numerator is not None and denominator is not None
                ), "Missing numerator or denominator for ratio metric, this should have been caught in validation!"
                numerator_column_name = self._column_association_resolver.resolve_measure_spec(
                    MeasureSpec(element_name=numerator.post_aggregation_measure_reference.element_name)
                ).column_name
                denominator_column_name = self._column_association_resolver.resolve_measure_spec(
                    MeasureSpec(element_name=denominator.post_aggregation_measure_reference.element_name)
                ).column_name

                metric_expr = SqlRatioComputationExpression(
                    numerator=SqlColumnReferenceExpression(
                        SqlColumnReference(
                            table_alias=from_data_set_alias,
                            column_name=numerator_column_name,
                        )
                    ),
                    denominator=SqlColumnReferenceExpression(
                        SqlColumnReference(
                            table_alias=from_data_set_alias,
                            column_name=denominator_column_name,
                        )
                    ),
                )
            elif metric.type is MetricType.MEASURE_PROXY:
                if len(metric.input_measures) > 0:
                    assert (
                        len(metric.input_measures) == 1
                    ), "Measure proxy metrics should always source from exactly 1 measure."
                    expr = self._column_association_resolver.resolve_measure_spec(
                        MeasureSpec(
                            element_name=metric.input_measures[0].post_aggregation_measure_reference.element_name
                        )
                    ).column_name
                else:
                    expr = metric.name
                # Use a column reference to improve query optimization.
                metric_expr = SqlColumnReferenceExpression(
                    SqlColumnReference(
                        table_alias=from_data_set_alias,
                        column_name=expr,
                    )
                )
            elif metric.type is MetricType.CUMULATIVE:
                assert (
                    len(metric.measure_references) == 1
                ), "Cumulative metrics should always source from exactly 1 measure."
                expr = self._column_association_resolver.resolve_measure_spec(
                    MeasureSpec(element_name=metric.input_measures[0].post_aggregation_measure_reference.element_name)
                ).column_name
                metric_expr = SqlColumnReferenceExpression(
                    SqlColumnReference(
                        table_alias=from_data_set_alias,
                        column_name=expr,
                    )
                )
            elif metric.type == MetricType.EXPR:
                assert metric.type_params.expr
                metric_expr = SqlStringExpression(sql_expr=metric.type_params.expr)
            else:
                assert_values_exhausted(metric.type)

            assert metric_expr

            output_column_association = self._column_association_resolver.resolve_metric_spec(metric_spec)
            metric_select_columns.append(
                SqlSelectColumn(
                    expr=metric_expr,
                    column_alias=output_column_association.column_name,
                )
            )
            metric_instances.append(
                MetricInstance(
                    associated_columns=(output_column_association,),
                    defined_from=[MetricModelReference(metric_name=metric_spec.element_name)],
                    spec=metric_spec,
                )
            )
        output_instance_set = output_instance_set.transform(AddMetrics(metric_instances))

        return SqlDataSet(
            instance_set=output_instance_set,
            sql_select_node=SqlSelectStatementNode(
                description=node.description,
                select_columns=tuple(metric_select_columns) + non_metric_select_column_set.as_tuple(),
                from_source=from_data_set.sql_select_node,
                from_source_alias=from_data_set_alias,
                joins_descs=(),
                group_bys=(),
                where=None,
                order_bys=(),
            ),
        )

    def visit_order_by_limit_node(self, node: OrderByLimitNode) -> SqlDataSet:  # noqa: D
        from_data_set: SqlDataSet = node.parent_node.accept(self)
        output_instance_set = from_data_set.instance_set
        from_data_set_alias = self._next_unique_table_alias()

        # Also, the output columns should always follow the resolver format.
        output_instance_set = output_instance_set.transform(ChangeAssociatedColumns(self._column_association_resolver))

        order_by_descriptions = []
        for order_by_spec in node.order_by_specs:
            column_associations = order_by_spec.item.column_associations(self._column_association_resolver)
            for column_association in column_associations:
                order_by_descriptions.append(
                    SqlOrderByDescription(
                        expr=SqlColumnReferenceExpression(
                            col_ref=SqlColumnReference(
                                table_alias=from_data_set_alias, column_name=column_association.column_name
                            )
                        ),
                        desc=order_by_spec.descending,
                    )
                )

        return SqlDataSet(
            instance_set=output_instance_set,
            sql_select_node=SqlSelectStatementNode(
                description=node.description,
                # This creates select expressions for all columns referenced in the instance set.
                select_columns=output_instance_set.transform(
                    CreateSelectColumnsForInstances(from_data_set_alias, self._column_association_resolver)
                ).as_tuple(),
                from_source=from_data_set.sql_select_node,
                from_source_alias=from_data_set_alias,
                joins_descs=(),
                group_bys=(),
                where=None,
                order_bys=tuple(order_by_descriptions),
                limit=node.limit,
            ),
        )

    def visit_write_to_result_dataframe_node(self, node: WriteToResultDataframeNode) -> SqlDataSet:
        """This is an operation that can't be represented as an SQL query.

        Instead, it should be handled in the execution plan as an operation that runs an SQL query and saves it to
        a dataframe.
        """
        raise RuntimeError("This node type is not supported.")

    def visit_write_to_result_table_node(self, node: WriteToResultTableNode[SourceDataSetT]) -> SqlDataSet:
        """Similar to visit_write_to_result_dataframe_node()."""
        raise RuntimeError("This node type is not supported.")

    def visit_pass_elements_filter_node(self, node: FilterElementsNode) -> SqlDataSet:
        """Generates the query that realizes the behavior of FilterElementsNode."""
        from_data_set: SqlDataSet = node.parent_node.accept(self)
        output_instance_set = from_data_set.instance_set.transform(FilterElements(node.include_specs))
        from_data_set_alias = self._next_unique_table_alias()

        # Also, the output columns should always follow the resolver format.
        output_instance_set = output_instance_set.transform(ChangeAssociatedColumns(self._column_association_resolver))

        return SqlDataSet(
            instance_set=output_instance_set,
            sql_select_node=SqlSelectStatementNode(
                description=node.description,
                # This creates select expressions for all columns referenced in the instance set.
                select_columns=output_instance_set.transform(
                    CreateSelectColumnsForInstances(from_data_set_alias, self._column_association_resolver)
                ).as_tuple(),
                from_source=from_data_set.sql_select_node,
                from_source_alias=from_data_set_alias,
                joins_descs=(),
                group_bys=(),
                where=None,
                order_bys=(),
            ),
        )

    def visit_where_constraint_node(self, node: WhereConstraintNode) -> SqlDataSet:
        """Adds where clause to SQL statement from parent node"""
        from_data_set: SqlDataSet = node.parent_node.accept(self)
        output_instance_set = from_data_set.instance_set
        from_data_set_alias = self._next_unique_table_alias()

        return SqlDataSet(
            instance_set=output_instance_set,
            sql_select_node=SqlSelectStatementNode(
                description=node.description,
                # This creates select expressions for all columns referenced in the instance set.
                select_columns=output_instance_set.transform(
                    CreateSelectColumnsForInstances(from_data_set_alias, self._column_association_resolver)
                ).as_tuple(),
                from_source=from_data_set.sql_select_node,
                from_source_alias=from_data_set_alias,
                joins_descs=(),
                group_bys=(),
                where=SqlStringExpression(
                    sql_expr=node.where.where_condition,
                    used_columns=tuple(node.where.linkable_names),
                    execution_parameters=node.where.execution_parameters,
                ),
                order_bys=(),
            ),
        )

    def _make_select_columns_for_metrics(
        self, table_alias_to_metric_specs: OrderedDict[str, Sequence[MetricSpec]]
    ) -> List[SqlSelectColumn]:
        """Creates select columns that get the given metric using the given table alias.

        e.g.

        with table_alias_to_metric_specs = {"a": MetricSpec(element_name="bookings")}

        ->

        a.bookings AS bookings
        """
        select_columns = []
        for table_alias, metric_specs in table_alias_to_metric_specs.items():
            for metric_spec in metric_specs:
                metric_column_name = self._column_association_resolver.resolve_metric_spec(metric_spec).column_name
                select_columns.append(
                    SqlSelectColumn(
                        expr=SqlColumnReferenceExpression(
                            col_ref=SqlColumnReference(
                                table_alias=table_alias,
                                column_name=metric_column_name,
                            )
                        ),
                        column_alias=metric_column_name,
                    )
                )
        return select_columns

    def visit_combine_metrics_node(self, node: CombineMetricsNode[SqlDataSetT]) -> SqlDataSet:  # noqa: D
        # Sanity check that all parents have the same linkable specs.
        parent_data_sets = [x.accept(self) for x in node.parent_nodes]
        assert (
            len(parent_data_sets) > 1
        ), "Shouldn't have a CombineMetricsNode in the dataflow plan if there's only 1 parent."
        linkable_specs = parent_data_sets[0].instance_set.spec_set.linkable_specs
        assert all(
            [set(x.instance_set.spec_set.linkable_specs) == set(linkable_specs) for x in parent_data_sets]
        ), "All parent nodes should have the same set of linkable instances since all values are coalesced."
        linkable_spec_set = parent_data_sets[0].instance_set.spec_set.transform(SelectOnlyLinkableSpecs())

        # Create a FULL OUTER join where the join key is all previous dimension values from all sources coalesced.
        #
        # e.g.
        #
        # FROM (
        #   ...
        # ) subq_9
        # FULL OUTER JOIN (
        #   ...
        # ) subq_10
        # ON
        #   subq_9.is_instant = subq_10.is_instant
        #   AND subq_9.ds = subq_10.ds
        # FULL OUTER JOIN (
        #   ...
        # ) subq_11
        # ON
        #   COALESCE(subq_9.is_instant, subq_10.is_instant) = subq_11.is_instant
        #   AND COALESCE(subq_9.ds, subq_10.ds) = subq_11.ds

        joins_descriptions = []
        parent_source_table_aliases = []
        table_alias_to_metric_specs: OrderedDict[str, Sequence[MetricSpec]] = OrderedDict()
        for parent_data_set in parent_data_sets:
            table_alias = self._next_unique_table_alias()
            parent_source_table_aliases.append(table_alias)
            table_alias_to_metric_specs[table_alias] = parent_data_set.instance_set.spec_set.metric_specs

        # Create the components of the FULL OUTER JOIN that combines metrics. Order doesn't
        # matter so we use the first element in the FROM clause and create join descriptions from
        # the rest.
        from_alias = parent_source_table_aliases[0]
        from_source = parent_data_sets[0].sql_select_node
        for i in range(1, len(parent_source_table_aliases)):
            joins_descriptions.append(
                SqlJoinDescription(
                    right_source=parent_data_sets[i].sql_select_node,
                    right_source_alias=parent_source_table_aliases[i],
                    on_condition=CreateOnConditionForCombiningMetrics(
                        column_association_resolver=self._column_association_resolver,
                        table_aliases_in_coalesce=parent_source_table_aliases[:i],
                        table_alias_on_right_equality=parent_source_table_aliases[i],
                    ).transform(linkable_spec_set),
                    join_type=SqlJoinType.FULL_OUTER,
                )
            )

        # We can merge all parent instances since the common linkable instances will be de-duped.
        output_instance_set = InstanceSet.merge([x.instance_set for x in parent_data_sets])
        output_instance_set = output_instance_set.transform(ChangeAssociatedColumns(self._column_association_resolver))

        metric_select_columns = self._make_select_columns_for_metrics(table_alias_to_metric_specs)
        linkable_select_columns = linkable_spec_set.transform(
            CreateSelectCoalescedColumnsForLinkableSpecs(
                column_association_resolver=self._column_association_resolver,
                table_aliases=parent_source_table_aliases,
            )
        )

        return SqlDataSet(
            instance_set=output_instance_set,
            sql_select_node=SqlSelectStatementNode(
                description=node.description,
                select_columns=tuple(metric_select_columns) + tuple(linkable_select_columns),
                from_source=from_source,
                from_source_alias=from_alias,
                joins_descs=tuple(joins_descriptions),
                group_bys=(),
                where=None,
                order_bys=(),
            ),
        )

    def visit_constrain_time_range_node(self, node: ConstrainTimeRangeNode[SourceDataSetT]) -> SqlDataSet:  # noqa: D
        from_data_set: SqlDataSet = node.parent_node.accept(self)
        from_data_set_alias = self._next_unique_table_alias()

        time_dimension_instances_for_metric_time = [
            time_dimension_instance
            for time_dimension_instance in from_data_set.instance_set.time_dimension_instances
            if time_dimension_instance.spec.element_name == DataSet.metric_time_dimension_name()
        ]

        # Use the smallest time granularity to build the comparison since that's what was used in the data source
        # definition and it wouldn't have a DATE_TRUNC() in the expression. We want to build
        #
        # ds >= '2020-01-01' AND ds <= '2020-02-01'
        #
        # not DATE_TRUNC('month', ds) >= '2020-01-01' AND DATE_TRUNC('month', ds <= '2020-02-01')
        def sort_function_for_time_granularity(instance: TimeDimensionInstance) -> int:
            return instance.spec.time_granularity.to_int()

        time_dimension_instances_for_metric_time.sort(key=sort_function_for_time_granularity)
        assert (
            len(time_dimension_instances_for_metric_time) > 0
        ), "No metric time dimensions found in the input data set for this node"

        time_dimension_instance_for_metric_time = time_dimension_instances_for_metric_time[0]

        # Build an expression like "ds >= CAST('2020-01-01' AS TIMESTAMP) AND ds <= CAST('2020-01-02' AS TIMESTAMP)"
        constrain_metric_time_column_condition = _make_time_range_comparison_expr(
            table_alias=from_data_set_alias,
            column_alias=time_dimension_instance_for_metric_time.associated_column.column_name,
            time_range_constraint=node.time_range_constraint,
        )

        output_instance_set = from_data_set.instance_set
        # Output columns should always follow the resolver format.
        output_instance_set = output_instance_set.transform(ChangeAssociatedColumns(self._column_association_resolver))

        return SqlDataSet(
            instance_set=output_instance_set,
            sql_select_node=SqlSelectStatementNode(
                description=node.description,
                # This creates select expressions for all columns referenced in the instance set.
                select_columns=output_instance_set.transform(
                    CreateSelectColumnsForInstances(from_data_set_alias, self._column_association_resolver)
                ).as_tuple(),
                from_source=from_data_set.sql_select_node,
                from_source_alias=from_data_set_alias,
                joins_descs=(),
                group_bys=(),
                where=constrain_metric_time_column_condition,
                order_bys=(),
            ),
        )

    def convert_to_sql_query_plan(
        self,
        sql_engine_attributes: SqlEngineAttributes,
        sql_query_plan_id: str,
        dataflow_plan_node: Union[BaseOutput, ComputedMetricsOutput],
        optimization_level: SqlQueryOptimizationLevel = SqlQueryOptimizationLevel.O4,
    ) -> SqlQueryPlan:
        """Create an SQL query plan that represents the computation up to the given dataflow plan node."""

        sql_select_node: SqlQueryPlanNode = dataflow_plan_node.accept(self).sql_select_node

        # TODO: Make this a more generally accessible attribute instead of checking against the
        # BigQuery-ness of the engine
        use_column_alias_in_group_by = sql_engine_attributes.sql_engine_type is SupportedSqlEngine.BIGQUERY

        for optimizer in SqlQueryOptimizerConfiguration.optimizers_for_level(
            optimization_level, use_column_alias_in_group_by=use_column_alias_in_group_by
        ):
            logger.info(f"Applying optimizer: {optimizer.__class__.__name__}")
            sql_select_node = optimizer.optimize(sql_select_node)

        return SqlQueryPlan(plan_id=sql_query_plan_id, render_node=sql_select_node)

    def visit_metric_time_dimension_transform_node(
        self, node: MetricTimeDimensionTransformNode[SqlDataSetT]
    ) -> SqlDataSet:
        """Implement the behavior of the MetricTimeDimensionTransformNode.

        This node will create an output data set that is similar to the input data set, but the measure instances it
        contains is a subset of the input data set. Only measure instances that have an aggregation time dimension
        matching the one defined in the node will be passed. In addition, an additional time dimension instance for
        "metric time" will be included. See DataSet.metric_time_dimension_reference().
        """
        input_data_set: SqlDataSet = node.parent_node.accept(self)

        # Find which measures have an aggregation time dimension that is the same as the one specified in the node.
        # Only these measures will be in the output data set.
        output_measure_instances = []
        for measure_instance in input_data_set.instance_set.measure_instances:
            measure = self._data_source_semantics.get_measure(measure_instance.spec.as_reference)
            if measure.checked_agg_time_dimension == node.aggregation_time_dimension_reference:
                output_measure_instances.append(measure_instance)

        if len(output_measure_instances) == 0:
            raise RuntimeError(
                f"No measure instances in the input source match the aggregation time dimension "
                f"{node.aggregation_time_dimension_reference}. Check if the dataflow plan was constructed correctly."
            )

        # Find time dimension instances that refer to the same dimension as the one specified in the node.
        matching_time_dimension_instances = []
        for time_dimension_instance in input_data_set.instance_set.time_dimension_instances:
            # The specification for the time dimension to use for aggregation is the local one.
            if (
                len(time_dimension_instance.spec.identifier_links) == 0
                and time_dimension_instance.spec.reference == node.aggregation_time_dimension_reference
            ):
                matching_time_dimension_instances.append(time_dimension_instance)

        output_time_dimension_instances: List[TimeDimensionInstance] = []
        output_time_dimension_instances.extend(input_data_set.instance_set.time_dimension_instances)
        output_column_to_input_column: OrderedDict[str, str] = OrderedDict()

        # For those matching time dimension instances, create the analog metric time dimension instances for the output.
        for matching_time_dimension_instance in matching_time_dimension_instances:
            metric_time_dimension_spec = DataSet.metric_time_dimension_spec(
                matching_time_dimension_instance.spec.time_granularity
            )
            metric_time_dimension_column_association = self._column_association_resolver.resolve_time_dimension_spec(
                metric_time_dimension_spec
            )
            output_time_dimension_instances.append(
                TimeDimensionInstance(
                    defined_from=matching_time_dimension_instance.defined_from,
                    associated_columns=(
                        self._column_association_resolver.resolve_time_dimension_spec(metric_time_dimension_spec),
                    ),
                    spec=metric_time_dimension_spec,
                )
            )
            output_column_to_input_column[
                metric_time_dimension_column_association.column_name
            ] = matching_time_dimension_instance.associated_column.column_name
        output_instance_set = InstanceSet(
            measure_instances=tuple(output_measure_instances),
            dimension_instances=input_data_set.instance_set.dimension_instances,
            time_dimension_instances=tuple(output_time_dimension_instances),
            identifier_instances=input_data_set.instance_set.identifier_instances,
            metric_instances=input_data_set.instance_set.metric_instances,
        )
        output_instance_set = ChangeAssociatedColumns(self._column_association_resolver).transform(output_instance_set)

        from_data_set_alias = self._next_unique_table_alias()

        return SqlDataSet(
            instance_set=output_instance_set,
            sql_select_node=SqlSelectStatementNode(
                description=node.description,
                # This creates select expressions for all columns referenced in the instance set.
                select_columns=CreateSelectColumnsForInstances(
                    column_resolver=self._column_association_resolver,
                    table_alias=from_data_set_alias,
                    output_to_input_column_mapping=output_column_to_input_column,
                )
                .transform(output_instance_set)
                .as_tuple(),
                from_source=input_data_set.sql_select_node,
                from_source_alias=from_data_set_alias,
                joins_descs=(),
                group_bys=(),
                where=None,
                order_bys=(),
            ),
        )
