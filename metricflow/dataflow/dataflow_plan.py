"""Nodes for building a dataflow plan."""

from __future__ import annotations

import textwrap
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, TypeVar, Generic, Optional, Sequence, Tuple, Union

import jinja2

from metricflow.constraints.time_constraint import TimeRangeConstraint
from metricflow.dag.id_generation import (
    DATAFLOW_NODE_AGGREGATE_MEASURES_ID_PREFIX,
    DATAFLOW_NODE_COMPUTE_METRICS_ID_PREFIX,
    DATAFLOW_NODE_JOIN_AGGREGATED_MEASURES_BY_GROUPBY_COLUMNS_PREFIX,
    DATAFLOW_NODE_JOIN_SELF_OVER_TIME_RANGE_ID_PREFIX,
    DATAFLOW_NODE_JOIN_TO_STANDARD_OUTPUT_ID_PREFIX,
    DATAFLOW_NODE_ORDER_BY_LIMIT_ID_PREFIX,
    DATAFLOW_NODE_PASS_FILTER_ELEMENTS_ID_PREFIX,
    DATAFLOW_NODE_READ_SQL_SOURCE_ID_PREFIX,
    DATAFLOW_NODE_WHERE_CONSTRAINT_ID_PREFIX,
    DATAFLOW_NODE_WRITE_TO_RESULT_DATAFRAME_ID_PREFIX,
    DATAFLOW_NODE_COMBINE_METRICS_ID_PREFIX,
    DATAFLOW_NODE_CONSTRAIN_TIME_RANGE_ID_PREFIX,
    DATAFLOW_NODE_SET_MEASURE_AGGREGATION_TIME,
)
from metricflow.dag.mf_dag import DagNode, DisplayedProperty, MetricFlowDag, NodeId
from metricflow.dataflow.builder.partitions import (
    PartitionDimensionJoinDescription,
    PartitionTimeDimensionJoinDescription,
)
from metricflow.dataflow.sql_table import SqlTable
from metricflow.dataset.dataset import DataSet
from metricflow.model.objects.metric import CumulativeMetricWindow
from metricflow.object_utils import pformat_big_objects
from metricflow.specs import (
    MetricInputMeasureSpec,
    OrderBySpec,
    InstanceSpec,
    MetricSpec,
    LinklessIdentifierSpec,
    TimeDimensionReference,
    SpecWhereClauseConstraint,
)
from metricflow.time.time_granularity import TimeGranularity
from metricflow.visitor import Visitable, VisitorOutputT

# The type of data set that is flowing out of the source nodes
SourceDataSetT = TypeVar("SourceDataSetT", bound=DataSet)


class DataflowPlanNode(Generic[SourceDataSetT], DagNode, Visitable, ABC):
    """A node in the graph representation of the dataflow.

    Each node in the graph performs an operation from the data that comes from the parent nodes, and the result is
    passed to the child nodes. The flow of data starts from source nodes, and ends at sink nodes.
    """

    def __init__(self, node_id: NodeId, parent_nodes: List[DataflowPlanNode]) -> None:
        """Constructor.

        Args:
            node_id: the ID for the node
            parent_nodes: data comes from the parent nodes.
        """
        self._parent_nodes = parent_nodes
        super().__init__(node_id=node_id)

    @property
    def parent_nodes(self) -> Sequence[DataflowPlanNode[SourceDataSetT]]:
        """Return the nodes where data for this node comes from."""
        return self._parent_nodes

    @abstractmethod
    def accept(self, visitor: DataflowPlanNodeVisitor[SourceDataSetT, VisitorOutputT]) -> VisitorOutputT:
        """Called when a visitor needs to visit this node."""
        pass


class DataflowPlanNodeVisitor(Generic[SourceDataSetT, VisitorOutputT], ABC):
    """An object that can be used to visit the nodes of a dataflow plan.

    Follows the visitor pattern: https://en.wikipedia.org/wiki/Visitor_pattern
    All visit* methods are similar and one exists for every type of node in the dataflow plan. The appropriate method
    will be called with DataflowPlanNode.accept().
    """

    @abstractmethod
    def visit_source_node(self, node: ReadSqlSourceNode[SourceDataSetT]) -> VisitorOutputT:  # noqa: D
        pass

    @abstractmethod
    def visit_join_to_base_output_node(self, node: JoinToBaseOutputNode[SourceDataSetT]) -> VisitorOutputT:  # noqa: D
        pass

    @abstractmethod
    def visit_join_aggregated_measures_by_groupby_columns_node(  # noqa: D
        self, node: JoinAggregatedMeasuresByGroupByColumnsNode[SourceDataSetT]
    ) -> VisitorOutputT:
        pass

    @abstractmethod
    def visit_aggregate_measures_node(self, node: AggregateMeasuresNode[SourceDataSetT]) -> VisitorOutputT:  # noqa: D
        pass

    @abstractmethod
    def visit_compute_metrics_node(self, node: ComputeMetricsNode[SourceDataSetT]) -> VisitorOutputT:  # noqa: D
        pass

    @abstractmethod
    def visit_order_by_limit_node(self, node: OrderByLimitNode[SourceDataSetT]) -> VisitorOutputT:  # noqa: D
        pass

    @abstractmethod
    def visit_where_constraint_node(self, node: WhereConstraintNode[SourceDataSetT]) -> VisitorOutputT:  # noqa: D
        pass

    @abstractmethod
    def visit_write_to_result_dataframe_node(  # noqa: D
        self, node: WriteToResultDataframeNode[SourceDataSetT]
    ) -> VisitorOutputT:
        pass

    @abstractmethod
    def visit_write_to_result_table_node(  # noqa: D
        self, node: WriteToResultTableNode[SourceDataSetT]
    ) -> VisitorOutputT:
        pass

    @abstractmethod
    def visit_pass_elements_filter_node(self, node: FilterElementsNode[SourceDataSetT]) -> VisitorOutputT:  # noqa: D
        pass

    @abstractmethod
    def visit_combine_metrics_node(self, node: CombineMetricsNode[SourceDataSetT]) -> VisitorOutputT:  # noqa: D
        pass

    @abstractmethod
    def visit_constrain_time_range_node(  # noqa: D
        self, node: ConstrainTimeRangeNode[SourceDataSetT]
    ) -> VisitorOutputT:
        pass

    @abstractmethod
    def visit_join_over_time_range_node(self, node: JoinOverTimeRangeNode[SourceDataSetT]) -> VisitorOutputT:  # noqa: D
        pass

    @abstractmethod
    def visit_metric_time_dimension_transform_node(  # noqa: D
        self, node: MetricTimeDimensionTransformNode[SourceDataSetT]
    ) -> VisitorOutputT:
        pass


class BaseOutput(Generic[SourceDataSetT], DataflowPlanNode[SourceDataSetT], ABC):
    """A node that outputs data in a "base" format.

    The base format is where the columns represent un-aggregated measures, dimensions, and identifiers.
    """

    pass


class ReadSqlSourceNode(Generic[SourceDataSetT], BaseOutput[SourceDataSetT]):
    """A source node where data from an SQL table or SQL query is read and output."""

    def __init__(self, data_set: SourceDataSetT) -> None:
        """Constructor.

        Args:
            data_set: dataset describing the SQL table / SQL query
        """
        self._dataset = data_set
        super().__init__(node_id=self.create_unique_id(), parent_nodes=[])

    @classmethod
    def id_prefix(cls) -> str:  # noqa: D
        return DATAFLOW_NODE_READ_SQL_SOURCE_ID_PREFIX

    def accept(self, visitor: DataflowPlanNodeVisitor[SourceDataSetT, VisitorOutputT]) -> VisitorOutputT:  # noqa: D
        return visitor.visit_source_node(self)

    @property
    def data_set(self) -> SourceDataSetT:
        """Return the data set that this source represents and is passed to the child nodes."""
        return self._dataset

    def __str__(self) -> str:  # noqa: D
        return jinja2.Template(
            textwrap.dedent(
                """\
                <{{ class_name }} data_set={{ data_set }} />
                """
            )
        ).render(class_name=self.__class__.__name__, data_set=str(self.data_set))

    @property
    def description(self) -> str:  # noqa: D
        return f"""Read From {self.data_set}"""

    @property
    def displayed_properties(self) -> List[DisplayedProperty]:  # noqa: D
        return super().displayed_properties + [
            DisplayedProperty("data_set", self.data_set),
        ]


@dataclass(frozen=True)
class JoinDescription(Generic[SourceDataSetT]):
    """Describes how data from a node should be joined to data from another node."""

    join_node: BaseOutput[SourceDataSetT]
    join_on_identifier: LinklessIdentifierSpec

    join_on_partition_dimensions: Tuple[PartitionDimensionJoinDescription, ...]
    join_on_partition_time_dimensions: Tuple[PartitionTimeDimensionJoinDescription, ...]


class JoinToBaseOutputNode(Generic[SourceDataSetT], BaseOutput[SourceDataSetT]):
    """A node that joins data from other nodes to a standard output node, one by one via identifier."""

    def __init__(
        self,
        parent_node: BaseOutput[SourceDataSetT],
        join_targets: List[JoinDescription[SourceDataSetT]],
        node_id: Optional[NodeId] = None,
    ) -> None:
        """Constructor.

        Args:
            parent_node: node with standard output
            join_targets: other sources that should be joined to this node.
            node_id: Override the node ID with this value
        """
        self._left_node = parent_node
        self._join_targets = join_targets

        # Doing a list comprehension throws a type error, so doing it this way.
        parent_nodes: List[DataflowPlanNode[SourceDataSetT]] = [self._left_node]
        for join_target in self._join_targets:
            parent_nodes.append(join_target.join_node)
        super().__init__(node_id=node_id or self.create_unique_id(), parent_nodes=parent_nodes)

    @classmethod
    def id_prefix(cls) -> str:  # noqa: D
        return DATAFLOW_NODE_JOIN_TO_STANDARD_OUTPUT_ID_PREFIX

    def accept(self, visitor: DataflowPlanNodeVisitor[SourceDataSetT, VisitorOutputT]) -> VisitorOutputT:  # noqa: D
        return visitor.visit_join_to_base_output_node(self)

    @property
    def description(self) -> str:  # noqa: D
        return """Join Standard Outputs"""

    @property
    def left_node(self) -> BaseOutput[SourceDataSetT]:  # noqa: D
        return self._left_node

    @property
    def join_targets(self) -> List[JoinDescription]:  # noqa: D
        return self._join_targets

    @property
    def displayed_properties(self) -> List[DisplayedProperty]:  # noqa: D
        return super().displayed_properties + [
            DisplayedProperty(f"join{i}_for_node_id_{join_description.join_node.node_id}", join_description)
            for i, join_description in enumerate(self._join_targets)
        ]


class JoinOverTimeRangeNode(Generic[SourceDataSetT], BaseOutput[SourceDataSetT]):
    """A node that allows for cumulative metric computation by doing a self join across a cumulative date range."""

    def __init__(
        self,
        parent_node: BaseOutput[SourceDataSetT],
        metric_time_dimension_reference: TimeDimensionReference,
        window: Optional[CumulativeMetricWindow],
        grain_to_date: Optional[TimeGranularity],
        node_id: Optional[NodeId] = None,
        time_range_constraint: Optional[TimeRangeConstraint] = None,
    ) -> None:
        """Constructor.

        Args:
            parent_node: node with standard output
            metric_time_dimension_reference: the name of the virtual time dimension used in queries to refer to the time
            dimension that each measure should be aggregated by.
            window: time window to join over
            grain_to_date: indicates time range should start from the beginning of this time granularity (eg month to day)
            node_id: Override the node ID with this value
            time_range_constraint: time range to aggregate over
        """
        self._parent_node = parent_node
        self._grain_to_date = grain_to_date
        self._window = window
        self._metric_time_dimension_reference = metric_time_dimension_reference
        self.time_range_constraint = time_range_constraint

        # Doing a list comprehension throws a type error, so doing it this way.
        parent_nodes: List[DataflowPlanNode[SourceDataSetT]] = [self._parent_node]
        super().__init__(node_id=node_id or self.create_unique_id(), parent_nodes=parent_nodes)

    @classmethod
    def id_prefix(cls) -> str:  # noqa: D
        return DATAFLOW_NODE_JOIN_SELF_OVER_TIME_RANGE_ID_PREFIX

    def accept(self, visitor: DataflowPlanNodeVisitor[SourceDataSetT, VisitorOutputT]) -> VisitorOutputT:  # noqa: D
        return visitor.visit_join_over_time_range_node(self)

    @property
    def metric_time_dimension_reference(self) -> TimeDimensionReference:  # noqa: D
        return self._metric_time_dimension_reference

    @property
    def grain_to_date(self) -> Optional[TimeGranularity]:  # noqa: D
        return self._grain_to_date

    @property
    def description(self) -> str:  # noqa: D
        return """Join Self Over Time Range"""

    @property
    def parent_node(self) -> BaseOutput[SourceDataSetT]:  # noqa: D
        return self._parent_node

    @property
    def window(self) -> Optional[CumulativeMetricWindow]:  # noqa: D
        return self._window

    @property
    def displayed_properties(self) -> List[DisplayedProperty]:  # noqa: D
        return super().displayed_properties


class AggregatedMeasuresOutput(Generic[SourceDataSetT], BaseOutput[SourceDataSetT], ABC):
    """A node that outputs data where the measures are aggregated.

    The measures are aggregated with respect to the present identifiers and dimensions.
    """

    pass


class AggregateMeasuresNode(Generic[SourceDataSetT], AggregatedMeasuresOutput[SourceDataSetT]):
    """A node that aggregates the measures by the associated group by elements.

    In the event that one or more of the aggregated input measures has an alias assigned to it, any output query
    resulting from an operation on this node must apply the alias and transform the measure instances accordingly,
    otherwise this join could produce a query with two identically named measure columns with, e.g., different
    constraints applied to the measure.
    """

    def __init__(self, parent_node: BaseOutput, metric_input_measure_specs: Tuple[MetricInputMeasureSpec, ...]) -> None:
        """Initializer for AggregateMeasuresNode

        The input measure specs are required for downstream nodes to be aware of any input measures with
        user-provided aliases, such as we might encounter with constrained and unconstrained versions of the
        same input measure.
        """
        self._parent_node = parent_node
        self._metric_input_measure_specs = metric_input_measure_specs

        super().__init__(node_id=self.create_unique_id(), parent_nodes=[self._parent_node])

    @classmethod
    def id_prefix(cls) -> str:  # noqa: D
        return DATAFLOW_NODE_AGGREGATE_MEASURES_ID_PREFIX

    def accept(self, visitor: DataflowPlanNodeVisitor[SourceDataSetT, VisitorOutputT]) -> VisitorOutputT:  # noqa: D
        return visitor.visit_aggregate_measures_node(self)

    @property
    def description(self) -> str:  # noqa: D
        return """Aggregate Measures"""

    @property
    def parent_node(self) -> BaseOutput:  # noqa: D
        return self._parent_node

    @property
    def metric_input_measure_specs(self) -> Tuple[MetricInputMeasureSpec, ...]:
        """Iterable of specs for measure inputs to downstream metrics.

        Used for assigning aliases to output columns produced by aggregated measures.
        """
        return self._metric_input_measure_specs


class JoinAggregatedMeasuresByGroupByColumnsNode(Generic[SourceDataSetT], AggregatedMeasuresOutput[SourceDataSetT]):
    """A node that joins aggregated measures with group by elements.

    This is designed to link two separate data sources with measures aggregated by the complete set of group by
    elements shared across both measures. Due to the way the DataflowPlan currently processes joins, this means
    each separate data source will be pre-aggregated, and this final join will be run across fully aggregated
    sets of input data. As such, all this requires is the list of aggregated measure outputs, since they can be
    transformed into a SqlDataSet containing the complete list of non-measure specs for joining.
    """

    def __init__(
        self,
        parent_nodes: Sequence[BaseOutput[SourceDataSetT]],
    ):
        """Constructor.

        Args:
            parent_nodes: sequence of nodes that output aggregated measures
        """
        if len(parent_nodes) < 2:
            raise ValueError(
                "This node is designed for joining 2 or more aggregated nodes together, but "
                f"we got {len(parent_nodes)}"
            )
        super().__init__(node_id=self.create_unique_id(), parent_nodes=list(parent_nodes))

    @classmethod
    def id_prefix(cls) -> str:  # noqa: D
        return DATAFLOW_NODE_JOIN_AGGREGATED_MEASURES_BY_GROUPBY_COLUMNS_PREFIX

    def accept(self, visitor: DataflowPlanNodeVisitor[SourceDataSetT, VisitorOutputT]) -> VisitorOutputT:  # noqa: D
        return visitor.visit_join_aggregated_measures_by_groupby_columns_node(self)

    @property
    def description(self) -> str:  # noqa: D
        return """Join Aggregated Measures with Standard Outputs"""

    @property
    def displayed_properties(self) -> List[DisplayedProperty]:  # noqa: D
        return super().displayed_properties + [
            DisplayedProperty("Join aggregated measure nodes: ", f"{[node.node_id for node in self.parent_nodes]}")
        ]


class ComputedMetricsOutput(Generic[SourceDataSetT], BaseOutput[SourceDataSetT], ABC):
    """A node that outputs data that contains metrics computed from measures."""

    pass


class ComputeMetricsNode(Generic[SourceDataSetT], ComputedMetricsOutput[SourceDataSetT]):
    """A node that computes metrics from input measures. Dimensions / identifiers are passed through."""

    def __init__(self, parent_node: BaseOutput[SourceDataSetT], metric_specs: List[MetricSpec]) -> None:  # noqa: D
        """Constructor.

        Args:
            parent_node: Node where data is coming from.
            metric_specs: The specs for the metrics that this should compute.
        """
        self._parent_node = parent_node
        self._metric_specs = metric_specs
        super().__init__(node_id=self.create_unique_id(), parent_nodes=[self._parent_node])

    @classmethod
    def id_prefix(cls) -> str:  # noqa: D
        return DATAFLOW_NODE_COMPUTE_METRICS_ID_PREFIX

    @property
    def metric_specs(self) -> List[MetricSpec]:  # noqa: D
        """The metric instances that this node is supposed to compute and should have in the output."""
        return self._metric_specs

    def accept(self, visitor: DataflowPlanNodeVisitor[SourceDataSetT, VisitorOutputT]) -> VisitorOutputT:  # noqa: D
        return visitor.visit_compute_metrics_node(self)

    @property
    def description(self) -> str:  # noqa: D
        return """Compute Metrics via Expressions"""

    @property
    def displayed_properties(self) -> List[DisplayedProperty]:  # noqa: D
        return super().displayed_properties + [
            DisplayedProperty("metric_spec", metric_spec) for metric_spec in self._metric_specs
        ]

    @property
    def parent_node(self) -> BaseOutput:  # noqa: D
        return self._parent_node


class OrderByLimitNode(Generic[SourceDataSetT], ComputedMetricsOutput[SourceDataSetT]):
    """A node that re-orders the input data with a limit."""

    def __init__(
        self,
        order_by_specs: List[OrderBySpec],
        parent_node: Union[BaseOutput[SourceDataSetT], ComputedMetricsOutput[SourceDataSetT]],
        limit: Optional[int] = None,
    ) -> None:
        """Constructor.

        Args:
            order_by_specs: describes how to order the incoming data.
            limit: number of rows to limit.
            parent_node: self-explanatory.
        """
        self._order_by_specs = order_by_specs
        self._limit = limit
        self._metrics_output_node = parent_node
        self._parent_node = parent_node
        super().__init__(node_id=self.create_unique_id(), parent_nodes=[self._metrics_output_node])

    @classmethod
    def id_prefix(cls) -> str:  # noqa: D
        return DATAFLOW_NODE_ORDER_BY_LIMIT_ID_PREFIX

    @property
    def order_by_specs(self) -> List[OrderBySpec]:
        """The elements that this node should order the input data."""
        return self._order_by_specs

    @property
    def limit(self) -> Optional[int]:
        """The number of rows to limit by"""
        return self._limit

    def accept(self, visitor: DataflowPlanNodeVisitor[SourceDataSetT, VisitorOutputT]) -> VisitorOutputT:  # noqa: D
        return visitor.visit_order_by_limit_node(self)

    @property
    def description(self) -> str:  # noqa: D
        return f"Order By {[x.item.qualified_name for x in self._order_by_specs]}" + (
            f" Limit {self._limit}" if self.limit else ""
        )

    @property
    def displayed_properties(self) -> List[DisplayedProperty]:  # noqa: D
        return (
            super().displayed_properties
            + [DisplayedProperty("order_by_spec", order_by_spec) for order_by_spec in self._order_by_specs]
            + [DisplayedProperty("limit", str(self.limit))]
        )

    @property
    def parent_node(self) -> Union[BaseOutput[SourceDataSetT], ComputedMetricsOutput[SourceDataSetT]]:  # noqa: D
        return self._parent_node


class MetricTimeDimensionTransformNode(Generic[SourceDataSetT], BaseOutput[SourceDataSetT]):
    """A node transforms the input data set so that it contains the metric time dimension and relevant measures.

    The metric time dimension is used later to aggregate all measures in the data set.

    Input: a data set containing measures along with the associated aggregation time dimension.

    Output: a data set similar to the input data set, but includes the configured aggregation time dimension as the
    metric time dimension and only contains measures that are defined to use it.
    """

    def __init__(  # noqa: D
        self,
        parent_node: BaseOutput[SourceDataSetT],
        aggregation_time_dimension_reference: TimeDimensionReference,
    ) -> None:
        self._aggregation_time_dimension_reference = aggregation_time_dimension_reference
        self._parent_node = parent_node
        super().__init__(node_id=self.create_unique_id(), parent_nodes=[parent_node])

    @classmethod
    def id_prefix(cls) -> str:  # noqa: D
        return DATAFLOW_NODE_SET_MEASURE_AGGREGATION_TIME

    @property
    def aggregation_time_dimension_reference(self) -> TimeDimensionReference:
        """The time dimension that measures in the input should be aggregated to."""
        return self._aggregation_time_dimension_reference

    def accept(self, visitor: DataflowPlanNodeVisitor[SourceDataSetT, VisitorOutputT]) -> VisitorOutputT:  # noqa: D
        return visitor.visit_metric_time_dimension_transform_node(self)

    @property
    def description(self) -> str:  # noqa: D
        return f"Metric Time Dimension '{self.aggregation_time_dimension_reference.element_name}'" ""

    @property
    def displayed_properties(self) -> List[DisplayedProperty]:  # noqa: D
        return super().displayed_properties + [
            DisplayedProperty("aggregation_time_dimension", self.aggregation_time_dimension_reference.element_name)
        ]

    @property
    def parent_node(self) -> BaseOutput[SourceDataSetT]:  # noqa: D
        return self._parent_node


class SinkNodeVisitor(Generic[SourceDataSetT, VisitorOutputT], ABC):
    """Similar to DataflowPlanNodeVisitor, but only for sink nodes."""

    @abstractmethod
    def visit_write_to_result_dataframe_node(  # noqa: D
        self, node: WriteToResultDataframeNode[SourceDataSetT]
    ) -> VisitorOutputT:
        pass

    @abstractmethod
    def visit_write_to_result_table_node(  # noqa: D
        self, node: WriteToResultTableNode[SourceDataSetT]
    ) -> VisitorOutputT:
        pass


class SinkOutput(Generic[SourceDataSetT], DataflowPlanNode[SourceDataSetT], ABC):
    """A node where incoming data goes out of the graph."""

    @abstractmethod
    def accept_sink_node_visitor(  # noqa: D
        self, visitor: SinkNodeVisitor[SourceDataSetT, VisitorOutputT]
    ) -> VisitorOutputT:
        pass

    @property
    @abstractmethod
    def parent_node(self) -> BaseOutput[SourceDataSetT]:  # noqa: D
        pass


class WriteToResultDataframeNode(Generic[SourceDataSetT], SinkOutput[SourceDataSetT]):
    """A node where incoming data gets written to a dataframe."""

    def __init__(self, parent_node: BaseOutput) -> None:  # noqa: D
        self._parent_node = parent_node
        super().__init__(node_id=self.create_unique_id(), parent_nodes=[parent_node])

    @classmethod
    def id_prefix(cls) -> str:  # noqa: D
        return DATAFLOW_NODE_WRITE_TO_RESULT_DATAFRAME_ID_PREFIX

    def accept(self, visitor: DataflowPlanNodeVisitor[SourceDataSetT, VisitorOutputT]) -> VisitorOutputT:  # noqa: D
        return visitor.visit_write_to_result_dataframe_node(self)

    @property
    def description(self) -> str:  # noqa: D
        return """Write to Dataframe"""

    @property
    def parent_node(self) -> BaseOutput[SourceDataSetT]:  # noqa: D
        assert len(self.parent_nodes) == 1
        return self._parent_node

    def accept_sink_node_visitor(  # noqa: D
        self, visitor: SinkNodeVisitor[SourceDataSetT, VisitorOutputT]
    ) -> VisitorOutputT:
        return visitor.visit_write_to_result_dataframe_node(self)


class WriteToResultTableNode(Generic[SourceDataSetT], SinkOutput[SourceDataSetT]):
    """A node where incoming data gets written to a table."""

    def __init__(  # noqa: D
        self,
        parent_node: BaseOutput[SourceDataSetT],
        output_sql_table: SqlTable,
    ) -> None:
        """Constructor.

        Args:
            parent_node: node that outputs the computed metrics.
            output_sql_table: the table where the computed metrics should be written to.
        """
        self._parent_node = parent_node
        self._output_sql_table = output_sql_table
        super().__init__(node_id=self.create_unique_id(), parent_nodes=[parent_node])

    @classmethod
    def id_prefix(cls) -> str:  # noqa: D
        return DATAFLOW_NODE_WRITE_TO_RESULT_DATAFRAME_ID_PREFIX

    def accept(self, visitor: DataflowPlanNodeVisitor[SourceDataSetT, VisitorOutputT]) -> VisitorOutputT:  # noqa: D
        return visitor.visit_write_to_result_table_node(self)

    @property
    def description(self) -> str:  # noqa: D
        return """Write to Table"""

    @property
    def parent_node(self) -> BaseOutput[SourceDataSetT]:  # noqa: D
        assert len(self.parent_nodes) == 1
        return self._parent_node

    def accept_sink_node_visitor(  # noqa: D
        self, visitor: SinkNodeVisitor[SourceDataSetT, VisitorOutputT]
    ) -> VisitorOutputT:
        return visitor.visit_write_to_result_table_node(self)

    @property
    def output_sql_table(self) -> SqlTable:  # noqa: D
        return self._output_sql_table


class FilterElementsNode(Generic[SourceDataSetT], BaseOutput[SourceDataSetT]):
    """Only passes the listed elements."""

    def __init__(  # noqa: D
        self,
        parent_node: BaseOutput[SourceDataSetT],
        include_specs: Sequence[InstanceSpec],
    ) -> None:
        self._include_specs = include_specs
        super().__init__(node_id=self.create_unique_id(), parent_nodes=[parent_node])

    @classmethod
    def id_prefix(cls) -> str:  # noqa: D
        return DATAFLOW_NODE_PASS_FILTER_ELEMENTS_ID_PREFIX

    @property
    def include_specs(self) -> List[InstanceSpec]:
        """Returns the specs for the elements that it should pass."""
        return list(self._include_specs)

    def accept(self, visitor: DataflowPlanNodeVisitor[SourceDataSetT, VisitorOutputT]) -> VisitorOutputT:  # noqa: D
        return visitor.visit_pass_elements_filter_node(self)

    @property
    def description(self) -> str:  # noqa: D
        formatted_str = textwrap.indent(
            pformat_big_objects([x.qualified_name for x in self._include_specs]), prefix="  "
        )
        return f"Pass Only Elements:\n{formatted_str}"

    @property
    def displayed_properties(self) -> List[DisplayedProperty]:  # noqa: D
        return super().displayed_properties + [
            DisplayedProperty("include_spec", include_spec) for include_spec in self._include_specs
        ]

    @property
    def parent_node(self) -> DataflowPlanNode:  # noqa: D
        assert len(self.parent_nodes) == 1
        return self.parent_nodes[0]


class WhereConstraintNode(AggregatedMeasuresOutput[SourceDataSetT]):
    """Only passes the listed elements."""

    def __init__(  # noqa: D
        self,
        parent_node: BaseOutput[SourceDataSetT],
        where_constraint: SpecWhereClauseConstraint,
    ) -> None:
        self._where = where_constraint
        self.parent_node = parent_node
        super().__init__(node_id=self.create_unique_id(), parent_nodes=[parent_node])

    @classmethod
    def id_prefix(cls) -> str:  # noqa: D
        return DATAFLOW_NODE_WHERE_CONSTRAINT_ID_PREFIX

    @property
    def where(self) -> SpecWhereClauseConstraint:
        """Returns the specs for the elements that it should pass."""
        return self._where

    def accept(self, visitor: DataflowPlanNodeVisitor[SourceDataSetT, VisitorOutputT]) -> VisitorOutputT:  # noqa: D
        return visitor.visit_where_constraint_node(self)

    @property
    def description(self) -> str:  # noqa: D
        # Can't put the where condition here as it can cause rendering issues when there are SQL execution parameters.
        # e.g. "Constrain Output with WHERE listing__country = :1"
        return "Constrain Output with WHERE"

    @property
    def displayed_properties(self) -> List[DisplayedProperty]:  # noqa: D
        return super().displayed_properties + [DisplayedProperty("where_condition", self.where)]


class CombineMetricsNode(Generic[SourceDataSetT], ComputedMetricsOutput[SourceDataSetT]):
    """Combines metrics from different nodes into a single output"""

    def __init__(  # noqa: D
        self,
        parent_nodes: Sequence[ComputedMetricsOutput[SourceDataSetT]],
    ) -> None:
        super().__init__(node_id=self.create_unique_id(), parent_nodes=list(parent_nodes))

    @classmethod
    def id_prefix(cls) -> str:  # noqa: D
        return DATAFLOW_NODE_COMBINE_METRICS_ID_PREFIX

    def accept(self, visitor: DataflowPlanNodeVisitor[SourceDataSetT, VisitorOutputT]) -> VisitorOutputT:  # noqa: D
        return visitor.visit_combine_metrics_node(self)

    @property
    def description(self) -> str:  # noqa: D
        return "Combine Metrics"


class ConstrainTimeRangeNode(AggregatedMeasuresOutput[SourceDataSetT], BaseOutput[SourceDataSetT]):
    """Constrains the time range of the input data set.

    For example, if the input data set had "sales by date", then this would restrict the data set so that it only
    includes sales for a specific range of dates.
    """

    def __init__(  # noqa: D
        self,
        parent_node: BaseOutput[SourceDataSetT],
        time_range_constraint: TimeRangeConstraint,
    ) -> None:
        self._time_range_constraint = time_range_constraint
        super().__init__(node_id=self.create_unique_id(), parent_nodes=[parent_node])

    @classmethod
    def id_prefix(cls) -> str:  # noqa: D
        return DATAFLOW_NODE_CONSTRAIN_TIME_RANGE_ID_PREFIX

    def accept(self, visitor: DataflowPlanNodeVisitor[SourceDataSetT, VisitorOutputT]) -> VisitorOutputT:  # noqa: D
        return visitor.visit_constrain_time_range_node(self)

    @property
    def description(self) -> str:  # noqa: D
        return (
            f"Constrain Time Range to [{self.time_range_constraint.start_time.isoformat()}, "
            f"{self.time_range_constraint.end_time.isoformat()}]"
        )

    @property
    def time_range_constraint(self) -> TimeRangeConstraint:  # noqa: D
        return self._time_range_constraint

    @property
    def parent_node(self) -> DataflowPlanNode:  # noqa: D
        assert len(self.parent_nodes) == 1
        return self.parent_nodes[0]

    @property
    def displayed_properties(self) -> List[DisplayedProperty]:  # noqa: D
        return super().displayed_properties + [
            DisplayedProperty("time_range_start", self.time_range_constraint.start_time.isoformat()),
            DisplayedProperty("time_range_end", self.time_range_constraint.end_time.isoformat()),
        ]


class DataflowPlan(Generic[SourceDataSetT], MetricFlowDag[SinkOutput[SourceDataSetT]]):
    """Describes the flow of metric data as it goes from source nodes to sink nodes in the graph."""

    def __init__(self, plan_id: str, sink_output_nodes: List[SinkOutput[SourceDataSetT]]) -> None:  # noqa: D
        if len(sink_output_nodes) == 0:
            raise RuntimeError("Can't create a dataflow plan without sink node(s).")
        self._sink_output_nodes = sink_output_nodes
        super().__init__(dag_id=plan_id, sink_nodes=sink_output_nodes)

    @property
    def sink_output_nodes(self) -> List[SinkOutput[SourceDataSetT]]:  # noqa: D
        return self._sink_output_nodes

    @property
    def sink_output_node(self) -> SinkOutput[SourceDataSetT]:  # noqa: D
        assert len(self._sink_output_nodes) == 1
        return self._sink_output_nodes[0]
