from __future__ import annotations

import collections
import logging
import time
from dataclasses import dataclass
from typing import List, TypeVar, Optional, Generic, Dict, Tuple, Sequence, Set, Union

from metricflow.constraints.time_constraint import TimeRangeConstraint
from metricflow.dag.id_generation import IdGeneratorRegistry, DATAFLOW_PLAN_PREFIX
from metricflow.dataflow.builder.costing import DefaultCostFunction, DataflowPlanNodeCostFunction
from metricflow.dataflow.builder.node_data_set import DataflowPlanNodeOutputDataSetResolver
from metricflow.dataflow.builder.node_evaluator import (
    NodeEvaluatorForLinkableInstances,
    JoinLinkableInstancesRecipe,
    LinkableInstanceSatisfiabilityEvaluation,
)
from metricflow.dataflow.dataflow_plan import (
    AggregateMeasuresNode,
    BaseOutput,
    CombineMetricsNode,
    ComputeMetricsNode,
    ComputedMetricsOutput,
    ConstrainTimeRangeNode,
    DataflowPlan,
    FilterElementsNode,
    JoinAggregatedMeasuresByGroupByColumnsNode,
    JoinDescription,
    JoinOverTimeRangeNode,
    JoinToBaseOutputNode,
    OrderByLimitNode,
    WhereConstraintNode,
    WriteToResultDataframeNode,
    WriteToResultTableNode,
    SinkOutput,
)
from metricflow.dataflow.dataflow_plan_to_text import dataflow_dag_as_text
from metricflow.dataflow.sql_table import SqlTable
from metricflow.dataset.dataset import DataSet
from metricflow.errors.errors import UnableToSatisfyQueryError
from metricflow.model.objects.metric import MetricType, CumulativeMetricWindow
from metricflow.model.semantic_model import SemanticModel
from metricflow.object_utils import pformat_big_objects, assert_exactly_one_arg_set
from metricflow.plan_conversion.column_resolver import DefaultColumnAssociationResolver
from metricflow.plan_conversion.node_processor import PreDimensionJoinNodeProcessor
from metricflow.plan_conversion.sql_dataset import SqlDataSet
from metricflow.plan_conversion.time_spine import TimeSpineSource
from metricflow.query.query_parser import MetricFlowQueryParser
from metricflow.specs import (
    MetricSpec,
    LinkableInstanceSpec,
    MetricFlowQuerySpec,
    MeasureSpec,
    TimeDimensionSpec,
    IdentifierSpec,
    InstanceSpec,
    DimensionSpec,
    OrderBySpec,
    SpecWhereClauseConstraint,
    LinkableSpecSet,
    ColumnAssociationResolver,
)
from metricflow.time.time_granularity import TimeGranularity

logger = logging.getLogger(__name__)

# The type of data set that at the source nodes.
SqlDataSetT = TypeVar("SqlDataSetT", bound=SqlDataSet)


@dataclass(frozen=True)
class MeasureRecipe(Generic[SqlDataSetT]):
    """Get a recipe for how to build a dataflow plan node that outputs measures and the needed linkable instances.

    The recipe involves filtering the measure node so that it only outputs the measures and the instances associated with
    required_local_linkable_specs, then joining the nodes containing the linkable instances according to the recipes
    in join_linkable_instances_recipes.
    """

    measure_node: BaseOutput[SqlDataSetT]
    required_local_linkable_specs: Tuple[LinkableInstanceSpec, ...]
    join_linkable_instances_recipes: Tuple[JoinLinkableInstancesRecipe, ...]


class DataflowPlanBuilder(Generic[SqlDataSetT]):
    """Builds a dataflow plan to satisfy a given query."""

    def __init__(  # noqa: D
        self,
        source_nodes: Sequence[BaseOutput[SqlDataSetT]],
        semantic_model: SemanticModel,
        time_spine_source: TimeSpineSource,
        cost_function: DataflowPlanNodeCostFunction = DefaultCostFunction[SqlDataSetT](),
        node_output_resolver: Optional[DataflowPlanNodeOutputDataSetResolver[SqlDataSetT]] = None,
        column_association_resolver: Optional[ColumnAssociationResolver] = None,
    ) -> None:
        self._data_source_semantics = semantic_model.data_source_semantics
        self._metric_semantics = semantic_model.metric_semantics
        self._metric_time_dimension_reference = DataSet.metric_time_dimension_reference()
        self._cost_function = cost_function
        self._source_nodes = source_nodes
        self._node_data_set_resolver = (
            DataflowPlanNodeOutputDataSetResolver[SqlDataSetT](
                column_association_resolver=(
                    DefaultColumnAssociationResolver(semantic_model)
                    if not column_association_resolver
                    else column_association_resolver
                ),
                semantic_model=semantic_model,
                time_spine_source=time_spine_source,
            )
            if not node_output_resolver
            else node_output_resolver
        )

    def build_plan(
        self, query_spec: MetricFlowQuerySpec, output_sql_table: Optional[SqlTable] = None
    ) -> DataflowPlan[SqlDataSetT]:
        """Generate a plan for reading the results of a query with the given spec into a dataframe or table"""
        metrics_output_node = self._build_metrics_output_node(query_spec)

        sink_node = DataflowPlanBuilder.build_sink_node_from_metrics_output_node(
            computed_metrics_output=metrics_output_node,
            order_by_specs=query_spec.order_by_specs,
            output_sql_table=output_sql_table,
            limit=query_spec.limit,
        )

        plan_id = IdGeneratorRegistry.for_class(DataflowPlanBuilder).create_id(DATAFLOW_PLAN_PREFIX)

        return DataflowPlan(plan_id=plan_id, sink_output_nodes=[sink_node])

    def _build_metrics_output_node(self, query_spec: MetricFlowQuerySpec) -> ComputedMetricsOutput[SqlDataSetT]:
        """Build a node that computes all requested metrics along with linkables."""

        compute_metrics_nodes: List[ComputedMetricsOutput[SqlDataSetT]] = []
        for metric_spec in query_spec.metric_specs:
            logger.info(f"Generating compute metrics node for {metric_spec}")
            metric = self._metric_semantics.get_metric(metric_spec)

            measure_specs = self._metric_semantics.measures_for_metric(metric_spec)

            logger.info(
                f"For {metric_spec}, needed measures are:\n" f"{pformat_big_objects(measure_specs=measure_specs)}"
            )

            combined_where = query_spec.where_constraint
            if metric.constraint:
                metric_constraint = MetricFlowQueryParser.convert_to_spec_where_constraint(
                    self._data_source_semantics, metric.constraint
                )
                if combined_where:
                    combined_where = combined_where.combine(metric_constraint)
                else:
                    combined_where = metric_constraint

            aggregated_measures_node = self.build_aggregated_measures(
                measure_specs=measure_specs,
                queried_linkable_specs=query_spec.linkable_specs,
                where_constraint=combined_where,
                time_range_constraint=query_spec.time_range_constraint,
                cumulative=metric.type == MetricType.CUMULATIVE,
                cumulative_window=metric.type_params.window if metric.type == MetricType.CUMULATIVE else None,
                cumulative_grain_to_date=(
                    metric.type_params.grain_to_date if metric.type == MetricType.CUMULATIVE else None
                ),
            )
            compute_metrics_nodes.append(
                self.build_computed_metrics_node(
                    metric_spec=metric_spec,
                    aggregated_measures_node=aggregated_measures_node,
                )
            )

        if len(compute_metrics_nodes) == 1:
            return compute_metrics_nodes[0]

        return CombineMetricsNode[SqlDataSetT](parent_nodes=compute_metrics_nodes)

    def build_plan_for_distinct_values(
        self,
        metric_specs: Tuple[MetricSpec, ...],
        dimension_spec: Optional[DimensionSpec] = None,
        time_dimension_spec: Optional[TimeDimensionSpec] = None,
        identifier_spec: Optional[IdentifierSpec] = None,
        time_range_constraint: Optional[TimeRangeConstraint] = None,
        limit: Optional[int] = None,
    ) -> DataflowPlan[SqlDataSetT]:
        """Generate a plan that would get the distinct values of a linkable instance.

        e.g. distinct listing__country_latest for bookings by listing__country_latest
        """
        assert_exactly_one_arg_set(
            dimension_spec=dimension_spec, time_dimension_spec=time_dimension_spec, identifier_spec=identifier_spec
        )

        # Doing this to keep the type checker happy, but assert_exactly_one_arg_set should ensure this.
        linkable_spec: Optional[LinkableInstanceSpec] = dimension_spec or time_dimension_spec or identifier_spec
        assert linkable_spec

        metrics_output_node = self._build_metrics_output_node(
            query_spec=MetricFlowQuerySpec(
                metric_specs=metric_specs,
                dimension_specs=(dimension_spec,) if dimension_spec else (),
                time_dimension_specs=(time_dimension_spec,) if time_dimension_spec else (),
                identifier_specs=(identifier_spec,) if identifier_spec else (),
                time_range_constraint=time_range_constraint,
            ),
        )

        distinct_values_node = FilterElementsNode(
            parent_node=metrics_output_node,
            include_specs=[linkable_spec],
        )

        sink_node = self.build_sink_node_from_metrics_output_node(
            computed_metrics_output=distinct_values_node,
            order_by_specs=(
                OrderBySpec(
                    item=linkable_spec,
                    descending=False,
                ),
            ),
            limit=limit,
        )

        plan_id = IdGeneratorRegistry.for_class(DataflowPlanBuilder).create_id(DATAFLOW_PLAN_PREFIX)

        return DataflowPlan(
            plan_id=plan_id,
            sink_output_nodes=[sink_node],
        )

    @staticmethod
    def build_sink_node_from_metrics_output_node(
        computed_metrics_output: BaseOutput[SqlDataSetT],
        order_by_specs: Sequence[OrderBySpec],
        output_sql_table: Optional[SqlTable] = None,
        limit: Optional[int] = None,
    ) -> SinkOutput[SqlDataSetT]:
        """Adds order by / limit / write nodes."""
        order_by_limit: Optional[OrderByLimitNode[SqlDataSetT]] = None

        if order_by_specs or limit:
            order_by_limit = OrderByLimitNode[SqlDataSetT](
                order_by_specs=list(order_by_specs),
                limit=limit,
                parent_node=computed_metrics_output,
            )

        write_result_node: SinkOutput[SqlDataSetT]
        if not output_sql_table:
            write_result_node = WriteToResultDataframeNode[SqlDataSetT](
                parent_node=order_by_limit or computed_metrics_output,
            )
        else:
            write_result_node = WriteToResultTableNode[SqlDataSetT](
                parent_node=order_by_limit or computed_metrics_output,
                output_sql_table=output_sql_table,
            )

        return write_result_node

    @staticmethod
    def _contains_multihop_linkables(linkable_specs: Sequence[LinkableInstanceSpec]) -> bool:
        """Returns true if any of the linkable specs requires a multi-hop join to realize."""
        return any(len(x.identifier_links) > 1 for x in linkable_specs)

    def _get_data_source_names_for_measures(self, measure_names: Sequence[MeasureSpec]) -> Set[str]:
        """Return the names of the data sources needed to compute the input measures

        This is a temporary method for use in assertion boundaries while we implement support for multiple data sources
        """
        data_source_names: Set[str] = set()
        for measure_name in measure_names:
            data_source_names = data_source_names.union(
                {d.name for d in self._data_source_semantics.get_data_sources_for_measure(measure_name.as_reference)}
            )
        return data_source_names

    def _sort_by_suitability(self, nodes: Sequence[BaseOutput[SqlDataSetT]]) -> Sequence[BaseOutput[SqlDataSetT]]:
        """Sort nodes by the cost, then by the number of linkable specs.

        Lower cost nodes will result in faster queries, and the lower the number of linkable specs means less
        aggregation required.
        """

        def sort_function(node: BaseOutput[SqlDataSetT]) -> Tuple[int, int]:
            data_set = self._node_data_set_resolver.get_output_data_set(node)
            return self._cost_function.calculate_cost(node).as_int, len(data_set.instance_set.spec_set.linkable_specs)

        return sorted(nodes, key=sort_function)

    def _select_source_nodes_with_meausres(
        self, measure_specs: Set[MeasureSpec], source_nodes: Sequence[BaseOutput[SqlDataSetT]]
    ) -> Sequence[BaseOutput[SqlDataSetT]]:
        nodes = []
        measure_specs_set = set(measure_specs)
        for source_node in source_nodes:
            measure_specs_in_node = self._node_data_set_resolver.get_output_data_set(
                source_node
            ).instance_set.spec_set.measure_specs
            if measure_specs_set.intersection(set(measure_specs_in_node)) == measure_specs_set:
                nodes.append(source_node)
        return nodes

    def _find_measure_recipe(
        self,
        measure_specs: Sequence[MeasureSpec],
        linkable_specs: Sequence[LinkableInstanceSpec],
        time_range_constraint: Optional[TimeRangeConstraint] = None,
    ) -> Optional[MeasureRecipe]:
        """Find a recipe for getting measure_specs along with the linkable specs.

        Prior to calling this method we should always be checking that all input measure specs come from
        the same base data source, otherwise the internal conditions here will be impossible to satisfy
        """
        data_sources = self._get_data_source_names_for_measures(measure_specs)
        if len(data_sources) > 1:
            raise ValueError(
                f"Cannot find a single measure recipe for measures {measure_specs} coming from multiple "
                f"data sources: {data_sources}. This suggests the measure_specs were not correctly filtered."
            )

        node_processor = PreDimensionJoinNodeProcessor(
            data_source_semantics=self._data_source_semantics,
            node_data_set_resolver=self._node_data_set_resolver,
        )

        source_nodes: Sequence[BaseOutput[SqlDataSetT]] = self._source_nodes

        # We only care about nodes that have all required measures
        potential_measure_nodes: Sequence[BaseOutput[SqlDataSetT]] = self._select_source_nodes_with_meausres(
            measure_specs=set(measure_specs), source_nodes=source_nodes
        )

        logger.info(f"There are {len(potential_measure_nodes)} potential measure source nodes")

        logger.info(f"Starting search with {len(source_nodes)} source nodes")
        start_time = time.time()
        # Only apply the time constraint to nodes that will be used for measures because some dimensional sources have
        # measures in them, and time constraining those would result in incomplete joins.
        if time_range_constraint:
            potential_measure_nodes = node_processor.add_time_range_constraint(
                source_nodes=potential_measure_nodes,
                metric_time_dimension_reference=self._metric_time_dimension_reference,
                time_range_constraint=time_range_constraint,
            )

        nodes_available_for_joins = node_processor.remove_unnecessary_nodes(
            desired_linkable_specs=linkable_specs,
            nodes=source_nodes,
            metric_time_dimension_reference=self._metric_time_dimension_reference,
        )
        logger.info(
            f"After removing unnecessary nodes, there are {len(nodes_available_for_joins)} nodes available for joins"
        )
        if DataflowPlanBuilder._contains_multihop_linkables(linkable_specs):
            nodes_available_for_joins = node_processor.add_multi_hop_joins(linkable_specs, source_nodes)
            logger.info(
                f"After adding multi-hop nonds, there are {nodes_available_for_joins} nodes available for joins"
            )

        logger.info(f"Processing nodes took: {time.time()-start_time:.2f}s")

        node_evaluator = NodeEvaluatorForLinkableInstances(
            data_source_semantics=self._data_source_semantics,
            nodes_available_for_joins=self._sort_by_suitability(nodes_available_for_joins),
            node_data_set_resolver=self._node_data_set_resolver,
        )

        # Dict from the node that contains the measure spec to the evaluation results.
        node_to_evaluation: Dict[BaseOutput, LinkableInstanceSatisfiabilityEvaluation] = {}

        for node in self._sort_by_suitability(potential_measure_nodes):
            data_set = self._node_data_set_resolver.get_output_data_set(node)

            missing_specs = [spec for spec in measure_specs if spec not in data_set.instance_set.spec_set.measure_specs]
            if missing_specs:
                logger.debug(
                    f"Skipping evaluation for node since it does not have all of the measure specs {missing_specs}:"
                    f"\n\n{dataflow_dag_as_text(node)}"
                )
                continue

            logger.debug(f"Evaluating measure node:\n{pformat_big_objects(measure_node=dataflow_dag_as_text(node))}")

            start_time = time.time()
            evaluation = node_evaluator.evaluate_node(
                start_node=node,
                required_linkable_specs=list(linkable_specs),
            )
            logger.info(f"Evaluation of {node} took {time.time() - start_time}s")

            logger.debug(
                f"Evaluation for measure node is:\n"
                f"{pformat_big_objects(node=dataflow_dag_as_text(node), evaluation=evaluation)}"
            )

            if len(evaluation.unjoinable_linkable_specs) > 0:
                logger.debug(
                    f"Skipping {node.node_id} since it contains un-joinable specs: "
                    f"{evaluation.unjoinable_linkable_specs}"
                )
                continue

            num_joins_required = len(evaluation.join_recipes)
            logger.info(f"Found candidate with node ID '{node.node_id}' with {num_joins_required} joins required.")

            node_to_evaluation[node] = evaluation

            # Since are evaluating nodes with the lowest cost first, if we find one without requiring any joins, then
            # this is going to be the lowest cost solution.
            if len(evaluation.join_recipes) == 0:
                logger.info("Not evaluating other nodes since we found one that doesn't require joins")

        logger.info(f"Found {len(node_to_evaluation)} candidate measure nodes.")

        if len(node_to_evaluation) > 0:

            cost_function = DefaultCostFunction[SqlDataSetT]()

            node_with_lowest_cost = min(node_to_evaluation, key=cost_function.calculate_cost)
            evaluation = node_to_evaluation[node_with_lowest_cost]
            logger.info(
                "Lowest cost node is:\n"
                + pformat_big_objects(
                    lowest_cost_node=dataflow_dag_as_text(node_with_lowest_cost),
                    evaluation=evaluation,
                    cost=cost_function.calculate_cost(node_with_lowest_cost),
                )
            )

            # Nodes containing the linkable instances will be joined to the node containing the measure, so these
            # identifiers will need to be present in the measure node.
            required_local_identifier_specs = tuple(x.join_on_identifier for x in evaluation.join_recipes)
            # Same thing with partitions.
            required_local_dimension_specs = tuple(
                y.start_node_dimension_spec for x in evaluation.join_recipes for y in x.join_on_partition_dimensions
            )
            required_local_time_dimension_specs = tuple(
                y.start_node_time_dimension_spec
                for x in evaluation.join_recipes
                for y in x.join_on_partition_time_dimensions
            )

            return MeasureRecipe(
                measure_node=node_with_lowest_cost,
                required_local_linkable_specs=(
                    evaluation.local_linkable_specs
                    + required_local_identifier_specs
                    + required_local_dimension_specs
                    + required_local_time_dimension_specs
                ),
                join_linkable_instances_recipes=node_to_evaluation[node_with_lowest_cost].join_recipes,
            )

        logger.error("No recipe could be constructed.")
        return None

    def build_computed_metrics_node(
        self,
        metric_spec: MetricSpec,
        aggregated_measures_node: Union[AggregateMeasuresNode[SqlDataSetT], BaseOutput[SqlDataSetT]],
    ) -> ComputeMetricsNode[SqlDataSetT]:
        """Builds a ComputeMetricsNode from aggregated measures."""

        return ComputeMetricsNode[SqlDataSetT](
            parent_node=aggregated_measures_node,
            metric_specs=[metric_spec],
        )

    def build_aggregated_measures(
        self,
        measure_specs: Tuple[MeasureSpec, ...],
        queried_linkable_specs: LinkableSpecSet,
        where_constraint: Optional[SpecWhereClauseConstraint] = None,
        time_range_constraint: Optional[TimeRangeConstraint] = None,
        cumulative: Optional[bool] = False,
        cumulative_window: Optional[CumulativeMetricWindow] = None,
        cumulative_grain_to_date: Optional[TimeGranularity] = None,
    ) -> BaseOutput[SqlDataSetT]:
        """Returns a node where the measures are aggregated by the linkable specs and constrained appropriately.

        This might be a node representing a single aggregation over one data source, or a node representing
        a composite set of aggregations originating from multiple data sources, and joined into a single
        aggregated set of measures.
        """
        output_nodes: List[BaseOutput[SqlDataSetT]] = []
        data_sources_for_measures = collections.defaultdict(list)
        for measure_spec in measure_specs:
            data_source_names = [
                dsource.name
                for dsource in self._data_source_semantics.get_data_sources_for_measure(
                    measure_reference=measure_spec.as_reference
                )
            ]
            assert (
                len(data_source_names) == 1
            ), f"Validation should enforce one data source per measure, but found {data_source_names} for {measure_spec}!"
            data_sources_for_measures[data_source_names[0]].append(measure_spec)

        for data_source, measures in data_sources_for_measures.items():
            logger.info(f"Building aggregated measures for {data_source}. Measures: {measures}")
            node = self._build_aggregated_measures_for_single_data_source(
                measure_specs=tuple(measures),
                queried_linkable_specs=queried_linkable_specs,
                where_constraint=where_constraint,
                time_range_constraint=time_range_constraint,
                cumulative=cumulative,
                cumulative_window=cumulative_window,
                cumulative_grain_to_date=cumulative_grain_to_date,
            )
            output_nodes.append(node)

        if len(output_nodes) == 1:
            return output_nodes[0]
        else:
            return FilterElementsNode(
                parent_node=JoinAggregatedMeasuresByGroupByColumnsNode(parent_nodes=output_nodes),
                include_specs=LinkableInstanceSpec.merge(queried_linkable_specs.as_tuple, measure_specs),
            )

    @staticmethod
    def _add_where_and_aggregate(
        measure_specs: Tuple[MeasureSpec, ...],
        queried_linkable_specs: LinkableSpecSet,
        unaggregated_node: BaseOutput[SqlDataSetT],
        where_constraint: SpecWhereClauseConstraint,
    ) -> BaseOutput[SqlDataSetT]:
        """Applies the where constraint appropriately and produce aggregated measures.

        Assumes that the input node has all measure specs, queried_linkable_specs, and also the specs required by the
        where clause.
        """
        where_constrained_node = WhereConstraintNode(
            parent_node=unaggregated_node,
            where_constraint=where_constraint,
        )

        if where_constraint.linkable_spec_set.is_subset_of(queried_linkable_specs):
            return AggregateMeasuresNode[SqlDataSetT](where_constrained_node)

        # At this point, it's the case that the linkable specs in the where clause are not a subset of the queried
        # linkable specs. A filter is needed after a where clause so that the linkable specs in the where clause don't
        # show up in the final result.
        #
        # e.g. for "bookings" by "ds" where "is_instant", "is_instant" should not be in the results.
        filtered_node = FilterElementsNode(
            parent_node=where_constrained_node,
            include_specs=measure_specs + queried_linkable_specs.as_tuple,
        )

        return AggregateMeasuresNode[SqlDataSetT](parent_node=filtered_node)

    def _build_aggregated_measures_for_single_data_source(
        self,
        measure_specs: Tuple[MeasureSpec, ...],
        queried_linkable_specs: LinkableSpecSet,
        where_constraint: Optional[SpecWhereClauseConstraint] = None,
        time_range_constraint: Optional[TimeRangeConstraint] = None,
        cumulative: Optional[bool] = False,
        cumulative_window: Optional[CumulativeMetricWindow] = None,
        cumulative_grain_to_date: Optional[TimeGranularity] = None,
    ) -> BaseOutput[SqlDataSetT]:
        metric_time_dimension_requested = self._metric_time_dimension_reference.element_name in [
            linkable_spec.element_name for linkable_spec in queried_linkable_specs.as_tuple
        ]
        cumulative_metric_adjusted_time_constraint: Optional[TimeRangeConstraint] = None
        if cumulative and time_range_constraint is not None:
            logger.info(f"Time range constraint before adjustment is {time_range_constraint}")
            granularity: Optional[TimeGranularity] = None
            count = 0
            if cumulative_window is not None:
                granularity = cumulative_window.granularity
                count = cumulative_window.count
            elif cumulative_grain_to_date is not None:
                count = 1
                granularity = cumulative_grain_to_date

            cumulative_metric_adjusted_time_constraint = (
                time_range_constraint.adjust_time_constraint_for_cumulative_metric(granularity, count)
            )
            logger.info(f"Adjusted time range constraint {cumulative_metric_adjusted_time_constraint}")

        required_linkable_specs = (
            LinkableSpecSet.merge((queried_linkable_specs, where_constraint.linkable_spec_set))
            if where_constraint
            else queried_linkable_specs
        )

        logger.info(
            f"Looking for a recipe to get:\n"
            f"{pformat_big_objects(measure_specs=measure_specs, required_linkable_set=required_linkable_specs)}"
        )

        find_recipe_start_time = time.time()
        measure_recipe = self._find_measure_recipe(
            measure_specs=measure_specs,
            time_range_constraint=cumulative_metric_adjusted_time_constraint or time_range_constraint,
            linkable_specs=required_linkable_specs.as_tuple,
        )
        logger.info(
            f"With {len(self._source_nodes)} source nodes, finding a recipe took "
            f"{time.time() - find_recipe_start_time:.2f}s"
        )

        logger.info(f"Using recipe:\n{pformat_big_objects(measure_recipe=measure_recipe)}")

        if not measure_recipe:
            # TODO: Improve for better user understandability.
            raise UnableToSatisfyQueryError(
                f"Recipe not found for measure specs: {measure_specs} and linkable specs: {required_linkable_specs}"
            )

        # Only get the required measure and the local linkable instances so that aggregations work correctly.
        filtered_measure_source_node = FilterElementsNode[SqlDataSetT](
            parent_node=measure_recipe.measure_node,
            include_specs=InstanceSpec.merge(measure_specs, measure_recipe.required_local_linkable_specs),
        )

        time_range_node: Optional[JoinOverTimeRangeNode[SqlDataSetT]] = None
        if cumulative:
            time_range_node = JoinOverTimeRangeNode(
                parent_node=filtered_measure_source_node,
                metric_time_dimension_reference=self._metric_time_dimension_reference,
                window=cumulative_window,
                grain_to_date=cumulative_grain_to_date,
                time_range_constraint=time_range_constraint,
            )

        filtered_measure_or_time_range_node = time_range_node or filtered_measure_source_node
        join_targets = []

        for join_recipe in measure_recipe.join_linkable_instances_recipes:
            # Figure out what elements to filter from the joined node.

            # Sanity check - all linkable specs should have a link, or else why would we be joining them.
            assert all([len(x.identifier_links) > 0 for x in join_recipe.satisfiable_linkable_specs])

            # If we're joining something in, then we need the associated identifier and partitions.
            include_specs: List[LinkableInstanceSpec] = [
                x.identifier_links[0] for x in join_recipe.satisfiable_linkable_specs
            ]
            include_specs.extend([x.node_to_join_dimension_spec for x in join_recipe.join_on_partition_dimensions])
            include_specs.extend(
                [x.node_to_join_time_dimension_spec for x in join_recipe.join_on_partition_time_dimensions]
            )

            # satisfiable_linkable_specs describes what can be satisfied after the join, so remove the identifier
            # link when filtering before the join.
            # e.g. if the node is used to satisfy "user_id__country", then the node must have the identifier
            # "user_id" and the "country" dimension so that it can be joined to the measure node.
            include_specs.extend([x.without_first_identifier_link() for x in join_recipe.satisfiable_linkable_specs])
            filtered_node_to_join = FilterElementsNode[SqlDataSetT](
                parent_node=join_recipe.node_to_join,
                include_specs=include_specs,
            )
            join_targets.append(
                JoinDescription(
                    join_node=filtered_node_to_join,
                    join_on_identifier=join_recipe.join_on_identifier,
                    join_on_partition_dimensions=join_recipe.join_on_partition_dimensions,
                    join_on_partition_time_dimensions=join_recipe.join_on_partition_time_dimensions,
                )
            )

        unaggregated_measure_node: BaseOutput[SqlDataSetT]
        if len(join_targets) > 0:
            filtered_measures_with_joined_elements = JoinToBaseOutputNode[SqlDataSetT](
                parent_node=filtered_measure_or_time_range_node,
                join_targets=join_targets,
            )

            specs_to_keep_after_join: List[InstanceSpec] = list(measure_specs)
            specs_to_keep_after_join.extend(required_linkable_specs.as_tuple)

            after_join_filtered_node = FilterElementsNode[SqlDataSetT](
                parent_node=filtered_measures_with_joined_elements,
                include_specs=specs_to_keep_after_join,
            )
            unaggregated_measure_node = after_join_filtered_node
        else:
            unaggregated_measure_node = filtered_measure_or_time_range_node

        cumulative_metric_constrained_node: Optional[ConstrainTimeRangeNode] = None
        if (
            cumulative_metric_adjusted_time_constraint is not None
            and time_range_constraint is not None
            and metric_time_dimension_requested
        ):
            cumulative_metric_constrained_node = ConstrainTimeRangeNode(
                unaggregated_measure_node, time_range_constraint
            )

        if where_constraint:
            return DataflowPlanBuilder._add_where_and_aggregate(
                measure_specs=measure_specs,
                unaggregated_node=cumulative_metric_constrained_node or unaggregated_measure_node,
                queried_linkable_specs=queried_linkable_specs,
                where_constraint=where_constraint,
            )

        return AggregateMeasuresNode[SqlDataSetT](
            parent_node=cumulative_metric_constrained_node or unaggregated_measure_node
        )
