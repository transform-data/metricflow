import logging

from metricflow.dataflow.builder.costing import DefaultCostFunction, DefaultCost
from metricflow.dataflow.dataflow_plan import (
    FilterElementsNode,
    AggregateMeasuresNode,
    JoinToBaseOutputNode,
    JoinDescription,
)
from metricflow.dataset.data_source_adapter import DataSourceDataSet
from metricflow.specs import (
    MeasureSpec,
    IdentifierSpec,
    DimensionSpec,
    LinklessIdentifierSpec,
    MetricInputMeasureSpec,
)
from metricflow.test.fixtures.model_fixtures import ConsistentIdObjectRepository

logger = logging.getLogger(__name__)


def test_costing(consistent_id_object_repository: ConsistentIdObjectRepository) -> None:  # noqa: D
    bookings_node = consistent_id_object_repository.simple_model_read_nodes["bookings_source"]
    listings_node = consistent_id_object_repository.simple_model_read_nodes["listings_latest"]

    bookings_spec = MeasureSpec(
        element_name="bookings",
    )
    bookings_filtered = FilterElementsNode[DataSourceDataSet](
        parent_node=bookings_node,
        include_specs=[
            bookings_spec,
            IdentifierSpec(
                element_name="listing",
                identifier_links=(),
            ),
        ],
    )

    listings_filtered = FilterElementsNode[DataSourceDataSet](
        parent_node=listings_node,
        include_specs=[
            DimensionSpec(
                element_name="country_latest",
                identifier_links=(),
            ),
            IdentifierSpec(
                element_name="listing",
                identifier_links=(),
            ),
        ],
    )

    join_node = JoinToBaseOutputNode[DataSourceDataSet](
        parent_node=bookings_filtered,
        join_targets=[
            JoinDescription(
                join_node=listings_filtered,
                join_on_identifier=LinklessIdentifierSpec.from_element_name("listing"),
                join_on_partition_dimensions=(),
                join_on_partition_time_dimensions=(),
            )
        ],
    )

    bookings_aggregated = AggregateMeasuresNode[DataSourceDataSet](
        parent_node=join_node, metric_input_measure_specs=(MetricInputMeasureSpec(measure_spec=bookings_spec),)
    )

    cost_function = DefaultCostFunction[DataSourceDataSet]()
    cost = cost_function.calculate_cost(bookings_aggregated)

    assert cost == DefaultCost(num_joins=1, num_aggregations=1)
