from metricflow.instances import InstanceSet
from metricflow.model.semantic_model import SemanticModel
from metricflow.plan_conversion.column_resolver import DefaultColumnAssociationResolver
from metricflow.plan_conversion.instance_converters import (
    CreateSelectColumnsWithMeasuresAggregated,
    FilterElements,
)
from metricflow.plan_conversion.select_column_gen import SelectColumnSet
from metricflow.specs import MeasureSpec, MetricInputMeasureSpec
from metricflow.sql.sql_exprs import (
    SqlFunction,
    SqlFunctionExpression,
)
from metricflow.test.fixtures.model_fixtures import ConsistentIdObjectRepository

__SOURCE_TABLE_ALIAS = "a"


def __get_filtered_measure_instance_set(
    data_source_name: str, measure_name: str, object_repo: ConsistentIdObjectRepository
) -> InstanceSet:
    """Gets an InstanceSet consisting of only the measure instance matching the given name and data source"""
    dataset = object_repo.simple_model_data_sets[data_source_name]
    instance_set = dataset.instance_set
    include_specs = [
        instance.spec for instance in instance_set.measure_instances if instance.spec.element_name == measure_name
    ]
    return FilterElements(include_specs=include_specs).transform(instance_set)


def test_sum_aggregation(
    consistent_id_object_repository: ConsistentIdObjectRepository,
    simple_semantic_model: SemanticModel,
) -> None:
    """Checks for function expression handling for booking_value, a SUM type metric in the simple model"""
    measure_name = "booking_value"
    instance_set = __get_filtered_measure_instance_set("bookings_source", measure_name, consistent_id_object_repository)

    select_column_set: SelectColumnSet = CreateSelectColumnsWithMeasuresAggregated(
        __SOURCE_TABLE_ALIAS,
        DefaultColumnAssociationResolver(simple_semantic_model),
        simple_semantic_model.data_source_semantics,
        (MetricInputMeasureSpec(measure_spec=MeasureSpec(element_name="booking_value")),),
    ).transform(instance_set=instance_set)

    assert len(select_column_set.measure_columns) == 1
    measure_column = select_column_set.measure_columns[0]
    expr = measure_column.expr
    assert isinstance(expr, SqlFunctionExpression)
    assert expr.sql_function == SqlFunction.SUM


def test_sum_boolean_aggregation(
    consistent_id_object_repository: ConsistentIdObjectRepository,
    simple_semantic_model: SemanticModel,
) -> None:
    """Checks for function expression handling for instant_bookings, a SUM_BOOLEAN type metric in the simple model"""
    measure_name = "instant_bookings"
    instance_set = __get_filtered_measure_instance_set("bookings_source", measure_name, consistent_id_object_repository)

    select_column_set: SelectColumnSet = CreateSelectColumnsWithMeasuresAggregated(
        __SOURCE_TABLE_ALIAS,
        DefaultColumnAssociationResolver(simple_semantic_model),
        simple_semantic_model.data_source_semantics,
        (MetricInputMeasureSpec(measure_spec=MeasureSpec(element_name="instant_bookings")),),
    ).transform(instance_set=instance_set)

    assert len(select_column_set.measure_columns) == 1
    measure_column = select_column_set.measure_columns[0]
    expr = measure_column.expr
    assert isinstance(expr, SqlFunctionExpression)
    # The SUM_BOOLEAN aggregation type is transformed to SUM at model parsing time
    assert expr.sql_function == SqlFunction.SUM


def test_avg_aggregation(
    consistent_id_object_repository: ConsistentIdObjectRepository,
    simple_semantic_model: SemanticModel,
) -> None:
    """Checks for function expression handling for average_booking_value, an AVG type metric in the simple model"""
    measure_name = "average_booking_value"
    instance_set = __get_filtered_measure_instance_set("bookings_source", measure_name, consistent_id_object_repository)

    select_column_set: SelectColumnSet = CreateSelectColumnsWithMeasuresAggregated(
        __SOURCE_TABLE_ALIAS,
        DefaultColumnAssociationResolver(simple_semantic_model),
        simple_semantic_model.data_source_semantics,
        (MetricInputMeasureSpec(measure_spec=MeasureSpec(element_name="average_booking_value")),),
    ).transform(instance_set=instance_set)

    assert len(select_column_set.measure_columns) == 1
    measure_column = select_column_set.measure_columns[0]
    expr = measure_column.expr
    assert isinstance(expr, SqlFunctionExpression)
    assert expr.sql_function == SqlFunction.AVERAGE


def test_count_distinct_aggregation(
    consistent_id_object_repository: ConsistentIdObjectRepository,
    simple_semantic_model: SemanticModel,
) -> None:
    """Checks for function expression handling for bookers, a COUNT_DISTINCT type metric in the simple model"""
    measure_name = "bookers"
    instance_set = __get_filtered_measure_instance_set("bookings_source", measure_name, consistent_id_object_repository)

    select_column_set: SelectColumnSet = CreateSelectColumnsWithMeasuresAggregated(
        __SOURCE_TABLE_ALIAS,
        DefaultColumnAssociationResolver(simple_semantic_model),
        simple_semantic_model.data_source_semantics,
        (MetricInputMeasureSpec(measure_spec=MeasureSpec(element_name="bookers")),),
    ).transform(instance_set=instance_set)

    assert len(select_column_set.measure_columns) == 1
    measure_column = select_column_set.measure_columns[0]
    expr = measure_column.expr
    assert isinstance(expr, SqlFunctionExpression)
    assert expr.sql_function == SqlFunction.COUNT_DISTINCT


def test_max_aggregation(
    consistent_id_object_repository: ConsistentIdObjectRepository,
    simple_semantic_model: SemanticModel,
) -> None:
    """Checks for function expression handling for largest_listing, a MAX type metric in the simple model"""
    measure_name = "largest_listing"
    instance_set = __get_filtered_measure_instance_set("listings_latest", measure_name, consistent_id_object_repository)

    select_column_set: SelectColumnSet = CreateSelectColumnsWithMeasuresAggregated(
        __SOURCE_TABLE_ALIAS,
        DefaultColumnAssociationResolver(simple_semantic_model),
        simple_semantic_model.data_source_semantics,
        (MetricInputMeasureSpec(measure_spec=MeasureSpec(element_name="largest_listing")),),
    ).transform(instance_set=instance_set)

    assert len(select_column_set.measure_columns) == 1
    measure_column = select_column_set.measure_columns[0]
    expr = measure_column.expr
    assert isinstance(expr, SqlFunctionExpression)
    assert expr.sql_function == SqlFunction.MAX


def test_min_aggregation(
    consistent_id_object_repository: ConsistentIdObjectRepository,
    simple_semantic_model: SemanticModel,
) -> None:
    """Checks for function expression handling for smallest_listing, a MIN type metric in the simple model"""
    measure_name = "smallest_listing"
    instance_set = __get_filtered_measure_instance_set("listings_latest", measure_name, consistent_id_object_repository)

    select_column_set: SelectColumnSet = CreateSelectColumnsWithMeasuresAggregated(
        __SOURCE_TABLE_ALIAS,
        DefaultColumnAssociationResolver(simple_semantic_model),
        simple_semantic_model.data_source_semantics,
        (MetricInputMeasureSpec(measure_spec=MeasureSpec(element_name="smallest_listing")),),
    ).transform(instance_set=instance_set)

    assert len(select_column_set.measure_columns) == 1
    measure_column = select_column_set.measure_columns[0]
    expr = measure_column.expr
    assert isinstance(expr, SqlFunctionExpression)
    assert expr.sql_function == SqlFunction.MIN


def test_aliased_sum(
    consistent_id_object_repository: ConsistentIdObjectRepository,
    simple_semantic_model: SemanticModel,
) -> None:
    """Checks for function expression handling for booking_value, a SUM type metric in the simple model, with an alias"""
    measure_name = "booking_value"
    instance_set = __get_filtered_measure_instance_set("bookings_source", measure_name, consistent_id_object_repository)

    select_column_set: SelectColumnSet = CreateSelectColumnsWithMeasuresAggregated(
        __SOURCE_TABLE_ALIAS,
        DefaultColumnAssociationResolver(simple_semantic_model),
        simple_semantic_model.data_source_semantics,
        (MetricInputMeasureSpec(measure_spec=MeasureSpec(element_name="booking_value"), alias="bvalue"),),
    ).transform(instance_set=instance_set)

    assert len(select_column_set.measure_columns) == 1
    measure_column = select_column_set.measure_columns[0]
    expr = measure_column.expr
    assert isinstance(expr, SqlFunctionExpression)
    assert expr.sql_function == SqlFunction.SUM
    assert measure_column.column_alias == "bvalue"
