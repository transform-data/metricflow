import pytest

from metricflow.model.model_validator import ModelValidator
from metricflow.model.objects.data_source import Mutability, MutabilityType
from metricflow.model.objects.elements.dimension import Dimension, DimensionType, DimensionTypeParams
from metricflow.model.objects.elements.measure import Measure, AggregationType
from metricflow.model.objects.metric import MetricType, MetricTypeParams
from metricflow.model.objects.user_configured_model import UserConfiguredModel
from metricflow.model.validations.validator_helpers import ModelValidationException
from metricflow.specs import MeasureReference, DimensionReference
from metricflow.test.model.validations.helpers import data_source_with_guaranteed_meta, metric_with_guaranteed_meta
from metricflow.time.time_granularity import TimeGranularity


def test_incompatible_dimension_type() -> None:  # noqa:D
    with pytest.raises(ModelValidationException, match=r"type conflict for dimension"):
        dim_reference = DimensionReference(element_name="dim")
        measure_reference = MeasureReference(element_name="measure")
        ModelValidator().checked_validations(
            UserConfiguredModel(
                data_sources=[
                    data_source_with_guaranteed_meta(
                        name="dim1",
                        sql_query=f"SELECT {dim_reference.element_name}, {measure_reference.element_name} FROM bar",
                        measures=[Measure(name=measure_reference, agg=AggregationType.SUM)],
                        dimensions=[
                            Dimension(
                                name=dim_reference,
                                type=DimensionType.TIME,
                                type_params=DimensionTypeParams(
                                    is_primary=True,
                                    time_granularity=TimeGranularity.DAY,
                                ),
                            )
                        ],
                        mutability=Mutability(type=MutabilityType.IMMUTABLE),
                    ),
                    data_source_with_guaranteed_meta(
                        name="categoricaldim",
                        sql_query="SELECT foo FROM bar",
                        dimensions=[Dimension(name=dim_reference, type=DimensionType.CATEGORICAL)],
                        mutability=Mutability(type=MutabilityType.IMMUTABLE),
                    ),
                ],
                metrics=[
                    metric_with_guaranteed_meta(
                        name=measure_reference.element_name,
                        type=MetricType.MEASURE_PROXY,
                        type_params=MetricTypeParams(measures=[measure_reference]),
                    )
                ],
                materializations=[],
            )
        )


def test_incompatible_dimension_is_partition() -> None:  # noqa:D
    with pytest.raises(ModelValidationException, match=r"conflicting is_partition attribute for dimension"):
        dim_ref1 = DimensionReference(element_name="dim1")
        measure_reference = MeasureReference(element_name="measure")
        ModelValidator().checked_validations(
            UserConfiguredModel(
                data_sources=[
                    data_source_with_guaranteed_meta(
                        name="dim1",
                        sql_query=f"SELECT {dim_ref1.element_name}, {measure_reference.element_name} FROM bar",
                        measures=[Measure(name=measure_reference, agg=AggregationType.SUM)],
                        dimensions=[
                            Dimension(
                                name=dim_ref1,
                                type=DimensionType.TIME,
                                is_partition=True,
                                type_params=DimensionTypeParams(
                                    is_primary=True,
                                    time_granularity=TimeGranularity.DAY,
                                ),
                            )
                        ],
                        mutability=Mutability(type=MutabilityType.IMMUTABLE),
                    ),
                    data_source_with_guaranteed_meta(
                        name="dim2",
                        sql_query="SELECT foo1 FROM bar",
                        dimensions=[
                            Dimension(
                                name=dim_ref1,
                                type=DimensionType.TIME,
                                is_partition=False,
                                type_params=DimensionTypeParams(
                                    time_granularity=TimeGranularity.DAY,
                                ),
                            )
                        ],
                        mutability=Mutability(type=MutabilityType.IMMUTABLE),
                    ),
                ],
                metrics=[
                    metric_with_guaranteed_meta(
                        name=measure_reference.element_name,
                        type=MetricType.MEASURE_PROXY,
                        type_params=MetricTypeParams(measures=[measure_reference]),
                    )
                ],
                materializations=[],
            )
        )
