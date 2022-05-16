import pytest

from metricflow.model.model_validator import ModelValidator
from metricflow.model.objects.data_source import DataSource, Mutability, MutabilityType
from metricflow.model.objects.elements.dimension import Dimension, DimensionType, DimensionTypeParams
from metricflow.model.objects.elements.measure import Measure, AggregationType
from metricflow.model.objects.metric import Metric, MetricType, MetricTypeParams
from metricflow.model.objects.user_configured_model import UserConfiguredModel
from metricflow.model.validations.validator_helpers import ModelValidationException
from metricflow.time.time_granularity import TimeGranularity


@pytest.mark.skip("TODO: Will convert to validation rule")
def test_inconsistent_elements() -> None:  # noqa:D
    dim_name = "ename"
    measure_name = "ename"
    with pytest.raises(ModelValidationException):
        ModelValidator().checked_validations(
            UserConfiguredModel(
                data_sources=[
                    DataSource(
                        name="s1",
                        sql_query="SELECT foo FROM bar",
                        dimensions=[
                            Dimension(
                                name=dim_name,
                                type=DimensionType.TIME,
                                type_params=DimensionTypeParams(
                                    time_granularity=TimeGranularity.DAY,
                                ),
                            )
                        ],
                        mutability=Mutability(type=MutabilityType.IMMUTABLE),
                    ),
                    DataSource(
                        name="s2",
                        sql_query="SELECT foo FROM bar",
                        measures=[Measure(name=measure_name, agg=AggregationType.SUM)],
                        mutability=Mutability(type=MutabilityType.IMMUTABLE),
                    ),
                ],
                metrics=[
                    Metric(
                        name=measure_name,
                        type=MetricType.MEASURE_PROXY,
                        type_params=MetricTypeParams(measures=[measure_name]),
                    )
                ],
                materializations=[],
            )
        )
