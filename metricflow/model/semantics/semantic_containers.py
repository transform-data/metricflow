from __future__ import annotations

import logging
from collections import defaultdict
from copy import deepcopy
from typing import Dict, List, Set, Optional, Sequence, Tuple, FrozenSet

from metricflow.errors.errors import (
    DuplicateMetricError,
    MetricNotFoundError,
    NonExistentMeasureError,
    InvalidDataSourceError,
)
from metricflow.instances import DataSourceReference, DataSourceElementReference
from metricflow.model.objects.data_source import DataSource, DataSourceOrigin
from metricflow.model.objects.elements.dimension import Dimension
from metricflow.model.objects.elements.identifier import Identifier
from metricflow.model.objects.elements.measure import Measure, AggregationType
from metricflow.model.objects.metric import Metric, MetricType
from metricflow.model.objects.user_configured_model import UserConfiguredModel
from metricflow.model.semantics.data_source_container import PydanticDataSourceContainer
from metricflow.model.semantics.linkable_spec_resolver import (
    ValidLinkableSpecResolver,
    LinkableElementProperties,
)
from metricflow.specs import (
    LinkableInstanceSpec,
    LinkableElementReference,
    MeasureSpec,
    MeasureReference,
    DimensionReference,
    IdentifierReference,
    MetricInputMeasureSpec,
    MetricSpec,
    TimeDimensionReference,
)

logger = logging.getLogger(__name__)

MAX_JOIN_HOPS = 2


class MetricSemantics:  # noqa: D
    def __init__(  # noqa: D
        self, user_configured_model: UserConfiguredModel, data_source_semantics: DataSourceSemantics
    ) -> None:
        self._user_configured_model = user_configured_model
        self._metrics: Dict[MetricSpec, Metric] = {}
        self._data_source_semantics = data_source_semantics

        # Dict from the name of the metric to the hash.
        self._metric_hashes: Dict[MetricSpec, str] = {}

        for metric in self._user_configured_model.metrics:
            self.add_metric(metric)

        self._linkable_spec_resolver = ValidLinkableSpecResolver(
            user_configured_model=self._user_configured_model,
            max_identifier_links=MAX_JOIN_HOPS,
        )

    def element_specs_for_metrics(
        self,
        metric_specs: List[MetricSpec],
        with_any_property: FrozenSet[LinkableElementProperties] = LinkableElementProperties.all_properties(),
        without_any_property: FrozenSet[LinkableElementProperties] = frozenset(),
    ) -> List[LinkableInstanceSpec]:
        """Dimensions common to all metrics requested (intersection)"""

        all_linkable_specs = self._linkable_spec_resolver.get_linkable_elements_for_metrics(
            metric_specs=metric_specs,
            with_any_of=with_any_property,
            without_any_of=without_any_property,
        ).as_spec_set

        return sorted(all_linkable_specs.as_tuple, key=lambda x: x.qualified_name)

    def get_metrics(self, metric_names: List[MetricSpec]) -> List[Metric]:  # noqa: D
        res = []
        for metric_name in metric_names:
            if metric_name not in self._metrics:
                raise MetricNotFoundError(f"Unable to find metric `{metric_name}`. Perhaps it has not been registered")
            res.append(self._metrics[metric_name])

        return res

    @property
    def metric_names(self) -> List[MetricSpec]:  # noqa: D
        return list(self._metrics.keys())

    def get_metric(self, metric_name: MetricSpec) -> Metric:  # noqa:D
        if metric_name not in self._metrics:
            raise MetricNotFoundError(f"Unable to find metric `{metric_name}`. Perhaps it has not been registered")
        return self._metrics[metric_name]

    def add_metric(self, metric: Metric) -> None:
        """Add metric, validating presence of required measures"""
        metric_spec = MetricSpec(element_name=metric.name)
        if metric_spec in self._metrics:
            raise DuplicateMetricError(f"Metric `{metric.name}` has already been registered")
        for measure_reference in metric.measure_references:
            if measure_reference not in self._data_source_semantics.measure_references:
                raise NonExistentMeasureError(
                    f"Metric `{metric.name}` references measure `{measure_reference}` which has not been registered"
                )
        self._metrics[metric_spec] = metric
        self._metric_hashes[metric_spec] = metric.definition_hash

    @property
    def valid_hashes(self) -> Set[str]:
        """Return all of the hashes of the metric definitions."""
        return set(self._metric_hashes.values())

    def measures_for_metric(self, metric_spec: MetricSpec) -> Tuple[MetricInputMeasureSpec, ...]:
        """Return the measure specs required to compute the metric."""
        metric = self.get_metric(metric_spec)
        input_measure_specs: List[MetricInputMeasureSpec] = []

        for input_measure in metric.input_measures:
            spec_constraint = (
                input_measure.constraint.to_spec_where_constraint(
                    data_source_semantics=self._data_source_semantics,
                )
                if input_measure.constraint is not None
                else None
            )
            spec = MetricInputMeasureSpec(
                measure_spec=MeasureSpec.from_reference(input_measure.measure_reference),
                constraint=spec_constraint,
                alias=input_measure.alias,
            )
            input_measure_specs.append(spec)

        return tuple(input_measure_specs)

    def contains_cumulative_metric(self, metric_specs: Sequence[MetricSpec]) -> bool:
        """Returns true if any of the specs correspond to a cumulative metric."""
        for metric_spec in metric_specs:
            if self.get_metric(metric_spec).type == MetricType.CUMULATIVE:
                return True
        return False


class DataSourceSemantics:
    """Tracks semantic information for data source held in a set of DataSourceContainers"""

    def __init__(  # noqa: D
        self,
        model: UserConfiguredModel,
        configured_data_source_container: PydanticDataSourceContainer,
    ) -> None:
        self._model = model
        self._measure_index: Dict[MeasureReference, List[DataSource]] = defaultdict(list)
        self._measure_aggs: Dict[
            MeasureReference, AggregationType
        ] = {}  # maps measures to their one consistent aggregation
        self._dimension_index: Dict[DimensionReference, List[DataSource]] = defaultdict(list)
        self._linkable_reference_index: Dict[LinkableElementReference, List[DataSource]] = defaultdict(list)
        self._entity_index: Dict[Optional[str], List[DataSource]] = defaultdict(list)
        self._identifier_ref_to_entity: Dict[IdentifierReference, Optional[str]] = {}
        self._data_source_names: Set[str] = set()

        self._configured_data_source_container = configured_data_source_container
        self._data_source_to_aggregation_time_dimensions: Dict[DataSourceReference, List[TimeDimensionReference]] = {}

        # Add semantic tracking for data sources from configured_data_source_container
        for data_source in self._configured_data_source_container.values():
            assert isinstance(data_source, DataSource)
            self.add_configured_data_source(data_source)

    def add_configured_data_source(self, data_source: DataSource) -> None:
        """Dont use this unless you mean it (ie in tests). The configured data sources are supposed to be static"""
        self._configured_data_source_container._put(data_source)
        self._add_data_source(data_source)

    def get_dimension_references(self) -> List[DimensionReference]:  # noqa: D
        return list(self._dimension_index.keys())

    def get_dimension(
        self, dimension_reference: DimensionReference, origin: Optional[DataSourceOrigin] = None
    ) -> Dimension:
        """Retrieves a full dimension object by name"""
        for dimension_source in self._dimension_index[dimension_reference]:
            if origin and dimension_source.origin != origin:
                continue
            dimension = dimension_source.get_dimension(dimension_reference)
            # find the data source that has the requested dimension by the requested identifier

            return deepcopy(dimension)

        raise ValueError(
            f"Could not find dimension with name ({dimension_reference.element_name}) in configured data sources"
        )

    def get_time_dimension(self, time_dimension_reference: TimeDimensionReference) -> Dimension:
        """Retrieves a full dimension object by name"""
        dimension_reference = time_dimension_reference.dimension_reference()

        if dimension_reference not in self._dimension_index:
            raise ValueError(
                f"Could not find dimension with name ({dimension_reference.element_name}) in configured data sources"
            )

        for dimension_source in self._dimension_index[dimension_reference]:
            dimension = dimension_source.get_dimension(dimension_reference)
            # TODO: Unclear if the deepcopy is necessary.
            return deepcopy(dimension)

        assert False, f"{time_dimension_reference} should have been in the dimension index"

    @property
    def measure_references(self) -> List[MeasureReference]:  # noqa: D
        return list(self._measure_index.keys())

    def get_measure(self, measure_reference: MeasureReference) -> Measure:  # noqa: D
        if measure_reference not in self._measure_index:
            raise ValueError(f"Could not find measure with name ({measure_reference}) in configured data sources")

        assert len(self._measure_index[measure_reference]) >= 1
        # Measures should be consistent across data sources, so just use the first one.
        return list(self._measure_index[measure_reference])[0].get_measure(measure_reference)

    def get_identifier_references(self) -> List[IdentifierReference]:  # noqa: D
        return list(self._identifier_ref_to_entity.keys())

    # DSC interface
    def get_data_sources_for_measure(self, measure_reference: MeasureReference) -> List[DataSource]:  # noqa: D
        return self._measure_index[measure_reference]

    def get_identifier_in_data_source(self, ref: DataSourceElementReference) -> Optional[Identifier]:  # Noqa: d
        data_source = self.get(ref.data_source_name)
        if not data_source:
            return None

        for identifier in data_source.identifiers:
            if identifier.reference.element_name == ref.element_name:
                return identifier

        return None

    def get(self, data_source_name: str) -> Optional[DataSource]:  # noqa: D
        if data_source_name in self._configured_data_source_container:
            data_source = self._configured_data_source_container.get(data_source_name)
            assert isinstance(data_source, DataSource)
            return data_source

        return None

    def get_by_reference(self, data_source_reference: DataSourceReference) -> Optional[DataSource]:  # noqa: D
        return self.get(data_source_reference.data_source_name)

    def _add_data_source(
        self,
        data_source: DataSource,
        fail_on_error: bool = True,
        logging_context: str = "",
    ) -> None:
        """Add data source semantic information, validating consistency with existing data sources."""
        errors = []

        if data_source.name in self._data_source_names:
            errors.append(f"name {data_source.name} already registered - please ensure data source names are unique")

        for measure in data_source.measures:
            if measure.reference in self._measure_aggs and self._measure_aggs[measure.reference] != measure.agg:
                errors.append(
                    f"conflicting aggregation (agg) for measure `{measure.reference.element_name}` registered as "
                    f"`{self._measure_aggs[measure.reference]}`; Got `{measure.agg}"
                )

        if errors:
            error_prefix = "\n  - "
            error_msg = (
                f"Unable to add data source `{data_source.name}` "
                f"{'while ' + logging_context + ' ' if logging_context else ''}"
                f"{'... skipping' if not fail_on_error else ''}.\n"
                f"Errors: {error_prefix + error_prefix.join(errors)}"
            )
            if fail_on_error:
                raise InvalidDataSourceError(error_msg)
            logger.warning(error_msg)
            return

        self._data_source_names.add(data_source.name)

        self._data_source_to_aggregation_time_dimensions[data_source.reference] = []

        for measure in data_source.measures:
            self._measure_aggs[measure.reference] = measure.agg
            self._measure_index[measure.reference].append(data_source)
            if (
                measure.checked_agg_time_dimension
                not in self._data_source_to_aggregation_time_dimensions[data_source.reference]
            ):
                self._data_source_to_aggregation_time_dimensions[data_source.reference].append(
                    measure.checked_agg_time_dimension
                )
        for dim in data_source.dimensions:
            self._linkable_reference_index[dim.reference].append(data_source)
            self._dimension_index[dim.reference].append(data_source)
        for ident in data_source.identifiers:
            self._identifier_ref_to_entity[ident.reference] = ident.entity
            self._entity_index[ident.entity].append(data_source)
            self._linkable_reference_index[ident.reference].append(data_source)

    @property
    def data_source_references(self) -> Sequence[DataSourceReference]:  # noqa: D
        data_source_names_sorted = sorted(self._data_source_names)
        return tuple(DataSourceReference(data_source_name=x) for x in data_source_names_sorted)

    def get_aggregation_time_dimensions(
        self, data_source_reference: DataSourceReference
    ) -> Sequence[TimeDimensionReference]:
        """Return all time dimensions used for measure aggregation in a data source."""
        assert (
            data_source_reference in self._data_source_to_aggregation_time_dimensions
        ), f"Data Source {data_source_reference} is not known"
        return self._data_source_to_aggregation_time_dimensions[data_source_reference]
