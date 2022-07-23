from __future__ import annotations

import datetime
import logging
import time
from dataclasses import dataclass
from typing import List, Optional, Tuple, Dict, Sequence

import fuzzywuzzy.fuzz
import fuzzywuzzy.process

from metricflow.constraints.time_constraint import TimeRangeConstraint
from metricflow.dataflow.builder.node_data_set import DataflowPlanNodeOutputDataSetResolver
from metricflow.dataflow.dataflow_plan import BaseOutput
from metricflow.dataset.data_source_adapter import DataSourceDataSet
from metricflow.dataset.dataset import DataSet
from metricflow.errors.errors import UnableToSatisfyQueryError
from metricflow.model.objects.constraints.where import WhereClauseConstraint
from metricflow.model.objects.elements.dimension import DimensionType
from metricflow.model.semantic_model import SemanticModel
from metricflow.model.semantics.semantic_containers import DataSourceSemantics
from metricflow.naming.linkable_spec_name import StructuredLinkableSpecName
from metricflow.object_utils import pformat_big_objects
from metricflow.query.query_exceptions import InvalidQueryException
from metricflow.specs import (
    MetricFlowQuerySpec,
    MetricSpec,
    DimensionSpec,
    TimeDimensionSpec,
    IdentifierSpec,
    LinkableInstanceSpec,
    LinklessIdentifierSpec,
    OrderBySpec,
    DimensionReference,
    IdentifierReference,
    OutputColumnNameOverride,
    SpecWhereClauseConstraint,
    LinkableSpecSet,
    TimeDimensionReference,
)
from metricflow.time.time_granularity import TimeGranularity
from metricflow.time.time_granularity_solver import (
    TimeGranularitySolver,
    PartialTimeDimensionSpec,
    RequestTimeGranularityException,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class LinkableInstanceSpecs:
    """All linkable specs."""

    dimension_specs: Tuple[DimensionSpec, ...]
    time_dimension_specs: Tuple[TimeDimensionSpec, ...]
    partial_time_dimension_specs: Tuple[PartialTimeDimensionSpec, ...]
    identifier_specs: Tuple[IdentifierSpec, ...]


class MetricFlowQueryParser:
    """Parse input strings from the user into a metric query specification.

    Definitions:
    element name - the name of an element (measure, dimension, identifier) in a data source, or a metric name.
    qualified name - an element name with prefixes and suffixes added to it that further describe transformations or
    conditions for the element to retrieve. e.g. "ds__month" is the "ds" time dimension at the "month" granularity. Or
    "user_id__country" is the "country" dimension that is retrieved by joining "user_id" to the measure data source.
    """

    # TODO: Separate methods out of MetricFlowQueryParser
    # https://app.asana.com/0/1161293048858925/1201755441255186

    # Prefix to use for indents when logging.
    _INDENT_PREFIX = "  "

    def __init__(  # noqa: D
        self,
        model: SemanticModel,
        source_nodes: Sequence[BaseOutput[DataSourceDataSet]],
        node_output_resolver: DataflowPlanNodeOutputDataSetResolver[DataSourceDataSet],
    ) -> None:
        self._model = model
        self._metric_semantics = model.metric_semantics
        self._data_source_semantics = model.data_source_semantics

        # Set up containers for known element names
        self._known_identifier_element_references = self._data_source_semantics.get_identifier_references()

        self._known_time_dimension_element_references = [DataSet.metric_time_dimension_reference()]
        self._known_dimension_element_references = []
        for dimension_reference in self._data_source_semantics.get_dimension_references():
            dimension = self._data_source_semantics.get_dimension(dimension_reference)
            if dimension.type == DimensionType.CATEGORICAL:
                self._known_dimension_element_references.append(dimension_reference)
            elif dimension.type == DimensionType.TIME:
                self._known_time_dimension_element_references.append(dimension_reference.time_dimension_reference)
            else:
                raise RuntimeError(f"Unhandled linkable type: {dimension.type}")

        self._known_metric_names = set(self._metric_semantics.metric_names)
        self._metric_time_dimension_reference = DataSet.metric_time_dimension_reference()
        self._time_granularity_solver = TimeGranularitySolver(
            semantic_model=self._model,
            source_nodes=source_nodes,
            node_output_resolver=node_output_resolver,
        )

    @staticmethod
    def convert_to_linkable_specs(
        data_source_semantics: DataSourceSemantics, where_constraint_names: List[str]
    ) -> LinkableSpecSet:
        """Processes where_clause_constraint.linkable_names into associated LinkableInstanceSpecs (dims, times, ids)

        where_constraint_names: WhereConstraintClause.linkable_names
        data_source_semantics: DataSourceSemantics from the instantiated class

        output: InstanceSpecSet of Tuple(DimensionSpec), Tuple(TimeDimensionSpec), Tuple(IdentifierSpec)
        """
        where_constraint_dimensions = []
        where_constraint_time_dimensions = []
        where_constraint_identifiers = []
        linkable_spec_names = [
            StructuredLinkableSpecName.from_name(linkable_name) for linkable_name in where_constraint_names
        ]
        dimension_references = {
            dimension_reference.element_name: dimension_reference
            for dimension_reference in data_source_semantics.get_dimension_references()
        }
        identifier_references = {
            identifier_reference.element_name: identifier_reference
            for identifier_reference in data_source_semantics.get_identifier_references()
        }

        for spec_name in linkable_spec_names:
            if spec_name.element_name in dimension_references:
                dimension = data_source_semantics.get_dimension(dimension_references[spec_name.element_name])
                if dimension.type == DimensionType.CATEGORICAL:
                    where_constraint_dimensions.append(DimensionSpec.from_name(spec_name.qualified_name))
                elif dimension.type == DimensionType.TIME:
                    where_constraint_time_dimensions.append(TimeDimensionSpec.from_name(spec_name.qualified_name))
                else:
                    raise RuntimeError(f"Unhandled type: {dimension.type}")
            elif spec_name.element_name in identifier_references:
                where_constraint_identifiers.append(IdentifierSpec.from_name(spec_name.qualified_name))
            else:
                raise InvalidQueryException(f"Unknown element: {spec_name}")

        return LinkableSpecSet(
            dimension_specs=tuple(where_constraint_dimensions),
            time_dimension_specs=tuple(where_constraint_time_dimensions),
            identifier_specs=tuple(where_constraint_identifiers),
        )

    @staticmethod
    def convert_to_spec_where_constraint(
        data_source_semantics: DataSourceSemantics, where_constraint: WhereClauseConstraint
    ) -> SpecWhereClauseConstraint:
        """Converts a where constraint to one using specs."""
        return SpecWhereClauseConstraint(
            where_condition=where_constraint.where,
            linkable_names=tuple(where_constraint.linkable_names),
            linkable_spec_set=MetricFlowQueryParser.convert_to_linkable_specs(
                data_source_semantics=data_source_semantics, where_constraint_names=where_constraint.linkable_names
            ),
            execution_parameters=where_constraint.sql_params,
        )

    @staticmethod
    def _top_fuzzy_matches(
        item: str,
        candidate_items: Sequence[str],
        max_suggestions: int = 6,
        min_score: int = 50,
    ) -> Sequence[str]:
        """Return the top items (by edit distance) in candidate_items that fuzzy matches the given item."""
        # Rank choices by edit distance score.
        # extract() return a tuple like (name, score)
        top_ranked_suggestions = sorted(
            fuzzywuzzy.process.extract(
                # This scorer seems to return the best results.
                item,
                list(candidate_items),
                limit=max_suggestions,
                scorer=fuzzywuzzy.fuzz.token_set_ratio,
            ),
            # Put the highest scoring item at the top of the list.
            key=lambda x: x[1],
            reverse=True,
        )
        # If the scores are too low, then it looks nonsensical, so remove those.
        top_ranked_suggestions = [x for x in top_ranked_suggestions if x[1] > min_score]
        return [x[0] for x in top_ranked_suggestions]

    def parse_and_validate_query(
        self,
        metric_names: Sequence[str],
        group_by_names: Sequence[str],
        limit: Optional[int] = None,
        time_constraint_start: Optional[datetime.datetime] = None,
        time_constraint_end: Optional[datetime.datetime] = None,
        where_constraint: Optional[WhereClauseConstraint] = None,
        where_constraint_str: Optional[str] = None,
        order: Optional[Sequence[str]] = None,
        time_granularity: Optional[TimeGranularity] = None,
    ) -> MetricFlowQuerySpec:
        """Parse the query into spec objects, validating them in the process.

        e.g. make sure that the given metric is a valid metric name.
        """
        start_time = time.time()
        try:
            return self._parse_and_validate_query(
                metric_names=metric_names,
                group_by_names=group_by_names,
                limit=limit,
                time_constraint_start=time_constraint_start,
                time_constraint_end=time_constraint_end,
                where_constraint=where_constraint,
                where_constraint_str=where_constraint_str,
                order=order,
                time_granularity=time_granularity,
            )
        finally:
            logger.info(f"Parsing the query took: {time.time() - start_time:.2f}s")

    def _validate_linkable_specs(
        self,
        metric_specs: Tuple[MetricSpec, ...],
        all_linkable_specs: LinkableInstanceSpecs,
        time_dimension_specs: Tuple[TimeDimensionSpec, ...],
    ) -> None:
        invalid_group_bys = self._get_invalid_linkable_specs(
            metric_specs=metric_specs,
            dimension_specs=all_linkable_specs.dimension_specs,
            time_dimension_specs=time_dimension_specs,
            identifier_specs=all_linkable_specs.identifier_specs,
        )
        if len(invalid_group_bys) > 0:
            valid_group_by_names_for_metrics = sorted(
                x.qualified_name
                for x in self._metric_semantics.element_specs_for_metrics(metric_specs=list(metric_specs))
            )
            # Create suggestions for invalid dimensions in case the user made a typo.
            suggestion_sections = {}
            for invalid_group_by in invalid_group_bys:
                suggestions = MetricFlowQueryParser._top_fuzzy_matches(
                    item=invalid_group_by.qualified_name, candidate_items=valid_group_by_names_for_metrics
                )
                section_key = f"Suggestions for invalid dimension '{invalid_group_by.qualified_name}'"
                section_value = pformat_big_objects(suggestions)
                suggestion_sections[section_key] = section_value
            raise UnableToSatisfyQueryError(
                f"Dimensions {[x.qualified_name for x in invalid_group_bys]} cannot be "
                f"resolved for metrics {[x.qualified_name for x in metric_specs]}. The invalid dimension may not "
                f"exist, require an ambiguous join (e.g. a join path that can be satisfied in multiple ways), "
                f"or require a fanout join.",
                context=suggestion_sections,
            )

    def _parse_and_validate_query(
        self,
        metric_names: Sequence[str],
        group_by_names: Sequence[str],
        limit: Optional[int] = None,
        time_constraint_start: Optional[datetime.datetime] = None,
        time_constraint_end: Optional[datetime.datetime] = None,
        where_constraint: Optional[WhereClauseConstraint] = None,
        where_constraint_str: Optional[str] = None,
        order: Optional[Sequence[str]] = None,
        time_granularity: Optional[TimeGranularity] = None,
    ) -> MetricFlowQuerySpec:
        assert not (
            where_constraint and where_constraint_str
        ), "Both where_constraint and where_constraint_str should not be set"

        parsed_where_constraint: Optional[WhereClauseConstraint]
        if where_constraint_str:
            parsed_where_constraint = WhereClauseConstraint.parse(where_constraint_str)
        else:
            parsed_where_constraint = where_constraint

        metric_specs = self._parse_metric_names(metric_names)

        if time_constraint_start is None:
            time_constraint_start = TimeRangeConstraint.ALL_TIME_BEGIN()
        elif time_constraint_start < TimeRangeConstraint.ALL_TIME_BEGIN():
            logger.warning(
                f"Start time for the supplied time constraint {time_constraint_start.isoformat()} is <less> than the "
                f"minimum allowed ('{TimeRangeConstraint.ALL_TIME_BEGIN().isoformat()}'). Changing to the minimum."
            )
            time_constraint_start = TimeRangeConstraint.ALL_TIME_BEGIN()
        elif time_constraint_start > TimeRangeConstraint.ALL_TIME_END():
            logger.warning(
                f"Start time for the supplied time constraint {time_constraint_start.isoformat()} is > than the "
                f"maximum allowed ('{TimeRangeConstraint.ALL_TIME_END().isoformat()}'). Changing to the maximum."
            )
            time_constraint_start = TimeRangeConstraint.ALL_TIME_END()

        if time_constraint_end is None:
            time_constraint_end = TimeRangeConstraint.ALL_TIME_END()
        elif time_constraint_end > TimeRangeConstraint.ALL_TIME_END():
            logger.warning(
                f"End time for the supplied time constraint {time_constraint_end.isoformat()} is > than the "
                f"maximum allowed ('{TimeRangeConstraint.ALL_TIME_END().isoformat()}'). Changing to the maximum."
            )
            time_constraint_end = TimeRangeConstraint.ALL_TIME_END()
        elif time_constraint_end < TimeRangeConstraint.ALL_TIME_BEGIN():
            logger.warning(
                f"End time for the supplied time constraint {time_constraint_end.isoformat()} is less than the "
                f"minimum allowed ('{TimeRangeConstraint.ALL_TIME_BEGIN().isoformat()}'). Changing to the minimum."
            )
            time_constraint_end = TimeRangeConstraint.ALL_TIME_BEGIN()

        if time_constraint_end < time_constraint_start:
            raise ValueError(
                f"End of time constraint {time_constraint_end.isoformat()} is before start of time constraint "
                f"{time_constraint_start.isoformat()}"
            )

        time_constraint: Optional[TimeRangeConstraint] = TimeRangeConstraint(
            start_time=time_constraint_start, end_time=time_constraint_end
        )
        if time_constraint == TimeRangeConstraint.all_time():
            # If the time constraint is all time, just ignore and not render
            time_constraint = None

        requested_linkable_specs = self._parse_linkable_element_names(group_by_names, metric_specs)
        partial_time_dimension_spec_replacements = (
            self._time_granularity_solver.resolve_granularity_for_partial_time_dimension_specs(
                metric_specs=metric_specs,
                partial_time_dimension_specs=requested_linkable_specs.partial_time_dimension_specs,
                metric_time_dimension_reference=self._metric_time_dimension_reference,
                time_granularity=time_granularity,
            )
        )

        all_group_by_names = list(group_by_names)
        if parsed_where_constraint is not None:
            all_group_by_names += parsed_where_constraint.linkable_names

        time_dimension_specs = requested_linkable_specs.time_dimension_specs + tuple(
            time_dimension_spec for _, time_dimension_spec in partial_time_dimension_spec_replacements.items()
        )

        self._time_granularity_solver.validate_time_granularity(metric_specs, time_dimension_specs)

        order_by_specs = self._parse_order_by(order or [], partial_time_dimension_spec_replacements)

        for metric_spec in metric_specs:
            metric = self._metric_semantics.get_metric(metric_spec)
            if metric.constraint is not None:
                all_linkable_specs = self._parse_linkable_element_names(
                    qualified_linkable_names=all_group_by_names + metric.constraint.linkable_names,
                    metric_specs=(metric_spec,),
                )
                self._validate_linkable_specs(
                    metric_specs=(metric_spec,),
                    all_linkable_specs=all_linkable_specs,
                    time_dimension_specs=time_dimension_specs,
                )
        all_linkable_specs = self._parse_linkable_element_names(
            qualified_linkable_names=all_group_by_names,
            metric_specs=metric_specs,
        )
        self._validate_linkable_specs(
            metric_specs=metric_specs,
            all_linkable_specs=all_linkable_specs,
            time_dimension_specs=time_dimension_specs,
        )

        self._validate_order_by_specs(
            order_by_specs=order_by_specs,
            metric_specs=metric_specs,
            linkable_specs=LinkableSpecSet(
                dimension_specs=requested_linkable_specs.dimension_specs,
                time_dimension_specs=time_dimension_specs,
                identifier_specs=requested_linkable_specs.identifier_specs,
            ),
        )

        # Update constraints to be appropriate for the time granularity of the query.
        if time_constraint:
            logger.info(f"Time constraint before adjustment is {time_constraint}")
            time_constraint = self._adjust_time_range_constraint(
                metric_specs=metric_specs,
                time_dimension_specs=time_dimension_specs,
                time_range_constraint=time_constraint,
            )
            logger.info(f"Time constraint after adjustment is {time_constraint}")

        # In some cases, the old framework does not use dundered suffixes in the output column name for the primary
        # time dimension. We should aim to get rid of this logic.
        output_column_name_overrides = []
        if (
            self._metric_time_dimension_specified_without_granularity(
                requested_linkable_specs.partial_time_dimension_specs
            )
            and not time_granularity
        ):
            if self._metrics_have_same_time_granularities(metric_specs):
                _, replace_with_time_dimension_spec = self._find_replacement_for_metric_time_dimension(
                    partial_time_dimension_spec_replacements
                )
                output_column_name_overrides.append(
                    OutputColumnNameOverride(
                        time_dimension_spec=replace_with_time_dimension_spec,
                        output_column_name=self._metric_time_dimension_reference.element_name,
                    )
                )

        if limit is not None and limit < 0:
            raise InvalidQueryException(f"Limit was specified as {limit}, which is < 0.")

        spec_where_constraint: Optional[SpecWhereClauseConstraint] = None
        if parsed_where_constraint:
            spec_where_constraint = SpecWhereClauseConstraint(
                where_condition=parsed_where_constraint.where,
                linkable_names=tuple(parsed_where_constraint.linkable_names),
                linkable_spec_set=MetricFlowQueryParser.convert_to_linkable_specs(
                    data_source_semantics=self._data_source_semantics,
                    where_constraint_names=parsed_where_constraint.linkable_names,
                ),
                execution_parameters=parsed_where_constraint.sql_params,
            )

        return MetricFlowQuerySpec(
            metric_specs=metric_specs,
            dimension_specs=requested_linkable_specs.dimension_specs,
            identifier_specs=requested_linkable_specs.identifier_specs,
            time_dimension_specs=time_dimension_specs,
            order_by_specs=order_by_specs,
            output_column_name_overrides=tuple(output_column_name_overrides),
            time_range_constraint=time_constraint,
            where_constraint=spec_where_constraint,
            limit=limit,
        )

    def _validate_order_by_specs(
        self,
        order_by_specs: Sequence[OrderBySpec],
        metric_specs: Sequence[MetricSpec],
        linkable_specs: LinkableSpecSet,
    ) -> None:
        """Checks that the order by specs references an item in the query."""

        for order_by_spec in order_by_specs:
            if not (
                order_by_spec.item in metric_specs
                or order_by_spec.item in linkable_specs.dimension_specs
                or order_by_spec.item in linkable_specs.time_dimension_specs
                or order_by_spec.item in linkable_specs.identifier_specs
            ):
                raise InvalidQueryException(f"Order by item {order_by_spec} not in the query")

    def _adjust_time_range_constraint(
        self,
        metric_specs: Sequence[MetricSpec],
        time_dimension_specs: Sequence[TimeDimensionSpec],
        time_range_constraint: TimeRangeConstraint,
    ) -> TimeRangeConstraint:
        """Adjust the time range constraint so that it matches the boundaries of the granularity of the result."""
        self._time_granularity_solver.validate_time_granularity(metric_specs, time_dimension_specs)

        smallest_primary_time_granularity_in_query = self._find_smallest_metric_time_dimension_spec_granularity(
            time_dimension_specs
        )
        if smallest_primary_time_granularity_in_query:
            adjusted_to_granularity = smallest_primary_time_granularity_in_query

        else:
            _, adjusted_to_granularity = self._time_granularity_solver.local_dimension_granularity_range(
                metric_specs=metric_specs,
                local_time_dimension_reference=self._metric_time_dimension_reference,
            )
        logger.info(f"Adjusted primary time granularity is {adjusted_to_granularity}")
        return self._time_granularity_solver.adjust_time_range_to_granularity(
            time_range_constraint, adjusted_to_granularity
        )

    def _find_replacement_for_metric_time_dimension(
        self, replacements: Dict[PartialTimeDimensionSpec, TimeDimensionSpec]
    ) -> Tuple[PartialTimeDimensionSpec, TimeDimensionSpec]:
        for partial_time_dimension_spec_to_replace, replace_with_time_dimension_spec in replacements.items():
            if (
                partial_time_dimension_spec_to_replace.element_name
                == self._metric_time_dimension_reference.element_name
                and partial_time_dimension_spec_to_replace.identifier_links == ()
            ):
                return partial_time_dimension_spec_to_replace, replace_with_time_dimension_spec

        raise RuntimeError(f"Replacement for metric time dimension '{self._metric_time_dimension_reference}' not found")

    def _metric_time_dimension_specified_without_granularity(
        self, partial_time_dimension_specs: Sequence[PartialTimeDimensionSpec]
    ) -> bool:
        # This detects a user query like: "query --metrics monthly_bookings --dimensions ds"
        # The granularity for "ds" is not specified by the user.
        for time_dimension_spec in partial_time_dimension_specs:
            if (
                time_dimension_spec.element_name == self._metric_time_dimension_reference.element_name
                and time_dimension_spec.identifier_links == ()
            ):
                return True
        return False

    def _metrics_have_same_time_granularities(self, metric_specs: Sequence[MetricSpec]) -> bool:
        (min_granularity, max_granularity,) = self._time_granularity_solver.local_dimension_granularity_range(
            metric_specs=metric_specs,
            local_time_dimension_reference=self._metric_time_dimension_reference,
        )
        return min_granularity == max_granularity

    def _find_smallest_metric_time_dimension_spec_granularity(
        self, time_dimension_specs: Sequence[TimeDimensionSpec]
    ) -> Optional[TimeGranularity]:
        metric_time_dimension_specs: List[TimeDimensionSpec] = [
            x
            for x in time_dimension_specs
            if (
                x.element_name == self._metric_time_dimension_reference.element_name
                and x.identifier_links == ()
                and x.time_granularity
            )
        ]

        metric_time_dimension_specs.sort(key=lambda x: x.time_granularity.to_int())
        if len(metric_time_dimension_specs) > 0:
            return metric_time_dimension_specs[0].time_granularity
        else:
            return None

    def _parse_metric_names(self, metric_names: Sequence[str]) -> Tuple[MetricSpec, ...]:
        """Converts metric names into metric names. An exception is thrown if the name is invalid."""

        # The config must be lower-case, so we lower case for case-insensitivity against query inputs from the user.
        metric_names = [x.lower() for x in metric_names]

        known_metric_names = set(self._metric_semantics.metric_names)
        metric_specs: List[MetricSpec] = []
        for metric_name in metric_names:
            metric_spec = MetricSpec(element_name=metric_name)
            if metric_spec not in known_metric_names:
                suggestions = {
                    f"Suggestions for '{metric_name}'": pformat_big_objects(
                        MetricFlowQueryParser._top_fuzzy_matches(
                            item=metric_name,
                            candidate_items=[x.qualified_name for x in self._metric_semantics.metric_names],
                        )
                    )
                }
                raise UnableToSatisfyQueryError(
                    f"Unknown metric: '{metric_name}'",
                    context=suggestions,
                )
            metric_specs.append(metric_spec)
        return tuple(metric_specs)

    def _parse_linkable_element_names(
        self, qualified_linkable_names: Sequence[str], metric_specs: Sequence[MetricSpec]
    ) -> LinkableInstanceSpecs:
        """Convert the linkable spec names into the respective specification objects."""

        qualified_linkable_names = [x.lower() for x in qualified_linkable_names]

        dimension_specs = []
        time_dimension_specs = []
        partial_time_dimension_specs = []
        identifier_specs = []

        for qualified_name in qualified_linkable_names:
            structured_name = StructuredLinkableSpecName.from_name(qualified_name)
            element_name = structured_name.element_name
            identifier_links = tuple(
                LinklessIdentifierSpec.from_element_name(x) for x in structured_name.identifier_link_names
            )
            # Create the spec based on the type of element referenced.
            if TimeDimensionReference(element_name=element_name) in self._known_time_dimension_element_references:
                if structured_name.time_granularity:
                    time_dimension_specs.append(
                        TimeDimensionSpec(
                            element_name=element_name,
                            identifier_links=identifier_links,
                            time_granularity=structured_name.time_granularity,
                        )
                    )
                else:
                    partial_time_dimension_specs.append(
                        PartialTimeDimensionSpec(
                            element_name=element_name,
                            identifier_links=identifier_links,
                        )
                    )
            elif DimensionReference(element_name=element_name) in self._known_dimension_element_references:
                dimension_specs.append(DimensionSpec(element_name=element_name, identifier_links=identifier_links))
            elif IdentifierReference(element_name=element_name) in self._known_identifier_element_references:
                identifier_specs.append(IdentifierSpec(element_name=element_name, identifier_links=identifier_links))
            else:
                valid_group_by_names_for_metrics = sorted(
                    x.qualified_name for x in self._metric_semantics.element_specs_for_metrics(list(metric_specs))
                )

                suggestions = {
                    f"Suggestions for '{qualified_name}'": pformat_big_objects(
                        MetricFlowQueryParser._top_fuzzy_matches(
                            item=qualified_name,
                            candidate_items=valid_group_by_names_for_metrics,
                        )
                    )
                }
                raise UnableToSatisfyQueryError(
                    f"Unknown element name '{element_name}' in dimension name '{qualified_name}'",
                    context=suggestions,
                )

        return LinkableInstanceSpecs(
            dimension_specs=tuple(dimension_specs),
            time_dimension_specs=tuple(time_dimension_specs),
            partial_time_dimension_specs=tuple(partial_time_dimension_specs),
            identifier_specs=tuple(identifier_specs),
        )

    def _get_invalid_linkable_specs(
        self,
        metric_specs: Tuple[MetricSpec, ...],
        dimension_specs: Tuple[DimensionSpec, ...],
        time_dimension_specs: Tuple[TimeDimensionSpec, ...],
        identifier_specs: Tuple[IdentifierSpec, ...],
    ) -> List[LinkableInstanceSpec]:
        """Checks that each requested linkable instance can be retrieved for the given metric"""
        invalid_linkable_specs: List[LinkableInstanceSpec] = []
        # TODO: distinguish between dimensions that invalid via typo vs ambiguous join path
        valid_linkable_specs = self._metric_semantics.element_specs_for_metrics(metric_specs=list(metric_specs))

        for dimension_spec in dimension_specs:
            if dimension_spec not in valid_linkable_specs:
                invalid_linkable_specs.append(dimension_spec)

        for identifier_spec in identifier_specs:
            if identifier_spec not in valid_linkable_specs:
                invalid_linkable_specs.append(identifier_spec)

        for time_dimension_spec in time_dimension_specs:
            if (
                time_dimension_spec not in valid_linkable_specs
                # Because the metric time dimension is a virtual dimension that's not in the model, it won't be included
                # in valid_linkable_specs.
                and time_dimension_spec.reference != DataSet.metric_time_dimension_reference()
            ):
                invalid_linkable_specs.append(time_dimension_spec)

        return invalid_linkable_specs

    def _parse_order_by(
        self,
        order_by_names: Sequence[str],
        time_dimension_spec_replacements: Dict[PartialTimeDimensionSpec, TimeDimensionSpec],
    ) -> Tuple[OrderBySpec, ...]:
        """time_dimension_spec_replacements is used to replace a partial spec from parsing the names to a full one."""
        # TODO: Validate identifier links
        # TODO: Validate order by items are in the query
        order_by_specs: List[OrderBySpec] = []
        for order_by_name in order_by_names:
            descending = False
            if order_by_name.startswith("-"):
                order_by_name = order_by_name[1:]
                descending = True
            parsed_name = StructuredLinkableSpecName.from_name(order_by_name)

            if MetricSpec(element_name=parsed_name.element_name) in self._known_metric_names:
                if parsed_name.time_granularity:
                    raise InvalidQueryException(
                        f"Order by item '{order_by_name}' references a metric but has a time granularity"
                    )
                if parsed_name.identifier_link_names:
                    raise InvalidQueryException(
                        f"Order by item '{order_by_name}' references a metric but has identifier links"
                    )
                order_by_specs.append(
                    OrderBySpec(
                        item=MetricSpec(element_name=parsed_name.element_name),
                        descending=descending,
                    )
                )
            elif DimensionReference(element_name=parsed_name.element_name) in self._known_dimension_element_references:
                if parsed_name.time_granularity:
                    raise InvalidQueryException(
                        f"Order by item '{order_by_name}' references a categorical dimension but has a time "
                        f"granularity"
                    )
                order_by_specs.append(
                    OrderBySpec(
                        item=DimensionSpec(
                            element_name=parsed_name.element_name,
                            identifier_links=tuple(
                                LinklessIdentifierSpec.from_element_name(x) for x in parsed_name.identifier_link_names
                            ),
                        ),
                        descending=descending,
                    )
                )
            elif (
                TimeDimensionReference(element_name=parsed_name.element_name)
                in self._known_time_dimension_element_references
            ):
                identifier_links = tuple(
                    LinklessIdentifierSpec.from_element_name(x) for x in parsed_name.identifier_link_names
                )
                if parsed_name.time_granularity:
                    order_by_specs.append(
                        OrderBySpec(
                            item=TimeDimensionSpec(
                                element_name=parsed_name.element_name,
                                identifier_links=identifier_links,
                                time_granularity=parsed_name.time_granularity,
                            ),
                            descending=descending,
                        )
                    )
                else:
                    # If the time granularity for an order by wasn't specified, replace it with the same time
                    # granularity as it was done for the requested dimensions.
                    partial_time_dimension_spec = PartialTimeDimensionSpec(
                        element_name=parsed_name.element_name,
                        identifier_links=identifier_links,
                    )

                    if partial_time_dimension_spec in time_dimension_spec_replacements:
                        order_by_specs.append(
                            OrderBySpec(
                                item=time_dimension_spec_replacements[partial_time_dimension_spec],
                                descending=descending,
                            )
                        )
                    else:
                        raise RequestTimeGranularityException(
                            f"Order by item '{order_by_name}' does not specify a time granularity and it does not "
                            f"match a requested time dimension"
                        )

            elif (
                IdentifierReference(element_name=parsed_name.element_name) in self._known_identifier_element_references
            ):
                if parsed_name.time_granularity:
                    raise InvalidQueryException(
                        f"Order by item '{order_by_name}' references an identifier but has a time granularity"
                    )
                order_by_specs.append(
                    OrderBySpec(
                        item=IdentifierSpec(
                            element_name=parsed_name.element_name,
                            identifier_links=tuple(
                                LinklessIdentifierSpec.from_element_name(x) for x in parsed_name.identifier_link_names
                            ),
                        ),
                        descending=descending,
                    )
                )
            else:
                raise InvalidQueryException(f"Order by item '{order_by_name}' references an element that is not known")

        return tuple(order_by_specs)
