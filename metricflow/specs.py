"""These spec classes generally describe something that we want or have.

* Not too many comments here since they seem mostly self-explanatory.
* To see whether a spec matches something that already exists, there could be a method that allows you to match a spec
  to another spec or relevant object.
* The match() method will enable sub-classes (may require some restructuring) to use specs to request things like,
  metrics named "sales*".
"""

from __future__ import annotations

import itertools
from abc import ABC, abstractmethod
from collections import OrderedDict
from dataclasses import dataclass
from typing import List, Optional, Sequence, Tuple, TypeVar, Generic

from metricflow.column_assoc import ColumnAssociation
from metricflow.constraints.time_constraint import TimeRangeConstraint
from metricflow.time.time_granularity import TimeGranularity
from metricflow.model.objects.base import FrozenBaseModel
from metricflow.naming.linkable_spec_name import StructuredLinkableSpecName
from metricflow.sql.sql_bind_parameters import SqlBindParameters


class ColumnAssociationResolver(ABC):
    """Get the default column associations for an element instance.

    This is used for naming columns in an SQL query consistently. For example, dimensions with links are
    named like <identifier link>__<dimension name> e.g. user_id__country, and time dimensions at a different time
    granularity are named <time dimension>__<time granularity> e.g. ds__month. Having a central place to name them will
    make it easier to change this later on. Names generated need to be unique within a query.

    It's also important to maintain this format because customers write constraints in SQL assuming this. This
    allows us to stick the constraint in as WHERE clauses without having to parse the constraint SQL.

    TODO: Updates are needed for time granularity in time dimensions, ToT for metrics.

    The resolve* methods should return the column associations / column names that it should use in queries for the given
    spec.
    """

    @abstractmethod
    def resolve_metric_spec(self, metric_spec: MetricSpec) -> ColumnAssociation:  # noqa: D
        pass

    @abstractmethod
    def resolve_measure_spec(self, measure_spec: MeasureSpec) -> ColumnAssociation:  # noqa: D
        pass

    @abstractmethod
    def resolve_dimension_spec(self, dimension_spec: DimensionSpec) -> ColumnAssociation:  # noqa: D
        pass

    @abstractmethod
    def resolve_time_dimension_spec(self, time_dimension_spec: TimeDimensionSpec) -> ColumnAssociation:  # noqa: D
        pass

    @abstractmethod
    def resolve_identifier_spec(self, identifier_spec: IdentifierSpec) -> Tuple[ColumnAssociation, ...]:  # noqa: D
        pass


class InstanceSpec(ABC, FrozenBaseModel):
    """A specification for an instance of a metric definition object.

    An instance is different from the definition object in that it correlates to columns in the data flow and can be in
    different states. e.g. a time dimension at a different time granularity.

    Added "type: ignore" as there is an issue with abstract methods in frozen dataclasses:
    https://github.com/python/mypy/issues/5374
    """

    """Name of the dimension or identifier in the data source."""
    element_name: str

    @abstractmethod
    def column_associations(self, resolver: ColumnAssociationResolver) -> Tuple[ColumnAssociation, ...]:
        """Figures out what columns in an SQL query that this spec should be associated with given the resolver.

        Debating whether this should be an abstract method, or whether it could just live in the different specs to
        allow for different signatures.
        """
        pass

    @staticmethod
    def merge(*specs: Sequence[InstanceSpec]) -> List[InstanceSpec]:
        """Merge all specs into a single list."""
        result: List[InstanceSpec] = []
        for spec in specs:
            result.extend(spec)
        return result

    @property
    @abstractmethod
    def qualified_name(self) -> str:
        """Return the qualified name of this spec. e.g. "user_id__country"."""
        pass


class LinkableInstanceSpec(InstanceSpec, ABC):
    """Generally a dimension or identifier that may be specified using identifier links.

    For example, user_id__country -> LinkableElementSpec(element_name="country", identifier_links=["user_id"]

    See InstanceSpec for the reason behind "type: ignore"
    """

    """A list representing the join path of identifiers to get to this element."""
    identifier_links: Tuple[LinklessIdentifierSpec, ...]

    def __post_init__(self) -> None:  # noqa: D
        for identifier in self.identifier_links:
            if len(identifier.identifier_links) > 0:
                raise RuntimeError("Identifier links may not have their own identifier links")

    @abstractmethod
    def without_first_identifier_link(self) -> LinkableInstanceSpec:
        """e.g. user_id__device_id__platform -> device_id__platform"""
        pass

    @abstractmethod
    def without_identifier_links(self) -> LinkableInstanceSpec:  # noqa: D
        """e.g. user_id__device_id__platform -> platform"""
        pass

    @staticmethod
    def merge_linkable_specs(*specs: Sequence[LinkableInstanceSpec]) -> List[LinkableInstanceSpec]:
        """Merge all specs into a single list."""
        result: List[LinkableInstanceSpec] = []
        for spec in specs:
            result.extend(spec)
        return result

    @property
    def qualified_name(self) -> str:
        """Return the qualified name of this spec. e.g. "user_id__country"."""
        return StructuredLinkableSpecName(
            identifier_link_names=tuple(x.element_name for x in self.identifier_links), element_name=self.element_name
        ).qualified_name


class IdentifierSpec(LinkableInstanceSpec):  # noqa: D
    def column_associations(self, resolver: ColumnAssociationResolver) -> Tuple[ColumnAssociation, ...]:  # noqa: D
        return resolver.resolve_identifier_spec(self)

    def without_first_identifier_link(self) -> LinkableInstanceSpec:  # noqa: D
        assert len(self.identifier_links) > 0, f"Spec does not have any identifier links: {self}"
        return IdentifierSpec(element_name=self.element_name, identifier_links=self.identifier_links[1:])

    def without_identifier_links(self) -> LinklessIdentifierSpec:  # noqa: D
        return LinklessIdentifierSpec.from_element_name(self.element_name)

    @property
    def as_linkless_prefix(self) -> Tuple[LinklessIdentifierSpec, ...]:
        """Creates tuple of linkless identifiers that could be included in the identifier_links of another spec

        eg as a prefix to a DimensionSpec's identifier links to when a join is occurring via this identifier
        """
        return (LinklessIdentifierSpec.from_element_name(self.element_name),) + self.identifier_links

    @staticmethod
    def from_name(name: str) -> IdentifierSpec:  # noqa: D
        structured_name = StructuredLinkableSpecName.from_name(name)
        return IdentifierSpec(
            identifier_links=tuple(
                LinklessIdentifierSpec.from_element_name(idl) for idl in structured_name.identifier_link_names
            ),
            element_name=structured_name.element_name,
        )


class LinklessIdentifierSpec(IdentifierSpec):
    """Similar to IdentifierSpec, but requires that it doesn't have identifier links."""

    @staticmethod
    def from_element_name(element_name: str) -> LinklessIdentifierSpec:  # noqa: D
        return LinklessIdentifierSpec(element_name=element_name, identifier_links=())

    def __post_init__(self) -> None:  # noqa: D
        if len(self.identifier_links) > 0:
            raise RuntimeError(f"{self.__class__.__name__} shouldn't have identifier links. Got: {self}")


IdentifierSpec.update_forward_refs()
LinklessIdentifierSpec.update_forward_refs()


class DimensionSpec(LinkableInstanceSpec):  # noqa: D
    def column_associations(self, resolver: ColumnAssociationResolver) -> Tuple[ColumnAssociation, ...]:  # noqa: D
        return (resolver.resolve_dimension_spec(self),)

    def without_first_identifier_link(self) -> LinkableInstanceSpec:  # noqa: D
        assert len(self.identifier_links) > 0, f"Spec does not have any identifier links: {self}"
        return DimensionSpec(element_name=self.element_name, identifier_links=self.identifier_links[1:])

    def without_identifier_links(self) -> LinkableInstanceSpec:  # noqa: D
        return DimensionSpec(element_name=self.element_name, identifier_links=())

    @staticmethod
    def from_linkable(spec: LinkableInstanceSpec) -> DimensionSpec:  # noqa: D
        return DimensionSpec(element_name=spec.element_name, identifier_links=spec.identifier_links)

    @staticmethod
    def from_name(name: str) -> DimensionSpec:
        """Construct from a name e.g. listing__ds__month."""
        parsed_name = StructuredLinkableSpecName.from_name(name)
        return DimensionSpec(
            identifier_links=tuple(
                [LinklessIdentifierSpec.from_element_name(idl) for idl in parsed_name.identifier_link_names]
            ),
            element_name=parsed_name.element_name,
        )

    @property
    def reference(self) -> DimensionReference:  # noqa: D
        return DimensionReference(element_name=self.element_name)


DimensionSpec.update_forward_refs()

DEFAULT_TIME_GRANULARITY = TimeGranularity.DAY


class TimeDimensionSpec(DimensionSpec):  # noqa: D
    time_granularity: TimeGranularity = DEFAULT_TIME_GRANULARITY

    def column_associations(self, resolver: ColumnAssociationResolver) -> Tuple[ColumnAssociation, ...]:  # noqa: D
        return (resolver.resolve_time_dimension_spec(self),)

    def without_first_identifier_link(self) -> LinkableInstanceSpec:  # noqa: D
        assert len(self.identifier_links) > 0, f"Spec does not have any identifier links: {self}"
        return TimeDimensionSpec(
            element_name=self.element_name,
            identifier_links=self.identifier_links[1:],
            time_granularity=self.time_granularity,
        )

    def without_identifier_links(self) -> LinklessIdentifierSpec:  # noqa: D
        return LinklessIdentifierSpec.from_element_name(self.element_name)

    @staticmethod
    def from_name(name: str) -> TimeDimensionSpec:  # noqa: D
        structured_name = StructuredLinkableSpecName.from_name(name)
        return TimeDimensionSpec(
            identifier_links=tuple(
                LinklessIdentifierSpec.from_element_name(idl) for idl in structured_name.identifier_link_names
            ),
            element_name=structured_name.element_name,
            time_granularity=structured_name.time_granularity or DEFAULT_TIME_GRANULARITY,
        )

    @property
    def reference(self) -> TimeDimensionReference:  # noqa: D
        return TimeDimensionReference(element_name=self.element_name)

    @property
    def dimension_reference(self) -> DimensionReference:  # noqa: D
        return DimensionReference(element_name=self.element_name)

    @property
    def qualified_name(self) -> str:  # noqa: D
        return StructuredLinkableSpecName(
            identifier_link_names=tuple(x.element_name for x in self.identifier_links),
            element_name=self.element_name,
            time_granularity=self.time_granularity,
        ).qualified_name


@dataclass(frozen=True)
class ElementReference:
    """Used when we need to refer to a dimension, measure, identifier, but other attributes are unknown."""

    element_name: str


@dataclass(frozen=True)
class LinkableElementReference(ElementReference):
    """Used when we need to refer to a dimension or identifier, but other attributes are unknown."""

    pass


@dataclass(frozen=True)
class MeasureReference(ElementReference):
    """Used when we need to refer to a measure (separate from LinkableElementReference because measures aren't linkable"""

    pass


@dataclass(frozen=True)
class DimensionReference(LinkableElementReference):  # noqa: D
    pass

    @property
    def time_dimension_reference(self) -> TimeDimensionReference:  # noqa: D
        return TimeDimensionReference(element_name=self.element_name)


@dataclass(frozen=True)
class IdentifierReference(LinkableElementReference):  # noqa: D
    pass


@dataclass(frozen=True)
class CompositeSubIdentifierReference(ElementReference):  # noqa: D
    pass


@dataclass(frozen=True)
class TimeDimensionReference(DimensionReference):  # noqa: D
    pass

    def dimension_reference(self) -> DimensionReference:  # noqa: D
        return DimensionReference(element_name=self.element_name)


class MeasureSpec(InstanceSpec):  # noqa: D
    def column_associations(self, resolver: ColumnAssociationResolver) -> Tuple[ColumnAssociation, ...]:  # noqa: D
        return (resolver.resolve_measure_spec(self),)

    @staticmethod
    def from_name(name: str) -> MeasureSpec:
        """Construct from a name e.g. listing__ds__month."""
        return MeasureSpec(element_name=name)

    @staticmethod
    def from_reference(reference: MeasureReference) -> MeasureSpec:
        """Initialize from a measure reference instance"""
        return MeasureSpec(element_name=reference.element_name)

    @property
    def qualified_name(self) -> str:  # noqa: D
        return self.element_name

    @property
    def as_reference(self) -> MeasureReference:  # noqa: D
        return MeasureReference(element_name=self.element_name)


class MetricSpec(InstanceSpec):  # noqa: D
    # Time-over-time could go here

    def column_associations(self, resolver: ColumnAssociationResolver) -> Tuple[ColumnAssociation, ...]:  # noqa: D
        return (resolver.resolve_metric_spec(self),)

    @property
    def qualified_name(self) -> str:  # noqa: D
        return self.element_name


class OrderBySpec(FrozenBaseModel):  # noqa: D
    item: InstanceSpec
    descending: bool


class FilterSpec(FrozenBaseModel):  # noqa: D
    expr: str
    elements: Tuple[InstanceSpec, ...]


class OutputColumnNameOverride(FrozenBaseModel):
    """Describes how we should name the output column for a time dimension instead of the default.

    Note: This is used temporarily to maintain compatibility with the old framework.
    """

    time_dimension_spec: TimeDimensionSpec
    output_column_name: str


class LinkableSpecSet(FrozenBaseModel):
    """Groups linkable specs."""

    dimension_specs: Tuple[DimensionSpec, ...] = ()
    time_dimension_specs: Tuple[TimeDimensionSpec, ...] = ()
    identifier_specs: Tuple[IdentifierSpec, ...] = ()

    @property
    def as_tuple(self) -> Tuple[LinkableInstanceSpec, ...]:  # noqa: D
        return tuple(itertools.chain(self.dimension_specs, self.time_dimension_specs, self.identifier_specs))

    @staticmethod
    def merge(spec_sets: Sequence[LinkableSpecSet]) -> LinkableSpecSet:
        """Merges and dedupes the linkable specs."""

        dimension_specs: List[DimensionSpec] = []
        time_dimension_specs: List[TimeDimensionSpec] = []
        identifier_specs: List[IdentifierSpec] = []

        for spec_set in spec_sets:
            for dimension_spec in spec_set.dimension_specs:
                if dimension_spec not in dimension_specs:
                    dimension_specs.append(dimension_spec)
            for time_dimension_spec in spec_set.time_dimension_specs:
                if time_dimension_spec not in time_dimension_specs:
                    time_dimension_specs.append(time_dimension_spec)
            for identifier_spec in spec_set.identifier_specs:
                if identifier_spec not in identifier_specs:
                    identifier_specs.append(identifier_spec)

        return LinkableSpecSet(
            dimension_specs=tuple(dimension_specs),
            time_dimension_specs=tuple(time_dimension_specs),
            identifier_specs=tuple(identifier_specs),
        )

    def is_subset_of(self, other_set: LinkableSpecSet) -> bool:  # noqa: D
        return set(self.as_tuple).issubset(set(other_set.as_tuple))


class SpecWhereClauseConstraint(FrozenBaseModel):
    """Similar to a WhereClauseConstraint, but with specs instead of strings"""

    # e.g. "listing__capacity_latest > 4"
    where_condition: str
    # e.g. {DimensionSpec(element_name="capacity_latest", identifier_links=("listing",))
    linkable_names: Tuple[str, ...]
    linkable_spec_set: LinkableSpecSet
    execution_parameters: SqlBindParameters

    def combine(self, other: SpecWhereClauseConstraint) -> SpecWhereClauseConstraint:  # noqa: D
        linkable_names = list(set(self.linkable_names).union(set(other.linkable_names)))
        where_condition = f"({self.where_condition}) AND ({other.where_condition})"
        new_sql_values = OrderedDict(self.execution_parameters.param_dict)
        for k, v in other.execution_parameters.param_dict.items():
            if k in new_sql_values and v != new_sql_values[k]:
                raise ValueError(
                    f"Cannot combine with an execution parameter collision. Both where clauses have key ({k}),"
                    f" but different values: ({v} != {new_sql_values[k]})"
                )

            new_sql_values[k] = v

        return SpecWhereClauseConstraint(
            where_condition=where_condition,
            linkable_names=tuple(linkable_names),
            linkable_spec_set=LinkableSpecSet.merge([self.linkable_spec_set, other.linkable_spec_set]),
            execution_parameters=SqlBindParameters(param_dict=OrderedDict(new_sql_values)),
        )


class MetricInputMeasureSpec(FrozenBaseModel):
    """The spec for a measure defined as a metric input.

    This is necessary because the MeasureSpec is used as a key linking the measures used in the query
    to the measures defined in the data sources. Adding metric-specific information, like constraints,
    causes lookups connecting query -> data source to fail in strange ways. This spec, then, provides
    both the key (in the form of a MeasureSpec) along with whatever measure-specific attributes
    a user might specify in a metric definition or query accessing the metric itself.
    """

    measure_spec: MeasureSpec
    constraint: Optional[SpecWhereClauseConstraint]


class MetricFlowQuerySpec(FrozenBaseModel):
    """Specs needed for running a query."""

    metric_specs: Tuple[MetricSpec, ...] = ()
    dimension_specs: Tuple[DimensionSpec, ...] = ()
    identifier_specs: Tuple[IdentifierSpec, ...] = ()
    time_dimension_specs: Tuple[TimeDimensionSpec, ...] = ()
    order_by_specs: Tuple[OrderBySpec, ...] = ()
    output_column_name_overrides: Tuple[OutputColumnNameOverride, ...] = ()
    time_range_constraint: Optional[TimeRangeConstraint] = None
    where_constraint: Optional[SpecWhereClauseConstraint] = None
    limit: Optional[int] = None

    @property
    def linkable_specs(self) -> LinkableSpecSet:  # noqa: D
        return LinkableSpecSet(
            dimension_specs=self.dimension_specs,
            time_dimension_specs=self.time_dimension_specs,
            identifier_specs=self.identifier_specs,
        )


TransformOutputT = TypeVar("TransformOutputT")


class InstanceSpecSetTransform(Generic[TransformOutputT], ABC):
    """Function to use for transforming spec sets."""

    @abstractmethod
    def transform(self, spec_set: InstanceSpecSet) -> TransformOutputT:  # noqa: D
        pass


class InstanceSpecSet(FrozenBaseModel):
    """Consolidates all specs used in an instance set"""

    metric_specs: Tuple[MetricSpec, ...] = ()
    measure_specs: Tuple[MeasureSpec, ...] = ()
    dimension_specs: Tuple[DimensionSpec, ...] = ()
    identifier_specs: Tuple[IdentifierSpec, ...] = ()
    time_dimension_specs: Tuple[TimeDimensionSpec, ...] = ()

    def merge(self, others: Sequence[InstanceSpecSet]) -> InstanceSpecSet:
        """Merge all sets into one set, without de-duplication."""
        return InstanceSpecSet(
            metric_specs=self.metric_specs + tuple(itertools.chain.from_iterable([x.metric_specs for x in others])),
            dimension_specs=self.dimension_specs
            + tuple(itertools.chain.from_iterable([x.dimension_specs for x in others])),
            identifier_specs=self.identifier_specs
            + tuple(itertools.chain.from_iterable([x.identifier_specs for x in others])),
            time_dimension_specs=self.time_dimension_specs
            + tuple(itertools.chain.from_iterable([x.time_dimension_specs for x in others])),
        )

    @property
    def linkable_specs(self) -> Sequence[LinkableInstanceSpec]:
        """All linkable specs in this set."""
        return list(itertools.chain(self.dimension_specs, self.time_dimension_specs, self.identifier_specs))

    @property
    def all_specs(self) -> Sequence[InstanceSpec]:  # noqa: D
        return tuple(
            itertools.chain(
                self.measure_specs,
                self.dimension_specs,
                self.time_dimension_specs,
                self.identifier_specs,
                self.metric_specs,
            )
        )

    def transform(self, transform_function: InstanceSpecSetTransform[TransformOutputT]) -> TransformOutputT:  # noqa: D
        return transform_function.transform(self)


class PartitionSpecSet(FrozenBaseModel):
    """Grouping of the linkable specs."""

    dimension_specs: Tuple[DimensionSpec, ...] = ()
    time_dimension_specs: Tuple[TimeDimensionSpec, ...] = ()
