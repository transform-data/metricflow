from __future__ import annotations

import logging
import re
from typing import List, Optional, Dict, Any

from moz_sql_parser import parse as moz_parse

from metricflow.errors.errors import ConstraintParseException
from metricflow.model.objects.base import HashableBaseModel, PydanticCustomInputParser
from metricflow.model.objects.elements.dimension import DimensionType
from metricflow.model.semantics import semantic_containers
from metricflow.naming.linkable_spec_name import StructuredLinkableSpecName
from metricflow.query.query_exceptions import InvalidQueryException
from metricflow.specs import (
    DimensionSpec,
    IdentifierSpec,
    LinkableSpecSet,
    SpecWhereClauseConstraint,
    TimeDimensionSpec,
)
from metricflow.sql.sql_bind_parameters import SqlBindParameters


logger = logging.getLogger(__name__)

LITERAL_STR = "literal"
INTERVAL_LITERAL = "interval"


class WhereClauseConstraint(PydanticCustomInputParser, HashableBaseModel):
    """Contains a string that is a where clause"""

    where: str
    linkable_names: List[str]
    sql_params: SqlBindParameters

    def __init__(  # noqa: D
        self,
        where: str = "",
        linkable_names: Optional[List[str]] = None,
        sql_params: Optional[SqlBindParameters] = None,
        # sql params: user-originated sql params that need to be escaped in a dialect-specific way keys are the
        # name of the template value in the `where` string, values are the string to be escaped and
        # inserted into the where string (ie where = "%(1)s", sql_values = {"1": "cote d'ivoire"})
    ) -> None:
        where = where.strip("\n") if where else ""
        linkable_names = linkable_names or []
        if sql_params is None:
            sql_params = SqlBindParameters()
        super().__init__(
            where=where,
            linkable_names=linkable_names,
            sql_params=sql_params,
        )

    @classmethod
    def _from_yaml_value(cls, input: Any) -> WhereClauseConstraint:
        """Parses a WhereClauseConstraint from a constraint string found in a user-provided model specification

        User-provided constraint strings are SQL snippets conforming to the expectations of SQL WHERE clauses,
        and as such we parse them using our standard parse method below.

        Note in some cases we might wish to initialize a WhereClauseConstraint inside of a model object. In such
        cases we simply pass the instance along, since it should have been pre-validated on initialization and
        therefore we expect it to be internally consistent.
        """
        if isinstance(input, str):
            return WhereClauseConstraint.parse(input)
        elif isinstance(input, WhereClauseConstraint):
            # This is internally constructed, pass it through and ignore it in error messaging
            return input
        else:
            raise ValueError(f"Expected input to be of type string, but got type {type(input)} with value: {input}")

    @staticmethod
    def parse(s: str) -> WhereClauseConstraint:
        """Parse a string into a WhereClauseConstraint

        We are assuming here that if we needed to parse a string, we wouldn't have bind parameters.
        Because if we had bind-parameters, the string would have existing structure, and we wouldn't need to parse it.
        """
        s = strip_where(s)

        where_str = f"WHERE {s}"
        # to piggyback on moz sql parser we need a SELECT statement
        # moz breaks the sql statement into clauses:
        # where_str = "WHERE is_instant" yields -> {'select': {'value': '_'}, 'from': '_', 'where': 'is_instant'}
        # where_str = "WHERE is_instant AND country = 'vanuatu' AND is_lux or ds < '2020-01-02'" yields ->
        # {'select': {'value': '_'}, 'from': '_', 'where': {'or': [{'and': ['is_instant', {'eq': ['country', {'literal': 'vanuatu'}]}, 'is_lux']}, {'lt': ['ds', {'literal': '2020-01-02'}]}]}}
        parsed = moz_parse(f"select _ from _ {where_str}")
        if "where" not in parsed:
            raise ConstraintParseException(parsed)

        where = parsed["where"]
        if isinstance(where, dict):
            if not len(where.keys()) == 1:
                raise ConstraintParseException(f"expected parsed constraint to contain exactly one key; got {where}")
            return WhereClauseConstraint(
                where=s,
                linkable_names=constraint_dimension_names_from_dict(where),
                sql_params=SqlBindParameters(),
            )
        elif isinstance(where, str):
            return WhereClauseConstraint(
                where=s,
                linkable_names=[where.strip()],
                sql_params=SqlBindParameters(),
            )
        else:
            raise TypeError(f"where-clause is neither a dict nor a string. Unexpectedly it is a {type(where)}")

    def to_spec_where_constraint(
        self,
        data_source_semantics: semantic_containers.DataSourceSemantics,
    ) -> SpecWhereClauseConstraint:
        """Converts a where constraint to one using specs."""
        return SpecWhereClauseConstraint(
            where_condition=self.where,
            linkable_names=tuple(self.linkable_names),
            linkable_spec_set=self._names_to_linkable_specs(
                data_source_semantics=data_source_semantics,
            ),
            execution_parameters=self.sql_params,
        )

    def _names_to_linkable_specs(
        self,
        data_source_semantics: semantic_containers.DataSourceSemantics,
    ) -> LinkableSpecSet:
        """Processes where_clause_constraint.linkable_names into associated LinkableInstanceSpecs (dims, times, ids)

        data_source_semantics: DataSourceSemantics from the instantiated class

        output: InstanceSpecSet of Tuple(DimensionSpec), Tuple(TimeDimensionSpec), Tuple(IdentifierSpec)
        """
        where_constraint_dimensions = []
        where_constraint_time_dimensions = []
        where_constraint_identifiers = []
        linkable_spec_names = [
            StructuredLinkableSpecName.from_name(linkable_name) for linkable_name in self.linkable_names
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

    def __repr__(self) -> str:  # noqa: D
        return f"{self.__class__.__name__}" f"(where={self.where}, linkable_names={self.linkable_names})"


def strip_where(s: str) -> str:
    """Removes WHERE from the beginning of the string, if present (regardless of case)"""
    # '^' tells the regex to only check the beginning of the string
    return re.sub("^where ", "", s, flags=re.IGNORECASE)


def constraint_dimension_names_from_dict(where: Dict[str, Any]) -> List[str]:  # type: ignore[misc] # noqa: D
    dims = []
    for key, clause in where.items():
        if key == LITERAL_STR or key == INTERVAL_LITERAL:
            continue
        dims += _get_dimensions_from_clause(clause)

    return dims


def constraint_values_from_dict(where: Dict[str, Any]) -> List[str]:  # type: ignore[misc] # noqa: d
    values = []
    for key, clause in where.items():
        if key == LITERAL_STR:
            values.append(clause)
        elif isinstance(clause, dict):
            values += constraint_values_from_dict(clause)
        elif isinstance(clause, list):
            for item in clause:
                if isinstance(item, dict):
                    values += constraint_values_from_dict(item)

    return values


def _constraint_dimensions_from_list(list_clause: List[Any]) -> List[str]:  # type: ignore[misc] # noqa: D
    dims = []
    for clause in list_clause:
        dims += _get_dimensions_from_clause(clause)

    return dims


def _get_dimensions_from_clause(clause: Any) -> List[str]:  # type: ignore[misc] # noqa: D
    if clause is not None:
        if isinstance(clause, dict):
            return constraint_dimension_names_from_dict(clause)
        elif isinstance(clause, list):
            return _constraint_dimensions_from_list(clause)
        elif isinstance(clause, str):
            return [clause.strip()]

    return []
