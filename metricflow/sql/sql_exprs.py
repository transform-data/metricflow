"""Nodes used in defining SQL expressions."""

from __future__ import annotations

import itertools
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import List, Generic, Sequence, Optional, Tuple, Dict

from metricflow.model.objects.elements.measure import AggregationType
from metricflow.dag.mf_dag import DagNode, DisplayedProperty, NodeId
from metricflow.dag.id_generation import (
    SQL_EXPR_COLUMN_REFERENCE_ID_PREFIX,
    SQL_EXPR_FUNCTION_ID_PREFIX,
    SQL_EXPR_STRING_ID_PREFIX,
    SQL_EXPR_COMPARISON_ID_PREFIX,
    SQL_EXPR_NULL_PREFIX,
    SQL_EXPR_LOGICAL_OPERATOR_PREFIX,
    SQL_EXPR_STRING_LITERAL_PREFIX,
    SQL_EXPR_IS_NULL_PREFIX,
    SQL_EXPR_DATE_TRUNC,
    SQL_EXPR_RATIO_COMPUTATION,
    SQL_EXPR_BETWEEN_PREFIX,
)
from metricflow.sql.sql_bind_parameters import SqlBindParameters
from metricflow.visitor import Visitable, VisitorOutputT
from metricflow.time.time_granularity import TimeGranularity
from metricflow.object_utils import assert_values_exhausted, flatten_nested_sequence


class SqlExpressionNode(DagNode, Generic[VisitorOutputT], Visitable, ABC):
    """An SQL expression like my_table.my_column, CONCAT(a, b) or 1 + 1 that evaluates to a value."""

    def __init__(self, node_id: NodeId, parent_nodes: List[SqlExpressionNode]) -> None:  # noqa: D
        self._parent_nodes = parent_nodes
        super().__init__(node_id=node_id)

    @property
    @abstractmethod
    def requires_parenthesis(self) -> bool:
        """Should expression needs be rendered with parenthesis when rendering inside other expressions.

        Useful for string expressions where we can't infer the structure. For example, in rendering

        SqlMathExpression(operator="*", left_expr=SqlStringExpression("a"), right_expr=SqlStringExpression("b + c")

        this can be used to differentiate between

        a * b + c vs. a * (b + c)
        """
        pass

    @abstractmethod
    def accept(self, visitor: SqlExpressionNodeVisitor) -> VisitorOutputT:
        """Called when a visitor needs to visit this node."""
        pass

    @property
    def execution_parameters(self) -> SqlBindParameters:
        """Execution parameters when running a query containing this expression.

        * See: https://docs.sqlalchemy.org/en/14/core/tutorial.html#using-textual-sql
        * Generally only defined for string expressions.
        """
        return SqlBindParameters()

    @property
    def parent_nodes(self) -> Sequence[SqlExpressionNode]:  # noqa: D
        return self._parent_nodes

    @property
    def as_column_reference_expression(self) -> Optional[SqlColumnReferenceExpression]:
        """If this is a column reference expression, return self."""
        return None

    @property
    def as_string_expression(self) -> Optional[SqlStringExpression]:
        """If this is a string expression, return self."""
        return None

    @abstractmethod
    def rewrite(
        self,
        column_replacements: Optional[SqlColumnReplacements] = None,
        should_render_table_alias: Optional[bool] = None,
    ) -> SqlExpressionNode:
        """Return the same semantic expression but with re-written according to the input.

        Args:
            column_replacements: Replaces column references according to this map.
            should_render_table_alias: Change if table aliases should be rendered for column reference expressions.
        """
        pass

    @property
    @abstractmethod
    def lineage(self) -> SqlExpressionTreeLineage:
        """Returns all nodes in the paths from this node to the root nodes"""
        pass

    def _parents_match(self, other: SqlExpressionNode) -> bool:  # noqa: D
        return all(x == y for x, y in itertools.zip_longest(self.parent_nodes, other.parent_nodes))

    @abstractmethod
    def matches(self, other: SqlExpressionNode) -> bool:
        """Similar to equals - returns true if these expressions are equivalent."""
        pass


@dataclass(frozen=True)
class SqlExpressionTreeLineage:
    """Captures the lineage of an expression node - contains itself and all ancestor nodes."""

    string_exprs: Tuple[SqlStringExpression, ...] = ()
    function_exprs: Tuple[SqlFunctionExpression, ...] = ()
    column_reference_exprs: Tuple[SqlColumnReferenceExpression, ...] = ()
    column_alias_reference_exprs: Tuple[SqlColumnAliasReferenceExpression, ...] = ()
    other_exprs: Tuple[SqlExpressionNode, ...] = ()

    @staticmethod
    def combine(lineages: Sequence[SqlExpressionTreeLineage]) -> SqlExpressionTreeLineage:
        """Combine multiple lineages into one lineage, without de-duping."""
        return SqlExpressionTreeLineage(
            string_exprs=flatten_nested_sequence(tuple(x.string_exprs for x in lineages)),
            function_exprs=flatten_nested_sequence(tuple(x.function_exprs for x in lineages)),
            column_reference_exprs=flatten_nested_sequence(tuple(x.column_reference_exprs for x in lineages)),
            column_alias_reference_exprs=flatten_nested_sequence(
                tuple(x.column_alias_reference_exprs for x in lineages)
            ),
            other_exprs=flatten_nested_sequence(tuple(x.other_exprs for x in lineages)),
        )

    @property
    def contains_string_exprs(self) -> bool:  # noqa: D
        return len(self.string_exprs) > 0

    @property
    def contains_column_alias_exprs(self) -> bool:  # noqa: D
        return len(self.column_alias_reference_exprs) > 0

    @property
    def contains_ambiguous_exprs(self) -> bool:  # noqa: D
        return self.contains_string_exprs or self.contains_column_alias_exprs


class SqlColumnReplacements:
    """When re-writing column references in expressions, this storing the mapping."""

    def __init__(self, column_replacements: Dict[SqlColumnReference, SqlExpressionNode]) -> None:  # noqa: D
        self._column_replacements = column_replacements

    def get_replacement(self, column_reference: SqlColumnReference) -> Optional[SqlExpressionNode]:  # noqa: D
        return self._column_replacements.get(column_reference)


class SqlExpressionNodeVisitor(Generic[VisitorOutputT], ABC):
    """A visitor to help visit the nodes of an expression.

    See similar visitor DataflowPlanVisitor.
    """

    @abstractmethod
    def visit_string_expr(self, node: SqlStringExpression) -> VisitorOutputT:  # noqa: D
        pass

    @abstractmethod
    def visit_column_reference_expr(self, node: SqlColumnReferenceExpression) -> VisitorOutputT:  # noqa: D
        pass

    @abstractmethod
    def visit_column_alias_reference_expr(self, node: SqlColumnAliasReferenceExpression) -> VisitorOutputT:  # noqa: D
        pass

    @abstractmethod
    def visit_comparison_expr(self, node: SqlComparisonExpression) -> VisitorOutputT:  # noqa: D
        pass

    @abstractmethod
    def visit_function_expr(self, node: SqlFunctionExpression) -> VisitorOutputT:  # noqa: D
        pass

    @abstractmethod
    def visit_null_expr(self, node: SqlNullExpression) -> VisitorOutputT:  # noqa: D
        pass

    @abstractmethod
    def visit_logical_expr(self, node: SqlLogicalExpression) -> VisitorOutputT:  # noqa: D
        pass

    @abstractmethod
    def visit_string_literal_expr(self, node: SqlStringLiteralExpression) -> VisitorOutputT:  # noqa: D
        pass

    @abstractmethod
    def visit_is_null_expr(self, node: SqlIsNullExpression) -> VisitorOutputT:  # noqa: D
        pass

    @abstractmethod
    def visit_cast_to_timestamp_expr(self, node: SqlCastToTimestampExpression) -> VisitorOutputT:  # noqa: D
        pass

    @abstractmethod
    def visit_date_trunc_expr(self, node: SqlDateTruncExpression) -> VisitorOutputT:  # noqa: D
        pass

    @abstractmethod
    def visit_time_delta_expr(self, node: SqlTimeDeltaExpression) -> VisitorOutputT:  # noqa: D
        pass

    @abstractmethod
    def visit_ratio_computation_expr(self, node: SqlRatioComputationExpression) -> VisitorOutputT:  # noqa: D
        pass

    @abstractmethod
    def visit_between_expr(self, node: SqlBetweenExpression) -> VisitorOutputT:  # noqa: D
        pass


class SqlStringExpression(SqlExpressionNode):
    """An SQL expression in a string format, so it lacks information about the structure.

    These are convenient to use, but because structure is lacking, it can't be easily handled for DB rendering and can
    impede optimizations.
    """

    def __init__(
        self,
        sql_expr: str,
        execution_parameters: Optional[SqlBindParameters] = None,
        requires_parenthesis: bool = True,
        used_columns: Optional[Tuple[str, ...]] = None,
    ) -> None:
        """Constructor.

        Args:
            sql_expr: The SQL in string form.
            execution_parameters: See SqlExpressionNode.execution_parameters
            requires_parenthesis: Whether this should be rendered with () if nested in another expression.
            used_columns: If set, indicates that the expression represented by the string only uses those columns. e.g.
            sql_expr="a + b", used_columns=["a", "b"]. This may be used by optimizers, and if specified, it must be
            complete. e.g. sql_expr="a + b + c", used_columns=["a", "b"] will cause problems.
        """
        self._sql_expr = sql_expr
        self._execution_parameters = execution_parameters or SqlBindParameters()
        self._requires_parenthesis = requires_parenthesis
        self._used_columns = used_columns
        super().__init__(node_id=self.create_unique_id(), parent_nodes=[])

    @classmethod
    def id_prefix(cls) -> str:  # noqa: D
        return SQL_EXPR_STRING_ID_PREFIX

    def accept(self, visitor: SqlExpressionNodeVisitor) -> VisitorOutputT:  # noqa: D
        return visitor.visit_string_expr(self)

    @property
    def description(self) -> str:  # noqa: D
        return f"String SQL Expression: {self._sql_expr}"

    @property
    def displayed_properties(self) -> List[DisplayedProperty]:  # noqa: D
        return super().displayed_properties + [DisplayedProperty("sql_expr", self._sql_expr)]

    @property
    def sql_expr(self) -> str:  # noqa: D
        return self._sql_expr

    @property
    def requires_parenthesis(self) -> bool:  # noqa: D
        return self._requires_parenthesis

    @property
    def execution_parameters(self) -> SqlBindParameters:  # noqa: D
        return self._execution_parameters

    @property
    def used_columns(self) -> Optional[Tuple[str, ...]]:  # noqa: D
        return self._used_columns

    def __repr__(self) -> str:  # noqa: D
        return f"{self.__class__.__name__}(node_id={self.node_id} sql_expr={self.sql_expr})"

    def rewrite(  # noqa: D
        self,
        column_replacements: Optional[SqlColumnReplacements] = None,
        should_render_table_alias: Optional[bool] = None,
    ) -> SqlExpressionNode:
        if column_replacements:
            raise NotImplementedError()
        return self

    @property
    def lineage(self) -> SqlExpressionTreeLineage:  # noqa: D
        return SqlExpressionTreeLineage(string_exprs=(self,))

    def matches(self, other: SqlExpressionNode) -> bool:  # noqa: D
        if not isinstance(other, SqlStringExpression):
            return False
        return (
            self.sql_expr == other.sql_expr
            and self.used_columns == other.used_columns
            and self.execution_parameters == other.execution_parameters
        )

    def as_string_expression(self) -> Optional[SqlStringExpression]:
        """If this is a string expression, return self."""
        return self


class SqlStringLiteralExpression(SqlExpressionNode):
    """A string literal like 'foo'. It shouldn't include delimiters as it should be added during rendering."""

    def __init__(self, literal_value: str) -> None:  # noqa: D
        self._literal_value = literal_value
        super().__init__(node_id=self.create_unique_id(), parent_nodes=[])

    @classmethod
    def id_prefix(cls) -> str:  # noqa: D
        return SQL_EXPR_STRING_LITERAL_PREFIX

    def accept(self, visitor: SqlExpressionNodeVisitor) -> VisitorOutputT:  # noqa: D
        return visitor.visit_string_literal_expr(self)

    @property
    def description(self) -> str:  # noqa: D
        return f"String Literal: {self._literal_value}"

    @property
    def displayed_properties(self) -> List[DisplayedProperty]:  # noqa: D
        return super().displayed_properties + [DisplayedProperty("value", self._literal_value)]

    @property
    def literal_value(self) -> str:  # noqa: D
        return self._literal_value

    @property
    def requires_parenthesis(self) -> bool:  # noqa: D
        return False

    @property
    def execution_parameters(self) -> SqlBindParameters:  # noqa: D
        return SqlBindParameters()

    def __repr__(self) -> str:  # noqa: D
        return f"{self.__class__.__name__}(node_id={self.node_id}, literal_value={self.literal_value})"

    def rewrite(  # noqa: D
        self,
        column_replacements: Optional[SqlColumnReplacements] = None,
        should_render_table_alias: Optional[bool] = None,
    ) -> SqlExpressionNode:
        return self

    @property
    def lineage(self) -> SqlExpressionTreeLineage:  # noqa: D
        return SqlExpressionTreeLineage(other_exprs=(self,))

    def matches(self, other: SqlExpressionNode) -> bool:  # noqa: D
        if not isinstance(other, SqlStringLiteralExpression):
            return False
        return self.literal_value == other.literal_value


@dataclass(frozen=True)
class SqlColumnReference:
    """Used with string expressions to specify what columns are referred to in the string expression."""

    table_alias: str
    column_name: str


class SqlColumnReferenceExpression(SqlExpressionNode):
    """An expression that evaluates to the value of a column in one of the sources in the select query.

    e.g. my_table.my_column
    """

    def __init__(self, col_ref: SqlColumnReference, should_render_table_alias: bool = True) -> None:
        """Constructor.

        Args:
            col_ref: the associated column reference.
            should_render_table_alias: When converting this to SQL text, whether the table alias needed to be included.
            e.g. "foo.bar" vs "bar".
        """
        self._col_ref = col_ref
        self._should_render_table_alias = should_render_table_alias
        super().__init__(node_id=self.create_unique_id(), parent_nodes=[])

    @classmethod
    def id_prefix(cls) -> str:  # noqa: D
        return SQL_EXPR_COLUMN_REFERENCE_ID_PREFIX

    def accept(self, visitor: SqlExpressionNodeVisitor) -> VisitorOutputT:  # noqa: D
        return visitor.visit_column_reference_expr(self)

    @property
    def col_ref(self) -> SqlColumnReference:  # noqa: D
        return self._col_ref

    @property
    def description(self) -> str:  # noqa: D
        return f"Column: {self.col_ref}"

    @property
    def displayed_properties(self) -> List[DisplayedProperty]:  # noqa: D
        return super().displayed_properties + [DisplayedProperty("col_ref", self.col_ref)]

    @property
    def requires_parenthesis(self) -> bool:  # noqa: D
        return False

    @property
    def as_column_reference_expression(self) -> Optional[SqlColumnReferenceExpression]:  # noqa:
        return self

    def rewrite(  # noqa: D
        self,
        column_replacements: Optional[SqlColumnReplacements] = None,
        should_render_table_alias: Optional[bool] = None,
    ) -> SqlExpressionNode:

        # TODO: Hack to work around the fact our test data set contains "user", which is a reserved keyword.
        # We should migrate "user" -> "user_id" in the test set.
        # This will force "user" to be rendered as "table_alias.user"
        if self.col_ref.column_name == "user":
            should_render_table_alias = True

        if column_replacements:
            replacement = column_replacements.get_replacement(self.col_ref)
            if replacement:
                if should_render_table_alias is not None:
                    return replacement.rewrite(should_render_table_alias=should_render_table_alias)
                else:
                    return replacement
            else:
                if should_render_table_alias is not None:
                    return SqlColumnReferenceExpression(
                        col_ref=self.col_ref, should_render_table_alias=should_render_table_alias
                    )
                return self

        if should_render_table_alias is not None:
            return SqlColumnReferenceExpression(
                col_ref=self.col_ref, should_render_table_alias=should_render_table_alias
            )

        return SqlColumnReferenceExpression(
            col_ref=self.col_ref, should_render_table_alias=self.should_render_table_alias
        )

    @property
    def lineage(self) -> SqlExpressionTreeLineage:  # noqa: D
        return SqlExpressionTreeLineage(column_reference_exprs=(self,))

    @property
    def should_render_table_alias(self) -> bool:  # noqa: D
        return self._should_render_table_alias

    def matches(self, other: SqlExpressionNode) -> bool:  # noqa: D
        if not isinstance(other, SqlColumnReferenceExpression):
            return False
        return self.col_ref == other.col_ref


class SqlColumnAliasReferenceExpression(SqlExpressionNode):
    """An expression that evaluates to the alias of a column, but is not qualified with a table alias.

    e.g. SELECT foo vs. SELECT a.foo.

    This is needed to handle some exceptional cases, but in general, this should not be used as it can lead to
    ambiguities.
    """

    def __init__(self, column_alias: str) -> None:  # noqa: D
        self._column_alias = column_alias
        super().__init__(node_id=self.create_unique_id(), parent_nodes=[])

    @classmethod
    def id_prefix(cls) -> str:  # noqa: D
        return SQL_EXPR_COLUMN_REFERENCE_ID_PREFIX

    def accept(self, visitor: SqlExpressionNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D
        return visitor.visit_column_alias_reference_expr(self)

    @property
    def column_alias(self) -> str:  # noqa: D
        return self._column_alias

    @property
    def description(self) -> str:  # noqa: D
        return f"Unqualified Column: {self._column_alias}"

    @property
    def displayed_properties(self) -> List[DisplayedProperty]:  # noqa: D
        return super().displayed_properties + [DisplayedProperty("column_alias", self.column_alias)]

    @property
    def requires_parenthesis(self) -> bool:  # noqa: D
        return False

    @property
    def as_column_reference_expression(self) -> Optional[SqlColumnReferenceExpression]:  # noqa:
        return None

    def rewrite(  # noqa: D
        self,
        column_replacements: Optional[SqlColumnReplacements] = None,
        should_render_table_alias: Optional[bool] = None,
    ) -> SqlExpressionNode:
        if column_replacements:
            raise NotImplementedError()
        return self

    @property
    def lineage(self) -> SqlExpressionTreeLineage:  # noqa: D
        return SqlExpressionTreeLineage(column_alias_reference_exprs=(self,))

    def matches(self, other: SqlExpressionNode) -> bool:  # noqa: D
        if not isinstance(other, SqlColumnAliasReferenceExpression):
            return False
        return self.column_alias == other.column_alias


class SqlComparison(Enum):  # noqa: D
    LESS_THAN = "<"
    GREATER_THAN = ">"
    LESS_THAN_OR_EQUALS = "<="
    GREATER_THAN_OR_EQUALS = ">="
    EQUALS = "="


class SqlComparisonExpression(SqlExpressionNode):
    """A comparison using >, <, <=, >=, =.

    e.g. my_table.my_column = a + b
    """

    def __init__(self, left_expr: SqlExpressionNode, comparison: SqlComparison, right_expr: SqlExpressionNode) -> None:
        """Constructor.

        Args:
            left_expr: The expression on the left side of the =
            comparison: The comparison to use on expressions
            right_expr: The expression on the right side of the =
        """
        self._left_expr = left_expr
        self._comparison = comparison
        self._right_expr = right_expr
        super().__init__(node_id=self.create_unique_id(), parent_nodes=[self._left_expr, self._right_expr])

    @classmethod
    def id_prefix(cls) -> str:  # noqa: D
        return SQL_EXPR_COMPARISON_ID_PREFIX

    def accept(self, visitor: SqlExpressionNodeVisitor) -> VisitorOutputT:  # noqa: D
        return visitor.visit_comparison_expr(self)

    @property
    def left_expr(self) -> SqlExpressionNode:  # noqa: D
        return self._left_expr

    @property
    def right_expr(self) -> SqlExpressionNode:  # noqa: D
        return self._right_expr

    @property
    def description(self) -> str:  # noqa: D
        return f"{self._comparison.value} Expression"

    @property
    def displayed_properties(self) -> List[DisplayedProperty]:  # noqa: D
        return super().displayed_properties + [
            DisplayedProperty("left_expr", self.left_expr),
            DisplayedProperty("comparison", self.comparison.value),
            DisplayedProperty("right_expr", self.right_expr),
        ]

    @property
    def requires_parenthesis(self) -> bool:  # noqa: D
        return True

    @property
    def comparison(self) -> SqlComparison:  # noqa: D
        return self._comparison

    def rewrite(  # noqa: D
        self,
        column_replacements: Optional[SqlColumnReplacements] = None,
        should_render_table_alias: Optional[bool] = None,
    ) -> SqlExpressionNode:
        return SqlComparisonExpression(
            left_expr=self.left_expr.rewrite(column_replacements, should_render_table_alias),
            comparison=self.comparison,
            right_expr=self.right_expr.rewrite(column_replacements, should_render_table_alias),
        )

    @property
    def lineage(self) -> SqlExpressionTreeLineage:  # noqa: D
        return SqlExpressionTreeLineage.combine(
            tuple(x.lineage for x in self.parent_nodes) + (SqlExpressionTreeLineage(other_exprs=(self,)),)
        )

    def matches(self, other: SqlExpressionNode) -> bool:  # noqa: D
        if not isinstance(other, SqlComparisonExpression):
            return False
        return self.comparison == other.comparison and self._parents_match(other)


class SqlFunction(Enum):
    """Names of known SQL functions like SUM() in SELECT SUM(...)

    Values are the SQL string to be used in rendering.
    """

    # Aggregation functions
    AVERAGE = "AVG"
    # Most engines implement count_distinct as a leading DISTINCT keyword like `COUNT(DISTINCT col1, col2...)`
    COUNT_DISTINCT = "COUNT"
    MAX = "MAX"
    MIN = "MIN"
    SUM = "SUM"

    # Field management functions
    COALESCE = "COALESCE"
    CONCAT = "CONCAT"

    @staticmethod
    def distinct_aggregation_functions() -> Sequence[SqlFunction]:
        """Returns a tuple containg all currently-supported DISTINCT type aggregation functions

        This is not a property because properties don't play nicely with static/class methods.
        """
        return (SqlFunction.COUNT_DISTINCT,)

    @staticmethod
    def is_distinct_aggregation(function_type: SqlFunction) -> bool:
        """Convenience method to check if the input function is a distinct aggregation type.

        This is useful in SQL expression rendering, as most engines implement distinct as a keyword modifier on
        an argument (e.g., `COUNT(DISTINCT expr)`) while our model handling and rendering supports distinct functions
        (e.g., `count_distinct(expr)`) and otherwise does not guarantee correct results when the DISTINCT keyword
        is used.
        """
        return function_type in SqlFunction.distinct_aggregation_functions()

    @staticmethod
    def is_aggregation(function_type: SqlFunction) -> bool:
        """Returns true if the given function is an aggregation function."""

        return function_type in (
            SqlFunction.AVERAGE,
            SqlFunction.COUNT_DISTINCT,
            SqlFunction.MAX,
            SqlFunction.MIN,
            SqlFunction.SUM,
        )

    @staticmethod
    def from_aggregation_type(aggregation_type: AggregationType) -> SqlFunction:
        """Converter method to get the SqlFunction value corresponding to the given AggregationType

        Make sure to leave the else: block in place, as this enforces an exhaustive switch through the
        AggregationType enumeration values.
        """
        if aggregation_type is AggregationType.AVERAGE:
            return SqlFunction.AVERAGE
        elif aggregation_type is AggregationType.COUNT_DISTINCT:
            return SqlFunction.COUNT_DISTINCT
        elif aggregation_type is AggregationType.MAX:
            return SqlFunction.MAX
        elif aggregation_type is AggregationType.MIN:
            return SqlFunction.MIN
        elif aggregation_type is AggregationType.SUM:
            return SqlFunction.SUM
        elif aggregation_type is AggregationType.SUM_BOOLEAN or aggregation_type is AggregationType.BOOLEAN:
            raise RuntimeError(
                f"Unhandled boolean aggregation type {aggregation_type} - this should have been transformed to SUM "
                "during model parsing."
            )

        assert_values_exhausted(aggregation_type)


class SqlFunctionExpression(SqlExpressionNode):
    """A function expression like SUM(1)."""

    @staticmethod
    def from_aggregation_type(
        aggregation_type: AggregationType, sql_column_expression: SqlColumnReferenceExpression
    ) -> SqlFunctionExpression:
        """Given the aggregation type, return an SQL function expression that does that aggregation on the given col."""
        return SqlFunctionExpression(
            sql_function=SqlFunction.from_aggregation_type(aggregation_type=aggregation_type),
            sql_function_args=[sql_column_expression],
        )

    def __init__(self, sql_function: SqlFunction, sql_function_args: List[SqlExpressionNode]) -> None:
        """Constructor.

        Args:
            sql_function: The function that this represents.
            sql_function_args: The arguments that should go into the function. e.g. for "CONCAT(a, b)", the arg
            expressions should be "a" and "b".
        """
        self._sql_function = sql_function
        self._sql_function_args = sql_function_args
        super().__init__(node_id=self.create_unique_id(), parent_nodes=sql_function_args)

    @classmethod
    def id_prefix(cls) -> str:  # noqa: D
        return SQL_EXPR_FUNCTION_ID_PREFIX

    @property
    def requires_parenthesis(self) -> bool:  # noqa: D
        return False

    def accept(self, visitor: SqlExpressionNodeVisitor) -> VisitorOutputT:  # noqa: D
        return visitor.visit_function_expr(self)

    @property
    def description(self) -> str:  # noqa: D
        return f"{self._sql_function.value} Expression"

    @property
    def displayed_properties(self) -> List[DisplayedProperty]:  # noqa: D
        return (
            super().displayed_properties
            + [DisplayedProperty("function", self.sql_function)]
            + [DisplayedProperty("argument", x) for x in self.sql_function_args]
        )

    @property
    def sql_function(self) -> SqlFunction:  # noqa: D
        return self._sql_function

    @property
    def sql_function_args(self) -> List[SqlExpressionNode]:  # noqa: D
        return self._sql_function_args

    def __repr__(self) -> str:  # noqa: D
        return f"{self.__class__.__name__}(node_id={self.node_id}, sql_function={self.sql_function.name})"

    def rewrite(  # noqa: D
        self,
        column_replacements: Optional[SqlColumnReplacements] = None,
        should_render_table_alias: Optional[bool] = None,
    ) -> SqlExpressionNode:
        return SqlFunctionExpression(
            sql_function=self.sql_function,
            sql_function_args=[
                x.rewrite(column_replacements, should_render_table_alias) for x in self.sql_function_args
            ],
        )

    @property
    def lineage(self) -> SqlExpressionTreeLineage:  # noqa: D
        return SqlExpressionTreeLineage.combine(
            tuple(x.lineage for x in self.parent_nodes) + (SqlExpressionTreeLineage(function_exprs=(self,)),)
        )

    def matches(self, other: SqlExpressionNode) -> bool:  # noqa: D
        if not isinstance(other, SqlFunctionExpression):
            return False
        return self.sql_function == other.sql_function and self._parents_match(other)


class SqlNullExpression(SqlExpressionNode):
    """Represents NULL."""

    def __init__(self) -> None:  # noqa: D
        super().__init__(node_id=self.create_unique_id(), parent_nodes=[])

    @classmethod
    def id_prefix(cls) -> str:  # noqa: D
        return SQL_EXPR_NULL_PREFIX

    @property
    def requires_parenthesis(self) -> bool:  # noqa: D
        return False

    def accept(self, visitor: SqlExpressionNodeVisitor) -> VisitorOutputT:  # noqa: D
        return visitor.visit_null_expr(self)

    @property
    def description(self) -> str:  # noqa: D
        return "NULL Expression"

    def rewrite(  # noqa: D
        self,
        column_replacements: Optional[SqlColumnReplacements] = None,
        should_render_table_alias: Optional[bool] = None,
    ) -> SqlExpressionNode:
        return self

    @property
    def lineage(self) -> SqlExpressionTreeLineage:  # noqa: D
        return SqlExpressionTreeLineage(other_exprs=(self,))

    def matches(self, other: SqlExpressionNode) -> bool:  # noqa: D
        return isinstance(other, SqlNullExpression)


class SqlLogicalOperator(Enum):
    """List all supported binary logical operator expressions.

    Value is the SQL string used when rendering the operator.
    """

    AND = "AND"
    OR = "OR"


class SqlLogicalExpression(SqlExpressionNode):
    """A logical expression like "a AND b AND c"."""

    def __init__(self, operator: SqlLogicalOperator, args: Tuple[SqlExpressionNode, ...]) -> None:  # noqa: D
        self._operator = operator
        super().__init__(node_id=self.create_unique_id(), parent_nodes=list(args))

    @classmethod
    def id_prefix(cls) -> str:  # noqa: D
        return SQL_EXPR_LOGICAL_OPERATOR_PREFIX

    @property
    def requires_parenthesis(self) -> bool:  # noqa: D
        return True

    def accept(self, visitor: SqlExpressionNodeVisitor) -> VisitorOutputT:  # noqa: D
        return visitor.visit_logical_expr(self)

    @property
    def description(self) -> str:  # noqa: D
        return f"Logical Operator {self._operator.value}"

    @property
    def args(self) -> Sequence[SqlExpressionNode]:  # noqa: D
        return self.parent_nodes

    @property
    def operator(self) -> SqlLogicalOperator:  # noqa: D
        return self._operator

    def rewrite(  # noqa: D
        self,
        column_replacements: Optional[SqlColumnReplacements] = None,
        should_render_table_alias: Optional[bool] = None,
    ) -> SqlExpressionNode:
        return SqlLogicalExpression(
            operator=self.operator,
            args=tuple(x.rewrite(column_replacements, should_render_table_alias) for x in self.args),
        )

    @property
    def lineage(self) -> SqlExpressionTreeLineage:  # noqa: D
        return SqlExpressionTreeLineage.combine(
            tuple(x.lineage for x in self.parent_nodes) + (SqlExpressionTreeLineage(other_exprs=(self,)),)
        )

    def matches(self, other: SqlExpressionNode) -> bool:  # noqa: D
        if not isinstance(other, SqlLogicalExpression):
            return False
        return self.operator == other.operator and self._parents_match(other)


class SqlIsNullExpression(SqlExpressionNode):
    """An IS NULL expression like "foo IS NULL"."""

    def __init__(self, arg: SqlExpressionNode) -> None:  # noqa: D
        self._arg = arg
        super().__init__(node_id=self.create_unique_id(), parent_nodes=[arg])

    @classmethod
    def id_prefix(cls) -> str:  # noqa: D
        return SQL_EXPR_IS_NULL_PREFIX

    @property
    def requires_parenthesis(self) -> bool:  # noqa: D
        return True

    def accept(self, visitor: SqlExpressionNodeVisitor) -> VisitorOutputT:  # noqa: D
        return visitor.visit_is_null_expr(self)

    @property
    def description(self) -> str:  # noqa: D
        return "IS NULL Expression"

    @property
    def arg(self) -> SqlExpressionNode:  # noqa: D
        return self._arg

    def rewrite(  # noqa: D
        self,
        column_replacements: Optional[SqlColumnReplacements] = None,
        should_render_table_alias: Optional[bool] = None,
    ) -> SqlExpressionNode:
        return SqlIsNullExpression(arg=self.arg.rewrite(column_replacements, should_render_table_alias))

    @property
    def lineage(self) -> SqlExpressionTreeLineage:  # noqa: D
        return SqlExpressionTreeLineage.combine([self.arg.lineage, SqlExpressionTreeLineage(other_exprs=(self,))])

    def matches(self, other: SqlExpressionNode) -> bool:  # noqa: D
        if not isinstance(other, SqlIsNullExpression):
            return False
        return self._parents_match(other)


class SqlTimeDeltaExpression(SqlExpressionNode):
    """create time delta between eg `DATE_SUB(ds, 2, month)`"""

    def __init__(  # noqa: D
        self,
        arg: SqlExpressionNode,
        count: int,
        granularity: TimeGranularity,
        grain_to_date: Optional[TimeGranularity] = None,
    ) -> None:
        super().__init__(node_id=self.create_unique_id(), parent_nodes=[arg])
        self._count = count
        self._time_granularity = granularity
        self._arg = arg
        self._grain_to_date = grain_to_date

    @classmethod
    def id_prefix(cls) -> str:  # noqa: D
        return SQL_EXPR_IS_NULL_PREFIX

    @property
    def requires_parenthesis(self) -> bool:  # noqa: D
        return False

    def accept(self, visitor: SqlExpressionNodeVisitor) -> VisitorOutputT:  # noqa: D
        return visitor.visit_time_delta_expr(self)

    @property
    def description(self) -> str:  # noqa: D
        return "Time delta"

    @property
    def arg(self) -> SqlExpressionNode:  # noqa: D
        return self._arg

    @property
    def grain_to_date(self) -> Optional[TimeGranularity]:  # noqa: D
        return self._grain_to_date

    @property
    def count(self) -> int:  # noqa: D
        return self._count

    @property
    def granularity(self) -> TimeGranularity:  # noqa: D
        return self._time_granularity

    def rewrite(  # noqa: D
        self,
        column_replacements: Optional[SqlColumnReplacements] = None,
        should_render_table_alias: Optional[bool] = None,
    ) -> SqlExpressionNode:
        return SqlTimeDeltaExpression(
            arg=self.arg.rewrite(column_replacements, should_render_table_alias),
            count=self.count,
            granularity=self.granularity,
            grain_to_date=self.grain_to_date,
        )

    @property
    def lineage(self) -> SqlExpressionTreeLineage:  # noqa: D
        return SqlExpressionTreeLineage.combine(
            tuple(x.lineage for x in self.parent_nodes) + (SqlExpressionTreeLineage(other_exprs=(self,)),)
        )

    def matches(self, other: SqlExpressionNode) -> bool:  # noqa: D
        if not isinstance(other, SqlTimeDeltaExpression):
            return False
        return (
            self.count == other.count
            and self.granularity == other.granularity
            and self.grain_to_date == other.grain_to_date
            and self._parents_match(other)
        )


class SqlCastToTimestampExpression(SqlExpressionNode):
    """Cast to the timestamp type like CAST('2020-01-01' AS TIMESTAMP)"""

    def __init__(self, arg: SqlExpressionNode) -> None:  # noqa: D
        super().__init__(node_id=self.create_unique_id(), parent_nodes=[arg])

    @classmethod
    def id_prefix(cls) -> str:  # noqa: D
        return SQL_EXPR_IS_NULL_PREFIX

    @property
    def requires_parenthesis(self) -> bool:  # noqa: D
        return False

    def accept(self, visitor: SqlExpressionNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D
        return visitor.visit_cast_to_timestamp_expr(self)

    @property
    def description(self) -> str:  # noqa: D
        return "Cast to Timestamp"

    @property
    def arg(self) -> SqlExpressionNode:  # noqa: D
        assert len(self.parent_nodes) == 1
        return self.parent_nodes[0]

    def rewrite(  # noqa: D
        self,
        column_replacements: Optional[SqlColumnReplacements] = None,
        should_render_table_alias: Optional[bool] = None,
    ) -> SqlExpressionNode:
        return SqlCastToTimestampExpression(arg=self.arg.rewrite(column_replacements, should_render_table_alias))

    @property
    def lineage(self) -> SqlExpressionTreeLineage:  # noqa: D
        return SqlExpressionTreeLineage.combine(
            tuple(x.lineage for x in self.parent_nodes) + (SqlExpressionTreeLineage(other_exprs=(self,)),)
        )

    def matches(self, other: SqlExpressionNode) -> bool:  # noqa: D
        if not isinstance(other, SqlCastToTimestampExpression):
            return False
        return self._parents_match(other)


class SqlDateTruncExpression(SqlExpressionNode):
    """Apply a date trunc to a column like CAST('2020-01-01' AS TIMESTAMP)"""

    def __init__(self, time_granularity: TimeGranularity, arg: SqlExpressionNode) -> None:
        """Constructor.

        Args:
            time_granularity: the granularity to DATE_TRUNC() to.
            arg: the value to DATE_TRUNC().
        """
        self._time_granularity = time_granularity
        super().__init__(node_id=self.create_unique_id(), parent_nodes=[arg])

    @classmethod
    def id_prefix(cls) -> str:  # noqa: D
        return SQL_EXPR_DATE_TRUNC

    @property
    def requires_parenthesis(self) -> bool:  # noqa: D
        return False

    def accept(self, visitor: SqlExpressionNodeVisitor) -> VisitorOutputT:  # noqa: D
        return visitor.visit_date_trunc_expr(self)

    @property
    def description(self) -> str:  # noqa: D
        return f"DATE_TRUNC() to {self.time_granularity}"

    @property
    def time_granularity(self) -> TimeGranularity:  # noqa: D
        return self._time_granularity

    @property
    def arg(self) -> SqlExpressionNode:  # noqa: D
        assert len(self.parent_nodes) == 1
        return self.parent_nodes[0]

    def rewrite(  # noqa: D
        self,
        column_replacements: Optional[SqlColumnReplacements] = None,
        should_render_table_alias: Optional[bool] = None,
    ) -> SqlExpressionNode:
        return SqlDateTruncExpression(
            time_granularity=self.time_granularity, arg=self.arg.rewrite(column_replacements, should_render_table_alias)
        )

    @property
    def lineage(self) -> SqlExpressionTreeLineage:  # noqa: D
        return SqlExpressionTreeLineage.combine(
            tuple(x.lineage for x in self.parent_nodes) + (SqlExpressionTreeLineage(other_exprs=(self,)),)
        )

    def matches(self, other: SqlExpressionNode) -> bool:  # noqa: D
        if not isinstance(other, SqlDateTruncExpression):
            return False
        return self.time_granularity == other.time_granularity and self._parents_match(other)


class SqlRatioComputationExpression(SqlExpressionNode):
    """Node for expressing Ratio metrics to allow for appropriate casting to float/double in each engine

    In future we might wish to break this up into a set of nodes, e.g., SqlCastExpression and SqlMathExpression
    or even add CAST to SqlFunctionExpression. However, at this time the only mathematical operation we encode
    is division, and we only use that for ratios. Similarly, the only times we do typecasting are when we are
    coercing timestamps (already handled) or computing ratio metrics.
    """

    def __init__(self, numerator: SqlExpressionNode, denominator: SqlExpressionNode) -> None:
        """Initialize this node for computing a ratio. Expression renderers should handle the casting

        Args:
            numerator: the expression for the numerator in the ratio
            denominator: the expression for the denominator in the ratio
        """
        self._numerator = numerator
        self._denominator = denominator
        super().__init__(node_id=self.create_unique_id(), parent_nodes=[numerator, denominator])

    @classmethod
    def id_prefix(cls) -> str:  # noqa: D
        return SQL_EXPR_RATIO_COMPUTATION

    @property
    def requires_parenthesis(self) -> bool:  # noqa: D
        return False

    def accept(self, visitor: SqlExpressionNodeVisitor) -> VisitorOutputT:  # noqa: D
        return visitor.visit_ratio_computation_expr(self)

    @property
    def description(self) -> str:  # noqa: D
        return "Divide numerator by denominator, with appropriate casting"

    @property
    def numerator(self) -> SqlExpressionNode:  # noqa: D
        return self._numerator

    @property
    def denominator(self) -> SqlExpressionNode:  # noqa: D
        return self._denominator

    def rewrite(  # noqa: D
        self,
        column_replacements: Optional[SqlColumnReplacements] = None,
        should_render_table_alias: Optional[bool] = None,
    ) -> SqlExpressionNode:
        return SqlRatioComputationExpression(
            numerator=self.numerator.rewrite(column_replacements, should_render_table_alias),
            denominator=self.denominator.rewrite(column_replacements, should_render_table_alias),
        )

    @property
    def lineage(self) -> SqlExpressionTreeLineage:  # noqa: D
        return SqlExpressionTreeLineage.combine(
            tuple(x.lineage for x in self.parent_nodes) + (SqlExpressionTreeLineage(other_exprs=(self,)),)
        )

    def matches(self, other: SqlExpressionNode) -> bool:  # noqa: D
        if not isinstance(other, SqlRatioComputationExpression):
            return False
        return self._parents_match(other)


class SqlBetweenExpression(SqlExpressionNode):
    """A BETWEEN clause like `column BETWEEN val1 AND val2`"""

    def __init__(  # noqa: D
        self, column_arg: SqlExpressionNode, left_value: SqlExpressionNode, right_value: SqlExpressionNode
    ) -> None:
        self._column_arg = column_arg
        self._left_value = left_value
        self._right_value = right_value
        super().__init__(node_id=self.create_unique_id(), parent_nodes=[column_arg, left_value, right_value])

    @classmethod
    def id_prefix(cls) -> str:  # noqa: D
        return SQL_EXPR_BETWEEN_PREFIX

    @property
    def requires_parenthesis(self) -> bool:  # noqa: D
        return False

    def accept(self, visitor: SqlExpressionNodeVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D
        return visitor.visit_between_expr(self)

    @property
    def description(self) -> str:  # noqa: D
        return "BETWEEN operator"

    @property
    def column_arg(self) -> SqlExpressionNode:  # noqa: D
        return self._column_arg

    @property
    def left_value(self) -> SqlExpressionNode:  # noqa: D
        return self._left_value

    @property
    def right_value(self) -> SqlExpressionNode:  # noqa: D
        return self._right_value

    def rewrite(  # noqa: D
        self,
        column_replacements: Optional[SqlColumnReplacements] = None,
        should_render_table_alias: Optional[bool] = None,
    ) -> SqlExpressionNode:
        return SqlBetweenExpression(
            column_arg=self.column_arg.rewrite(column_replacements, should_render_table_alias),
            left_value=self.left_value.rewrite(column_replacements, should_render_table_alias),
            right_value=self.right_value.rewrite(column_replacements, should_render_table_alias),
        )

    @property
    def lineage(self) -> SqlExpressionTreeLineage:  # noqa: D
        return SqlExpressionTreeLineage.combine(
            tuple(x.lineage for x in self.parent_nodes) + (SqlExpressionTreeLineage(other_exprs=(self,)),)
        )

    def matches(self, other: SqlExpressionNode) -> bool:  # noqa: D
        if not isinstance(other, SqlBetweenExpression):
            return False
        return self._parents_match(other)
