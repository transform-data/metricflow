from __future__ import annotations

import logging
import textwrap
from abc import ABC
from collections import namedtuple
from dataclasses import dataclass
from typing import List

import jinja2

from metricflow.sql.sql_bind_parameters import SqlBindParameters
from metricflow.sql.sql_exprs import (
    SqlExpressionNodeVisitor,
    SqlColumnReferenceExpression,
    SqlStringExpression,
    SqlComparisonExpression,
    SqlExpressionNode,
    SqlFunction,
    SqlFunctionExpression,
    SqlNullExpression,
    SqlLogicalExpression,
    SqlStringLiteralExpression,
    SqlIsNullExpression,
    SqlCastToTimestampExpression,
    SqlDateTruncExpression,
    SqlTimeDeltaExpression,
    SqlRatioComputationExpression,
    SqlColumnAliasReferenceExpression,
    SqlBetweenExpression,
)
from metricflow.time.time_granularity import TimeGranularity

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SqlExpressionRenderResult:
    """The result of rendering an SQL expression tree to a string."""

    sql: str
    execution_parameters: SqlBindParameters


class SqlExpressionRenderer(SqlExpressionNodeVisitor[SqlExpressionRenderResult], ABC):
    """Renders SqlExpressions into strings"""

    def render_sql_expr(self, sql_expr: SqlExpressionNode) -> SqlExpressionRenderResult:
        """Render the given expression to a string."""
        return sql_expr.accept(self)

    @property
    def double_data_type(self) -> str:
        """Property for the double data type, for engine-specific type casting

        TODO: Eliminate this in favor of some kind of engine properties struct
        """
        return "DOUBLE"


class DefaultSqlExpressionRenderer(SqlExpressionRenderer):
    """Renders the SQL query plan assuming ANSI SQL."""

    def visit_string_expr(self, node: SqlStringExpression) -> SqlExpressionRenderResult:  # noqa: D
        """Renders an arbitrary string expression like 1+1=2"""
        return SqlExpressionRenderResult(sql=node.sql_expr, execution_parameters=node.execution_parameters)

    def visit_column_reference_expr(self, node: SqlColumnReferenceExpression) -> SqlExpressionRenderResult:  # noqa: D
        """Render a reference to a column in a table like my_table.my_col"""
        return SqlExpressionRenderResult(
            sql=(
                f"{node.col_ref.table_alias}.{node.col_ref.column_name}"
                if node.should_render_table_alias
                else node.col_ref.column_name
            ),
            execution_parameters=SqlBindParameters(),
        )

    def visit_column_alias_reference_expr(  # noqa: D
        self, node: SqlColumnAliasReferenceExpression
    ) -> SqlExpressionRenderResult:
        """Render a reference to a column without a known table alias. e.g. foo.bar vs bar."""
        return SqlExpressionRenderResult(
            sql=node.column_alias,
            execution_parameters=SqlBindParameters(),
        )

    def visit_comparison_expr(self, node: SqlComparisonExpression) -> SqlExpressionRenderResult:
        """Render a comparison expression like 1 = 2"""
        combined_params = SqlBindParameters()

        left_expr_rendered = self.render_sql_expr(node.left_expr)
        combined_params.update(left_expr_rendered.execution_parameters)

        right_expr_rendered = self.render_sql_expr(node.right_expr)
        combined_params.update(right_expr_rendered.execution_parameters)

        # To avoid issues with operator precedence, use parenthesis to group the left / right expressions if they
        # contain operators.
        return SqlExpressionRenderResult(
            # Render a + b = c
            sql=(
                (f"({left_expr_rendered.sql})" if node.left_expr.requires_parenthesis else left_expr_rendered.sql)
                + f" {node.comparison.value} "
                + (f"({right_expr_rendered.sql})" if node.right_expr.requires_parenthesis else right_expr_rendered.sql)
            ),
            execution_parameters=combined_params,
        )

    def visit_function_expr(self, node: SqlFunctionExpression) -> SqlExpressionRenderResult:  # noqa: D
        """Render a function call like CONCAT(a, b)"""
        args_rendered = [self.render_sql_expr(x) for x in node.sql_function_args]
        combined_params = SqlBindParameters()
        for arg_rendered in args_rendered:
            combined_params.update(arg_rendered.execution_parameters)

        distinct_prefix = "DISTINCT " if SqlFunction.is_distinct_aggregation(node.sql_function) else ""
        args_string = ", ".join([x.sql for x in args_rendered])

        return SqlExpressionRenderResult(
            sql=f"{node.sql_function.value}({distinct_prefix}{args_string})",
            execution_parameters=combined_params,
        )

    def visit_null_expr(self, node: SqlNullExpression) -> SqlExpressionRenderResult:  # noqa: D
        return SqlExpressionRenderResult(
            sql="NULL",
            execution_parameters=SqlBindParameters(),
        )

    def visit_string_literal_expr(self, node: SqlStringLiteralExpression) -> SqlExpressionRenderResult:  # noqa: D
        return SqlExpressionRenderResult(
            sql=f"'{node.literal_value}'",
            execution_parameters=SqlBindParameters(),
        )

    def visit_logical_expr(self, node: SqlLogicalExpression) -> SqlExpressionRenderResult:  # noqa: D
        RenderedExpr = namedtuple("RenderedExpr", ["expr", "requires_parenthesis"])
        args_rendered = [RenderedExpr(self.render_sql_expr(x), x.requires_parenthesis) for x in node.args]
        combined_parameters = SqlBindParameters()
        args_sql: List[str] = []

        can_be_rendered_in_one_line = sum(len(x.expr.sql) for x in args_rendered) < 60

        for arg_rendered in args_rendered:
            combined_parameters.update(arg_rendered.expr.execution_parameters)
            arg_sql = self._render_logical_arg(
                arg_rendered.expr, arg_rendered.requires_parenthesis, render_in_one_line=can_be_rendered_in_one_line
            )
            args_sql.append(arg_sql)

        sql = f" {node.operator.value} ".join(args_sql)

        return SqlExpressionRenderResult(
            sql=sql,
            execution_parameters=combined_parameters,
        )

    @staticmethod
    def _render_logical_arg(
        arg_rendered: SqlExpressionRenderResult, requires_parenthesis: bool, render_in_one_line: bool
    ) -> str:
        # Put everything on 1 line for short expressions, but otherwise put one expression per line
        # Note: multi-line expressions are always enclosed in parentheses.

        # e.g.
        # Put everything on 1 line for short expressions like:
        #
        # (1 < 2) AND foo
        #
        # but for long expressions do:
        #
        #  (
        #    some_long_expression1
        #  ) AND (
        #    some_long_expression2
        #  ) AND (
        #    some_long_expression3
        # )

        if render_in_one_line:
            return arg_rendered.sql if not requires_parenthesis else f"({arg_rendered.sql})"
        else:
            return (
                jinja2.Template(
                    textwrap.dedent(
                        """\
                    (
                      {{ arg_sql | indent(2) }}
                    )
                    """
                    )
                )
                .render(arg_sql=arg_rendered.sql)
                .rstrip()
            )

    def visit_is_null_expr(self, node: SqlIsNullExpression) -> SqlExpressionRenderResult:  # noqa: D
        arg_rendered = self.render_sql_expr(node.arg)

        return SqlExpressionRenderResult(
            sql=f"{arg_rendered.sql} IS NULL" if not node.arg.requires_parenthesis else f"({arg_rendered.sql}) IS NULL",
            execution_parameters=arg_rendered.execution_parameters,
        )

    def visit_cast_to_timestamp_expr(self, node: SqlCastToTimestampExpression) -> SqlExpressionRenderResult:  # noqa: D
        arg_rendered = self.render_sql_expr(node.arg)
        return SqlExpressionRenderResult(
            sql=f"CAST({arg_rendered.sql} AS TIMESTAMP)",
            execution_parameters=arg_rendered.execution_parameters,
        )

    def visit_date_trunc_expr(self, node: SqlDateTruncExpression) -> SqlExpressionRenderResult:  # noqa: D
        arg_rendered = self.render_sql_expr(node.arg)

        return SqlExpressionRenderResult(
            sql=f"DATE_TRUNC('{node.time_granularity.value}', {arg_rendered.sql})",
            execution_parameters=arg_rendered.execution_parameters,
        )

    def visit_time_delta_expr(self, node: SqlTimeDeltaExpression) -> SqlExpressionRenderResult:  # noqa: D
        arg_rendered = node.arg.accept(self)
        if node.grain_to_date:
            return SqlExpressionRenderResult(
                sql=f"DATE_TRUNC('{node.granularity.value}', {arg_rendered.sql}::timestamp)",
                execution_parameters=arg_rendered.execution_parameters,
            )

        count = node.count
        granularity = node.granularity
        if granularity == TimeGranularity.QUARTER:
            granularity = TimeGranularity.MONTH
            count *= 3
        return SqlExpressionRenderResult(
            sql=f"DATEADD({granularity.value}, -{count}, {arg_rendered.sql})",
            execution_parameters=arg_rendered.execution_parameters,
        )

    def visit_ratio_computation_expr(self, node: SqlRatioComputationExpression) -> SqlExpressionRenderResult:
        """Render the ratio computation for a ratio metric

        This requires both a type cast to a floating point type (default to DOUBLE, engine-permitting) and
        the requisite division between numerator and denominator
        """
        rendered_numerator = self.render_sql_expr(node.numerator)
        rendered_denominator = self.render_sql_expr(node.denominator)

        numerator_sql = f"CAST({rendered_numerator.sql} AS {self.double_data_type})"
        denominator_sql = f"CAST(NULLIF({rendered_denominator.sql}, 0) AS {self.double_data_type})"

        execution_parameters = SqlBindParameters()
        execution_parameters.update(rendered_numerator.execution_parameters)
        execution_parameters.update(rendered_denominator.execution_parameters)

        return SqlExpressionRenderResult(
            sql=f"{numerator_sql} / {denominator_sql}",
            execution_parameters=execution_parameters,
        )

    def visit_between_expr(self, node: SqlBetweenExpression) -> SqlExpressionRenderResult:  # noqa: D
        rendered_column_arg = self.render_sql_expr(node.column_arg)
        rendered_left_value = self.render_sql_expr(node.left_value)
        rendered_right_value = self.render_sql_expr(node.right_value)

        execution_parameters = SqlBindParameters()
        execution_parameters.update(rendered_column_arg.execution_parameters)
        execution_parameters.update(rendered_left_value.execution_parameters)
        execution_parameters.update(rendered_right_value.execution_parameters)

        return SqlExpressionRenderResult(
            sql=f"{rendered_column_arg.sql} BETWEEN {rendered_left_value.sql} AND {rendered_right_value.sql}",
            execution_parameters=execution_parameters,
        )
