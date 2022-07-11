from __future__ import annotations

from typing import Callable, List
from metricflow.dataflow.sql_column import SqlColumn

from metricflow.inference.context.data_warehouse import DataWarehouseInferenceContext
from metricflow.inference.rule.base import (
    InferenceRule,
    InferenceSignal,
    InferenceSignalConfidence,
    InferenceSignalNode,
)


ColumnMatcher = Callable[[SqlColumn], bool]


class ColumnMatcherRule(InferenceRule):
    """Inference rule that checks for matches across all columns."""

    def __init__(
        self, matcher: ColumnMatcher, type_node: InferenceSignalNode, confidence: InferenceSignalConfidence
    ) -> None:
        """Initialize the class.

        matcher: a function to determine whether a `SqlColumns` matches. If it does, produce the signal
        type_node: the `InferenceSignalNode` to produce whenever the pattern is matched
        confidence: the `InferenceSignalConfidence` to produce whenever the pattern is matched
        """
        self.matcher = matcher
        self.type_node = type_node
        self.confidence = confidence

    def process(self, warehouse: DataWarehouseInferenceContext) -> List[InferenceSignal]:  # type: ignore
        """Try to match all columns' names with the matching function.

        If they do match, produce a signal with the configured type and confidence.
        """
        matching_columns = [column for column in warehouse.columns if self.matcher(column)]
        signals = [
            InferenceSignal(
                column=column,
                type_node=self.type_node,
                reason=f"Column matched by rule {self.__class__.__name__}.",
                confidence=self.confidence,
            )
            for column in matching_columns
        ]
        return signals
