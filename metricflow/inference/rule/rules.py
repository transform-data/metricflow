from __future__ import annotations
from abc import abstractmethod

from typing import Callable, List, TypeVar

from metricflow.inference.context.data_warehouse import (
    ColumnProperties,
    DataWarehouseInferenceContext,
)
from metricflow.inference.rule.base import InferenceRule
from metricflow.inference.models import (
    InferenceSignal,
    InferenceSignalConfidence,
    InferenceSignalNode,
)

T = TypeVar("T", bound="ColumnMatcherRule")

ColumnMatcher = Callable[[T, ColumnProperties], bool]


class ColumnMatcherRule(InferenceRule):
    """Inference rule that checks for matches across all columns.

    This is a useful shortcut for making rules that match columns one by one with preset confidence
    values, types and match reasons.

    If you need a more specific rule with varying confidence, column cross-checking and that outputs
    multiple types, inherit from `InferenceRule` directly.

    type_node: the `InferenceSignalNode` to produce whenever the pattern is matched
    confidence: the `InferenceSignalConfidence` to produce whenever the pattern is matched
    complimentary_signal: whether the produced signal should be complimentary or not
    match_reason: a human-readable string of the reason why this was matched
    """

    type_node: InferenceSignalNode
    confidence: InferenceSignalConfidence
    complimentary_signal: bool
    match_reason: str

    @abstractmethod
    def match_column(self, props: ColumnProperties) -> bool:
        """A function to determine whether `ColumnProperties` matches. If it does, produce the signal"""
        raise NotImplementedError

    def process(self, warehouse: DataWarehouseInferenceContext) -> List[InferenceSignal]:  # type: ignore
        """Try to match all columns' properties with the matching function.

        If they do match, produce a signal with the configured type and confidence.
        """
        matching_columns = [column for column, props in warehouse.columns.items() if self.match_column(props)]
        signals = [
            InferenceSignal(
                column=column,
                type_node=self.type_node,
                reason=self.match_reason,
                confidence=self.confidence,
                is_complimentary=self.complimentary_signal,
            )
            for column in matching_columns
        ]
        return signals


class LowCardinalityRatioRule(ColumnMatcherRule):
    """Inference rule that checks for string columns with low cardinality to count ratio.

    The ratio is calculated as `distinct_count/(count - null_count)`.
    """

    def __init__(self, cardinality_count_ratio_threshold: float) -> None:
        """Initialize the rule.

        cardinality_count_ratio_threshold: rations below this threshold will match.
        """
        assert cardinality_count_ratio_threshold >= 0 and cardinality_count_ratio_threshold <= 1
        self.threshold = cardinality_count_ratio_threshold
        super().__init__()

    match_reason = "Column has low cardinality"

    def match_column(self, props: ColumnProperties) -> bool:  # noqa: D
        denom = props.row_count - props.null_count

        # undefined ratio
        if denom == 0:
            return False

        ratio = props.distinct_row_count / denom
        return ratio < self.threshold
