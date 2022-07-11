from __future__ import annotations
from abc import ABC, abstractmethod

from dataclasses import dataclass
from enum import Enum
import logging
from typing import List, Optional, Tuple, Type

from metricflow.dataflow.sql_column import SqlColumn
from metricflow.inference.context.base import InferenceContext
from metricflow.inference.context.data_warehouse import DataWarehouseInferenceContext


logger = logging.getLogger(__name__)


class InferenceSignalConfidence(Enum):
    """A discrete enumeration of possible confidence values for an inference signal.

    We chose discrete confidence values instead of a continuous range (e.g a float between 0 and 1)
    to standardize confidence outputs between heuristic rules. We want to avoid different rules
    assuming different values for, say, "medium" or "high" confidence, since this could skew
    results in favor/against certain rules.
    """

    FOR_SURE = 3
    HIGH = 2
    MEDIUM = 1
    LOW = 0


class InferenceSignalNode(ABC):
    """A node in the inference signal type hierarchy.

    This class can be used to assembly a type hierarchy tree. It can be used by heuristics
    to check whether signals produced by rules are conflicting or complimentary, relying
    on the property that sibling nodes are mutually exclusive in the hierarchy.
    """

    def __init__(self, parent: Optional[InferenceSignalNode]) -> None:  # noqa: D
        self.parent = parent
        self.children: List[InferenceSignalNode] = []

        if parent is not None:
            parent.children.append(self)

    @property
    def ancestors(self) -> List[InferenceSignalNode]:
        """The list of all ancestors for this node, from the root to the direct parent."""
        if self.parent is None:
            return []

        return self.parent.ancestors + [self.parent]

    def is_descendant(self, other: InferenceSignalNode) -> bool:
        """Whether self is a descendant of other."""
        return other in self.ancestors


# This is kinda horrible but there's no way of instancing the tree with type safety without
# hardcoding it. Having some magic that dynamically assigns attributes could work, but then
# we lose IDE autocompletion and static checking
class _TreeNodes:  # noqa: D
    root = InferenceSignalNode(None)
    id = InferenceSignalNode(root)
    foreign_id = InferenceSignalNode(id)
    unique_id = InferenceSignalNode(id)
    primary_id = InferenceSignalNode(unique_id)
    dimension = InferenceSignalNode(root)
    time_dimension = InferenceSignalNode(dimension)
    categorical_dimension = InferenceSignalNode(dimension)
    measure = InferenceSignalNode(root)


class InferenceSignalType:
    """All possible inference signal types."""

    UNKNOWN = _TreeNodes.root

    class ID:
        """Indicates a column was inferred to be an ID."""

        UNKNOWN = _TreeNodes.id
        FOREIGN = _TreeNodes.foreign_id
        UNIQUE = _TreeNodes.unique_id
        PRIMARY = _TreeNodes.primary_id

    class DIMENSION:
        """Indicates a column was inferred to be a dimension."""

        UNKNOWN = _TreeNodes.dimension
        TIME = _TreeNodes.time_dimension
        CATEGORICAL = _TreeNodes.categorical_dimension

    class MEASURE:
        """Indicates a column was inferred to be a measure."""

        UNKNOWN = _TreeNodes.measure


@dataclass(frozen=True)
class InferenceSignal:
    """Encapsulates a piece of evidence about a column produced by an inference rule.

    column: the target column for this signal
    type_node: the type node of the signal
    reason: a human-readable string that explains why this signal was produced. It may
        eventually reach the user's eyeballs.
    confidence: the confidence that this signal is correct.
    """

    column: SqlColumn
    type_node: InferenceSignalNode
    reason: str
    confidence: InferenceSignalConfidence


RequiredContextsType = Tuple[Type[InferenceContext], ...]


class InferenceRule(ABC):
    """Implements some sort of heuristic that produces signals about columns.

    An inference rule produces zero or more `InferenceSignal` instances about whatever
    columns it thinks it should, based on input `InferenceContext`s.

    Concrete implementations should aim to be short and modularized. It is preferred to
    compose multiple small rules that each produce a signal type than to make one large
    rule with complex logic to produce a bunch of signals.

    It currently only accepts DataWarehouseInferenceContext as input, but this will probably
    be generalized later.
    """

    @abstractmethod
    def process(self, warehouse: DataWarehouseInferenceContext) -> List[InferenceSignal]:
        """The actual rule implementation that returns a list of signals based on the input contexts."""
        raise NotImplementedError
