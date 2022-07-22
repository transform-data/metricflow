from abc import ABC, abstractmethod
from typing import List

from metricflow.dataflow.sql_column import SqlColumn
from metricflow.inference.models import InferenceResult, InferenceSignal


class InferenceSolver(ABC):
    """Base class for inference solvers.

    Inference solvers implement algorithms for making a final decision over what a column is or isn't, based on
    signals produced by inference rules for that column.
    """

    @abstractmethod
    def solve_column(self, column: SqlColumn, signals: List[InferenceSignal]) -> InferenceResult:
        """Make a decision about a column based on its input signals."""
        raise NotImplementedError
