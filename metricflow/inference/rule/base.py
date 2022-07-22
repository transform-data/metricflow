from __future__ import annotations
from abc import ABC, abstractmethod
from typing import List

from metricflow.inference.models import InferenceSignal
from metricflow.inference.context.data_warehouse import DataWarehouseInferenceContext


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
