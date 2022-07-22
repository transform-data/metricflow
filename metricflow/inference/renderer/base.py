from abc import ABC
from typing import List

from metricflow.inference.models import InferenceResult


class InferenceRenderer(ABC):
    """Render inference results into some format."""

    def render(self, results: List[InferenceResult]) -> None:
        """Render a set of inference results into the screen, some file, the network or whatever."""
        raise NotImplementedError
