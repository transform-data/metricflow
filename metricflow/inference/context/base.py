from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Generic, TypeVar


class InferenceContext(ABC):
    """Encapsulates information that can be used by an inference signaling rule or inference policy."""

    pass


TContext = TypeVar("TContext", bound=InferenceContext)


class InferenceContextProvider(Generic[TContext], ABC):
    """Provides a populated inference context from some datasource."""

    @abstractmethod
    def get_context(self) -> TContext:
        """Fetch inference context data and return it."""
        pass
