from __future__ import annotations
from collections import defaultdict

from typing import List

from metricflow.inference.context.base import InferenceContextProvider
from metricflow.inference.context.data_warehouse import DataWarehouseInferenceContextProvider
from metricflow.inference.rule.base import InferenceRule, InferenceSignal
from metricflow.inference.solver.base import InferenceSolver
from metricflow.inference.renderer.base import InferenceRenderer

# TODO: we still need to add input/output context validations and optimizations.
# Case 1: Rule 1 requires Context of type A, but no provider privides it. Should fail before running
# Case 2: ContextProvider 1 provides Context of type A, but no rule uses it. Should proceed without actually fetching that context.
# Case 3: ContextProviders 1 and 2 both provide Contexts of type A, which is ambiguous. Should fail before running


class InferenceRunner:
    """Glues together all other inference classes in a sequence that actually runs inference."""

    def __init__(
        self,
        context_providers: List[InferenceContextProvider],
        ruleset: List[InferenceRule],
        solver: InferenceSolver,
        renderers: List[InferenceRenderer],
    ) -> None:
        """Initialize the inference runner.

        context_providers: a list of context providers to be used
        ruleset: the set of rules that will produce signals
        solver: the inference solver to be used
        renderers: the renderers that will write inference results as meaningful output
        """
        if len(context_providers) != 1 or not isinstance(context_providers[0], DataWarehouseInferenceContextProvider):
            raise ValueError("Currently, InferenceRunner requires exactly one DataWarehouseInferenceContextProvider.")

        if len(ruleset) == 0:
            raise ValueError("Running inference with an empty ruleset would produce no result.")

        if len(renderers) == 0:
            raise ValueError("Running inference with no renderer would discard the results.")

        self.context_providers = context_providers
        self.ruleset = ruleset
        self.solver = solver
        self.renderers = renderers

    def run(self) -> None:
        """Runs inference with the given configs."""
        warehouse = self.context_providers[0].get_context()
        signals: List[InferenceSignal] = []
        for rule in self.ruleset:
            rule_signals = rule.process(warehouse)
            for rule_signal in rule_signals:
                signals.append(rule_signal)

        signals_by_column = defaultdict(list)
        for signal in signals:
            signals_by_column[signal.column].append(signal)

        results = [self.solver.solve_column(column, signals) for column, signals in signals_by_column.items()]

        for renderer in self.renderers:
            renderer.render(results)
