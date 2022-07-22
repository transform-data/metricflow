from __future__ import annotations
from collections import defaultdict

import sys
from typing import Dict, List, TextIO
from metricflow.dataflow.sql_table import SqlTable

from metricflow.inference.models import InferenceResult
from metricflow.inference.renderer.base import InferenceRenderer


class StreamInferenceRenderer(InferenceRenderer):
    """Writes inference results to a TextIO as human-readable text."""

    def __init__(self, stream: TextIO, close_after_render: bool) -> None:
        """Initializes the renderer.

        stream: the `TextIO` to write outputs to.
        """
        self.stream = stream
        self.close_after_render = close_after_render

    @staticmethod
    def stdout() -> StreamInferenceRenderer:
        """Factory method to create a `StreamInferenceRenderer` that writes to stdout."""
        return StreamInferenceRenderer(stream=sys.stdout, close_after_render=False)

    @staticmethod
    def file(path: str) -> StreamInferenceRenderer:
        """Factory method to create a `StreamInferenceRenderer` that writes to a file."""
        file_stream = open(path, "w")
        return StreamInferenceRenderer(stream=file_stream, close_after_render=True)

    def render(self, results: List[InferenceResult]) -> None:
        """Write the results to the configured TextIO as human-readable text."""
        list_delim = "\n    - "

        results_by_table: Dict[SqlTable, List[InferenceResult]] = defaultdict(list)
        for result in results:
            results_by_table[result.column.table].append(result)

        for table, results in results_by_table.items():
            lines: List[str] = [table.sql + "\n"]
            for result in results:
                reasons_str = " -- "
                if len(result.reasons) > 0:
                    reasons_str = list_delim + list_delim.join(result.reasons)

                problems_str = " -- "
                if len(result.problems) > 0:
                    problems_str = list_delim + list_delim.join(result.problems)

                lines += [
                    f"  {result.column.column_name}\n",
                    f"    type: {result.type_node.name}\n",
                    "    reasons: ",
                    reasons_str,
                    "\n    problems: ",
                    problems_str,
                    "\n\n",
                ]
            self.stream.writelines(lines)

        if self.close_after_render:
            self.stream.close()
