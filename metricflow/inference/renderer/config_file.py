from collections import defaultdict
import os
from pathlib import Path
from typing import Dict, Literal, Union, List, TypedDict
from typing_extensions import NotRequired

from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap

from metricflow.dataflow.sql_table import SqlTable
from metricflow.inference.models import InferenceResult, InferenceSignalType
from metricflow.inference.renderer.base import InferenceRenderer

yaml = YAML()


class RenderedTimeColumnConfigTypeParams(TypedDict):  # noqa: D
    time_granularity: Literal["day"]
    is_primary: NotRequired[bool]


class RenderedColumnConfig(TypedDict):  # noqa: D
    name: str
    type: str
    type_params: NotRequired[RenderedTimeColumnConfigTypeParams]


class ConfigFileRenderer(InferenceRenderer):
    """Writes inference results to a set config files"""

    UNKNOWN_FIELD_VALUE = "FIXME"

    def __init__(self, dir_path: Union[str, Path], overwrite=False) -> None:
        """Initializes the renderer.

        dir_path: The path to the config directory
        overwrite: If set to False, will raise error if the directory exists
        """
        dir_path = os.path.abspath(dir_path)

        if not overwrite and os.path.exists(dir_path):
            raise ValueError("ConfigFileRender.overwrite is False but path exists.")

        if os.path.isfile(dir_path):
            raise ValueError("ConfigFileRenderer `dir_path` is a file.")

        self.dir_path = dir_path

    def _get_filename_for_table(self, table: SqlTable) -> str:
        return os.path.abspath(os.path.join(self.dir_path, f"{table.table_name}.yaml"))

    def _fixme(self, comment: str) -> str:
        return f"FIXME: {comment}"

    def _render_id_columns(self, results: List[InferenceResult]) -> List[CommentedMap[RenderedColumnConfig]]:
        type_map = {
            InferenceSignalType.ID.PRIMARY: "primary",
            InferenceSignalType.ID.FOREIGN: "foreign",
            InferenceSignalType.ID.UNIQUE: "unique",
        }

        rendered: List[RenderedColumnConfig] = [
            {
                "name": result.column.column_name,
                "type": type_map.get(result.type_node, ConfigFileRenderer.UNKNOWN_FIELD_VALUE),
            }
            for result in results
            if result.type_node.is_descendant(InferenceSignalType.ID.UNKNOWN)
        ]

        return rendered

    def _render_dimension_columns(self, results: List[InferenceResult]) -> List[CommentedMap[RenderedColumnConfig]]:
        type_map = {
            InferenceSignalType.DIMENSION.TIME: "time",
            InferenceSignalType.DIMENSION.PRIMARY_TIME: "time",
            InferenceSignalType.DIMENSION.CATEGORICAL: "categorical",
        }

        rendered: List[CommentedMap[RenderedColumnConfig]] = []
        for result in results:
            if not result.type_node.is_descendant(InferenceSignalType.DIMENSION.UNKNOWN):
                continue

            result_data: CommentedMap[RenderedColumnConfig] = CommentedMap(
                {
                    "name": result.column.column_name,
                    "type": type_map.get(result.type_node, ConfigFileRenderer.UNKNOWN_FIELD_VALUE),
                }
            )

            if result_data["type"] == ConfigFileRenderer.UNKNOWN_FIELD_VALUE:
                result_data.yaml_add_eol_comment(self._fixme("unknown field value"), "type")

            if result.type_node.is_descendant(InferenceSignalType.DIMENSION.TIME):
                type_params: CommentedMap[RenderedTimeColumnConfigTypeParams] = {"time_granularity": "day"}
                if result.type_node.is_descendant(InferenceSignalType.DIMENSION.PRIMARY_TIME):
                    type_params["is_primary"] = True
                result_data["type_params"] = type_params

            if len(result.problems) > 0:
                result_data.yaml_set_comment_before_after_key(
                    key="name",
                    before=f"{ConfigFileRenderer.UNKNOWN_FIELD_VALUE}: " + ", ".join(result.problems),
                )

            rendered.append(result_data)

        return rendered

    def _get_comments_for_unknown_columns(self, results: List[InferenceResult]) -> List[str]:
        delim = "\n  - "
        return [
            self._fixme(result.column.column_name + delim + delim.join(result.problems))
            for result in results
            if result.type_node == InferenceSignalType.UNKNOWN
        ]

    def _get_configuration_data_for_table(self, table: SqlTable, results: List[InferenceResult]) -> Dict:
        data = CommentedMap(
            {
                "data_source": {
                    "name": table.table_name,
                    "sql_table": table.sql,
                    "identifiers": self._render_id_columns(results),
                    "dimensions": self._render_dimension_columns(results),
                    "measures": [],
                }
            }
        )

        header_comments = [self._fixme("Unreviewed inferred config file")] + self._get_comments_for_unknown_columns(
            results
        )
        data.yaml_set_comment_before_after_key(
            key="data_source",
            before="\n".join(header_comments),
        )

        return data

    def render(self, results: List[InferenceResult]) -> None:
        """Render the inference results to files in the configured directory."""
        os.makedirs(self.dir_path, exist_ok=True)

        results_by_table: Dict[SqlTable, List[InferenceResult]] = defaultdict(list)
        for result in results:
            results_by_table[result.column.table].append(result)

        for table, results in results_by_table.items():
            table_data = self._get_configuration_data_for_table(table, results)
            table_path = self._get_filename_for_table(table)
            with open(table_path, "w") as table_file:
                yaml.dump(table_data, table_file)
