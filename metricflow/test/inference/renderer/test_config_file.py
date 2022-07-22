import os
from pathlib import Path

import pytest
from metricflow.dataflow.sql_column import SqlColumn

from ruamel.yaml import YAML

from metricflow.inference.models import InferenceResult, InferenceSignalType
from metricflow.inference.renderer.config_file import ConfigFileRenderer

yaml = YAML()


def test_no_overwrite_with_existing_dir(tmpdir: Path):  # noqa: D
    with pytest.raises(ValueError):
        ConfigFileRenderer(tmpdir, False)


def test_dir_path_is_file(tmpdir: Path):  # noqa: D
    file_path = os.path.join(tmpdir, "file.txt")
    with open(file_path, "w") as f:
        f.write("file contents!")

    with pytest.raises(ValueError):
        ConfigFileRenderer(file_path, False)


def test_render_configs(tmpdir: Path):  # noqa: D
    inference_results = [
        InferenceResult(
            column=SqlColumn.from_string("db.schema.test_table.id"),
            type_node=InferenceSignalType.ID.PRIMARY,
            reasons=[],
        ),
        InferenceResult(
            column=SqlColumn.from_string("db.schema.test_table.time_dim"),
            type_node=InferenceSignalType.DIMENSION.TIME,
            reasons=[],
        ),
        InferenceResult(
            column=SqlColumn.from_string("db.schema.test_table.primary_time_dim"),
            type_node=InferenceSignalType.DIMENSION.PRIMARY_TIME,
            reasons=[],
        ),
    ]

    renderer = ConfigFileRenderer(tmpdir, True)

    renderer.render(inference_results)

    table_file_path = os.path.join(tmpdir, "test_table.yaml")
    assert os.path.isfile(table_file_path)

    with open(table_file_path, "r") as f:
        file_contents = yaml.load(f)

    assert file_contents == {
        "data_source": {
            "name": "test_table",
            "sql_table": "db.schema.test_table",
            "identifiers": [{"type": "primary", "name": "id"}],
            "dimensions": [
                {"type": "time", "name": "time_dim", "type_params": {"time_granularity": "day"}},
                {
                    "type": "time",
                    "name": "primary_time_dim",
                    "type_params": {"is_primary": True, "time_granularity": "day"},
                },
            ],
            "measures": [],
        }
    }
