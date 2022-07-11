import pytest

from metricflow.dataflow.sql_column import SqlColumn
from metricflow.dataflow.sql_table import SqlTable
from metricflow.inference.context.data_warehouse import (
    ColumnProperties,
    DataWarehouseInferenceContext,
    InferenceColumnType,
    TableProperties,
)


@pytest.fixture
def warehouse_ctx() -> DataWarehouseInferenceContext:
    """A dummy DataWarehouseInferenceContext to be used as a fixture"""
    return DataWarehouseInferenceContext(
        table_props=[
            TableProperties(
                table=SqlTable.from_string("db.schema.table"),
                column_props=[
                    ColumnProperties(
                        column=SqlColumn.from_string("db.schema.table.id"),
                        type=InferenceColumnType.INTEGER,
                        row_count=1,
                        distinct_row_count=1,
                        is_nullable=True,
                        null_count=0,
                        min_value=0,
                        max_value=1,
                    ),
                    ColumnProperties(
                        column=SqlColumn.from_string("db.schema.table.othertable_id"),
                        type=InferenceColumnType.INTEGER,
                        row_count=2,
                        distinct_row_count=2,
                        is_nullable=True,
                        null_count=0,
                        min_value=0,
                        max_value=1,
                    ),
                    ColumnProperties(
                        column=SqlColumn.from_string("db.schema.table.test_column"),
                        type=InferenceColumnType.INTEGER,
                        row_count=1,
                        distinct_row_count=1,
                        is_nullable=True,
                        null_count=0,
                        min_value=0,
                        max_value=1,
                    ),
                    ColumnProperties(
                        column=SqlColumn.from_string("db.schema.othertable.othertable_id"),
                        type=InferenceColumnType.INTEGER,
                        row_count=2,
                        distinct_row_count=2,
                        is_nullable=True,
                        null_count=0,
                        min_value=0,
                        max_value=1,
                    ),
                ],
            )
        ]
    )
