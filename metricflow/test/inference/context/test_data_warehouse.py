from metricflow.dataflow.sql_column import SqlColumn
from metricflow.dataflow.sql_table import SqlTable
from metricflow.inference.context.data_warehouse import (
    ColumnProperties,
    InferenceColumnType,
    DataWarehouseInferenceContext,
    TableProperties,
)


def test_column_properties_is_empty():
    """Just some easy assertions to test is_empty works as intended."""
    props = ColumnProperties(
        column=SqlColumn.from_string("db.schema.table.column"),
        type=InferenceColumnType.INTEGER,
        row_count=10000,
        distinct_row_count=1000,
        is_nullable=True,
        null_count=1,
        min_value=0,
        max_value=9999,
    )
    assert not props.is_empty

    empty_props = ColumnProperties(
        column=SqlColumn.from_string("db.schema.table.column"),
        type=InferenceColumnType.UNKNOWN,
        row_count=0,
        distinct_row_count=0,
        is_nullable=False,
        null_count=0,
        min_value=None,
        max_value=None,
    )
    assert empty_props.is_empty


def test_table_properties() -> None:
    """Test `TableProperties` initialization.

    This test case asserts that the conversion from the `column_props` argument (which is a list) to
    `self.columns` (which is a dict) implemented by `TableProperties.__post_init__` works as intended.
    """
    table = SqlTable.from_string("db.schema.table")
    col_props = [
        ColumnProperties(
            column=SqlColumn(table=table, column_name="column1"),
            type=InferenceColumnType.INTEGER,
            row_count=1000,
            distinct_row_count=1000,
            is_nullable=False,
            null_count=0,
            min_value=0,
            max_value=999,
        ),
        ColumnProperties(
            column=SqlColumn(table=table, column_name="column2"),
            type=InferenceColumnType.FLOAT,
            row_count=2000,
            distinct_row_count=1000,
            is_nullable=True,
            null_count=10,
            min_value=0,
            max_value=1000,
        ),
    ]

    table_props = TableProperties(table=table, column_props=col_props)

    assert table_props.columns == {
        col_props[0].column: col_props[0],
        col_props[1].column: col_props[1],
    }


def test_data_warehouse_inference_context() -> None:
    """Test `DataWarehouseInferenceContext` initialization.

    This test case asserts that the conversion from the `table_props` argument
    (which is a list) to `self.tables` and `self.columns` (which are dicts)
    implemented by `DataWarehouseInferenceContext.__post_init__` works as intended.
    """
    t1 = SqlTable.from_string("db.schema1.table1")
    t2 = SqlTable.from_string("db.schema2.table1")

    t1_cols = [
        ColumnProperties(
            column=SqlColumn(table=t1, column_name="column1"),
            type=InferenceColumnType.INTEGER,
            row_count=1000,
            distinct_row_count=1000,
            is_nullable=False,
            null_count=0,
            min_value=0,
            max_value=999,
        ),
        ColumnProperties(
            column=SqlColumn(table=t1, column_name="column2"),
            type=InferenceColumnType.FLOAT,
            row_count=2000,
            distinct_row_count=1000,
            is_nullable=True,
            null_count=10,
            min_value=0,
            max_value=1000,
        ),
    ]
    t1_props = TableProperties(table=t1, column_props=t1_cols)

    t2_cols = [
        ColumnProperties(
            column=SqlColumn(table=t2, column_name="column_a"),
            type=InferenceColumnType.FLOAT,
            row_count=1000,
            distinct_row_count=1000,
            is_nullable=False,
            null_count=0,
            min_value=0,
            max_value=999,
        ),
    ]
    t2_props = TableProperties(table=t2, column_props=t2_cols)

    ctx = DataWarehouseInferenceContext(table_props=[t1_props, t2_props])

    assert ctx.tables == {t1: t1_props, t2: t2_props}

    assert ctx.columns == {
        t1_cols[0].column: t1_cols[0],
        t1_cols[1].column: t1_cols[1],
        t2_cols[0].column: t2_cols[0],
    }
