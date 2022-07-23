from metricflow.dataflow.sql_table import SqlTable
from metricflow.dataflow.sql_column import SqlColumn


def test_sql_column() -> None:  # noqa: D
    sql_column = SqlColumn(
        table=SqlTable(db_name="test_db", schema_name="test_schema", table_name="test_table"), column_name="test_column"
    )
    column_str = "test_db.test_schema.test_table.test_column"

    assert sql_column.sql == column_str
    assert SqlColumn.from_string(column_str) == sql_column
    assert sql_column == SqlColumn.from_names(
        db_name="test_db", schema_name="test_schema", table_name="test_table", column_name="test_column"
    )

    assert sql_column.db_name == sql_column.table.db_name
    assert sql_column.schema_name == sql_column.table.schema_name
    assert sql_column.table_name == sql_column.table.table_name

    json_serialized_column = sql_column.json()
    deserialized_column = SqlColumn.parse_raw(json_serialized_column)

    assert sql_column == deserialized_column
