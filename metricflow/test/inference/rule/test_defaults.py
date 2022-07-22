from metricflow.dataflow.sql_column import SqlColumn
from metricflow.dataflow.sql_table import SqlTable
from metricflow.inference.context.data_warehouse import (
    ColumnProperties,
    DataWarehouseInferenceContext,
    InferenceColumnType,
    TableProperties,
)
from metricflow.inference.models import InferenceSignalType
import metricflow.inference.rule.defaults as defaults


def get_column_properties(column_str: str, type: InferenceColumnType, unique: bool) -> ColumnProperties:  # noqa: D
    return ColumnProperties(
        column=SqlColumn.from_string(column_str),
        type=type,
        row_count=10000,
        distinct_row_count=10000 if unique else 9000,
        is_nullable=False,
        null_count=0,
        min_value=0,
        max_value=9999,
    )


def test_any_identifier_by_name_matcher():  # noqa: D
    assert defaults.AnyIdentifierByNameRule().match_column(
        get_column_properties("db.schema.table.id", InferenceColumnType.INTEGER, True)
    )
    assert defaults.AnyIdentifierByNameRule().match_column(
        get_column_properties("db.schema.table.tableid", InferenceColumnType.INTEGER, True)
    )
    assert defaults.AnyIdentifierByNameRule().match_column(
        get_column_properties("db.schema.table.table_id", InferenceColumnType.INTEGER, True)
    )
    assert defaults.AnyIdentifierByNameRule().match_column(
        get_column_properties("db.schema.table.othertable_id", InferenceColumnType.INTEGER, True)
    )
    assert not defaults.AnyIdentifierByNameRule().match_column(
        get_column_properties("db.schema.table.whatever", InferenceColumnType.INTEGER, True)
    )


def test_primary_identifier_by_name_matcher():  # noqa: D
    assert defaults.PrimaryIdentifierByNameRule().match_column(
        get_column_properties("db.schema.table.id", InferenceColumnType.INTEGER, True)
    )
    assert defaults.PrimaryIdentifierByNameRule().match_column(
        get_column_properties("db.schema.table.tableid", InferenceColumnType.INTEGER, True)
    )
    assert defaults.PrimaryIdentifierByNameRule().match_column(
        get_column_properties("db.schema.table.table_id", InferenceColumnType.INTEGER, True)
    )
    assert defaults.PrimaryIdentifierByNameRule().match_column(
        get_column_properties("db.schema.tables.table_id", InferenceColumnType.INTEGER, True)
    )
    assert defaults.PrimaryIdentifierByNameRule().match_column(
        get_column_properties("db.schema.tables.tableid", InferenceColumnType.INTEGER, True)
    )
    assert not defaults.PrimaryIdentifierByNameRule().match_column(
        get_column_properties("db.schema.table.othertable_id", InferenceColumnType.INTEGER, True)
    )
    assert not defaults.PrimaryIdentifierByNameRule().match_column(
        get_column_properties("db.schema.table.othertableid", InferenceColumnType.INTEGER, True)
    )
    assert not defaults.PrimaryIdentifierByNameRule().match_column(
        get_column_properties("db.schema.table.whatever", InferenceColumnType.INTEGER, True)
    )


def test_unique_identifier_by_distinct_count_matcher():  # noqa: D
    assert defaults.UniqueIdentifierByDistinctCountRule().match_column(
        get_column_properties("db.schema.table.unique_id", InferenceColumnType.INTEGER, True)
    )
    assert not defaults.UniqueIdentifierByDistinctCountRule().match_column(
        get_column_properties("db.schema.table.unique_id", InferenceColumnType.STRING, False)
    )


def test_time_dimension_by_time_type_matcher():  # noqa: D
    assert defaults.TimeDimensionByTimeTypeRule().match_column(
        get_column_properties("db.schema.table.time", InferenceColumnType.DATETIME, True)
    )

    assert not defaults.TimeDimensionByTimeTypeRule().match_column(
        get_column_properties("db.schema.table.time", InferenceColumnType.INTEGER, True)
    )
    assert not defaults.TimeDimensionByTimeTypeRule().match_column(
        get_column_properties("db.schema.table.time", InferenceColumnType.FLOAT, True)
    )
    assert not defaults.TimeDimensionByTimeTypeRule().match_column(
        get_column_properties("db.schema.table.time", InferenceColumnType.BOOLEAN, True)
    )
    assert not defaults.TimeDimensionByTimeTypeRule().match_column(
        get_column_properties("db.schema.table.time", InferenceColumnType.STRING, True)
    )
    assert not defaults.TimeDimensionByTimeTypeRule().match_column(
        get_column_properties("db.schema.table.time", InferenceColumnType.UNKNOWN, True)
    )


def test_primary_time_dimension_by_name_matcher():  # noqa: D
    assert defaults.PrimaryTimeDimensionByNameRule().match_column(
        get_column_properties("db.schema.table.ds", InferenceColumnType.DATETIME, True)
    )
    assert defaults.PrimaryTimeDimensionByNameRule().match_column(
        get_column_properties("db.schema.table.created_at", InferenceColumnType.DATETIME, True)
    )
    assert not defaults.PrimaryTimeDimensionByNameRule().match_column(
        get_column_properties("db.schema.table.bla", InferenceColumnType.DATETIME, True)
    )
    assert not defaults.PrimaryTimeDimensionByNameRule().match_column(
        get_column_properties("db.schema.table.time", InferenceColumnType.DATETIME, True)
    )


def test_primary_time_dimension_if_only_time_rule():  # noqa: D
    table = SqlTable.from_string("db.schema.table")
    single_time_col_warehouse = DataWarehouseInferenceContext(
        table_props=[
            TableProperties(
                table=table,
                column_props=[
                    get_column_properties("db.schema.table.id", InferenceColumnType.INTEGER, True),
                    get_column_properties("db.schema.table.time", InferenceColumnType.DATETIME, True),
                ],
            )
        ]
    )
    single_time_col_signals = defaults.PrimaryTimeDimensionIfOnlyTimeRule().process(single_time_col_warehouse)
    assert len(single_time_col_signals) == 1
    assert single_time_col_signals[0].column == SqlColumn.from_string("db.schema.table.time")
    assert single_time_col_signals[0].type_node == InferenceSignalType.DIMENSION.PRIMARY_TIME

    many_time_col_warehouse = DataWarehouseInferenceContext(
        table_props=[
            TableProperties(
                table=table,
                column_props=[
                    get_column_properties("db.schema.table.id", InferenceColumnType.INTEGER, True),
                    get_column_properties("db.schema.table.time", InferenceColumnType.DATETIME, True),
                    get_column_properties("db.schema.table.othertime", InferenceColumnType.DATETIME, True),
                ],
            )
        ]
    )
    many_time_col_signals = defaults.PrimaryTimeDimensionIfOnlyTimeRule().process(many_time_col_warehouse)
    assert len(many_time_col_signals) == 0


def test_categorical_dimension_by_boolean_type_matcher():  # noqa: D
    assert defaults.CategoricalDimensionByBooleanTypeRule().match_column(
        get_column_properties("db.schema.table.dim", InferenceColumnType.BOOLEAN, True)
    )
    assert not defaults.CategoricalDimensionByBooleanTypeRule().match_column(
        get_column_properties("db.schema.table.bla", InferenceColumnType.FLOAT, True)
    )


def test_categorical_dimension_by_string_type_matcher():  # noqa: D
    assert defaults.CategoricalDimensionByStringTypeRule().match_column(
        get_column_properties("db.schema.table.dim", InferenceColumnType.STRING, True)
    )
    assert not defaults.CategoricalDimensionByStringTypeRule().match_column(
        get_column_properties("db.schema.table.bla", InferenceColumnType.FLOAT, True)
    )


def test_categorical_dimension_by_integer_type_matcher():  # noqa: D
    assert defaults.CategoricalDimensionByIntegerTypeRule().match_column(
        get_column_properties("db.schema.table.dim", InferenceColumnType.INTEGER, True)
    )
    assert not defaults.CategoricalDimensionByIntegerTypeRule().match_column(
        get_column_properties("db.schema.table.bla", InferenceColumnType.FLOAT, True)
    )


def test_measure_by_real_type_matcher():  # noqa: D
    assert defaults.MeasureByRealTypeRule().match_column(
        get_column_properties("db.schema.table.measure", InferenceColumnType.FLOAT, True)
    )
    assert not defaults.MeasureByRealTypeRule().match_column(
        get_column_properties("db.schema.table.measure", InferenceColumnType.INTEGER, True)
    )
    assert not defaults.MeasureByRealTypeRule().match_column(
        get_column_properties("db.schema.table.measure", InferenceColumnType.DATETIME, True)
    )
    assert not defaults.MeasureByRealTypeRule().match_column(
        get_column_properties("db.schema.table.measure", InferenceColumnType.BOOLEAN, True)
    )
    assert not defaults.MeasureByRealTypeRule().match_column(
        get_column_properties("db.schema.table.measure", InferenceColumnType.STRING, True)
    )
    assert not defaults.MeasureByRealTypeRule().match_column(
        get_column_properties("db.schema.table.measure", InferenceColumnType.UNKNOWN, True)
    )


def test_measure_by_integer_type_matcher():  # noqa: D
    assert defaults.MeasureByIntegerTypeRule().match_column(
        get_column_properties("db.schema.table.measure", InferenceColumnType.INTEGER, True)
    )
    assert not defaults.MeasureByRealTypeRule().match_column(
        get_column_properties("db.schema.table.measure", InferenceColumnType.BOOLEAN, True)
    )
