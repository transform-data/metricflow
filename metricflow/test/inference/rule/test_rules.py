from metricflow.dataflow.sql_column import SqlColumn
from metricflow.dataflow.sql_table import SqlTable
from metricflow.inference.context.data_warehouse import (
    DataWarehouseInferenceContext,
    ColumnProperties,
    InferenceColumnType,
    TableProperties,
)
from metricflow.inference.models import InferenceSignalConfidence, InferenceSignalType
from metricflow.inference.rule.rules import ColumnMatcherRule, LowCardinalityRatioRule


def create_context_with_counts(rows: int, distinct: int, nulls: int) -> DataWarehouseInferenceContext:
    """Get a `DataWarehouseInferenceContext` with the designated counts"""
    return DataWarehouseInferenceContext(
        table_props=[
            TableProperties(
                table=SqlTable.from_string("db.schema.table"),
                column_props=[
                    ColumnProperties(
                        column=SqlColumn.from_string("db.schema.table.column"),
                        type=InferenceColumnType.INTEGER,
                        row_count=rows,
                        distinct_row_count=distinct,
                        null_count=nulls,
                        is_nullable=nulls != 0,
                        min_value=0,
                        max_value=rows - 1,
                    )
                ],
            )
        ]
    )


class TestLowCardinalityRule(LowCardinalityRatioRule):  # noqa: D
    type_node = InferenceSignalType.DIMENSION.CATEGORICAL
    confidence = InferenceSignalConfidence.MEDIUM
    complimentary_signal = False


def test_column_matcher(warehouse_ctx: DataWarehouseInferenceContext):  # noqa: D
    class TestRule(ColumnMatcherRule):
        type_node = InferenceSignalType.DIMENSION.UNKNOWN
        confidence = InferenceSignalConfidence.MEDIUM
        match_reason = "test reason"
        complimentary_signal = False

        def match_column(self, props: ColumnProperties) -> bool:
            return props.column.column_name.endswith("test_column")

    signals = TestRule().process(warehouse_ctx)

    assert len(signals) == 1
    assert signals[0].confidence == InferenceSignalConfidence.MEDIUM
    assert signals[0].type_node == InferenceSignalType.DIMENSION.UNKNOWN
    assert signals[0].column == SqlColumn.from_string("db.schema.table.test_column")
    assert signals[0].reason == "test reason"
    assert not signals[0].is_complimentary


def test_low_cardinality_ratio_rule_high_cardinality_doesnt_match():  # noqa: D
    rule = TestLowCardinalityRule(0.1)
    ctx = create_context_with_counts(100, 100, 0)

    signals = rule.process(ctx)
    assert len(signals) == 0


def test_low_cardinality_ratio_rule_low_cardinality_lots_of_nulls_doesnt_match():  # noqa: D
    rule = TestLowCardinalityRule(0.1)
    ctx = create_context_with_counts(100, 2, 99)

    signals = rule.process(ctx)
    assert len(signals) == 0


def test_low_cardinality_ratio_rule_low_cardinality_all_nulls_doesnt_match():  # noqa: D
    rule = TestLowCardinalityRule(0.1)
    ctx = create_context_with_counts(100, 1, 100)

    signals = rule.process(ctx)
    assert len(signals) == 0


def test_low_cardinality_ratio_rule_low_cardinality_matches():  # noqa: D
    rule = TestLowCardinalityRule(0.1)
    ctx = create_context_with_counts(100, 1, 0)

    signals = rule.process(ctx)
    assert len(signals) == 1
