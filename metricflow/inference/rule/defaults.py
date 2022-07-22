from typing import List

from metricflow.inference.context.data_warehouse import (
    ColumnProperties,
    InferenceColumnType,
    DataWarehouseInferenceContext,
)
from metricflow.inference.models import InferenceSignalConfidence, InferenceSignalType, InferenceSignal
from metricflow.inference.rule.base import InferenceRule
from metricflow.inference.rule.rules import ColumnMatcherRule, LowCardinalityRatioRule


# -------------
# IDENTIFIERS
# -------------


class AnyIdentifierByNameRule(ColumnMatcherRule):
    """Inference rule that checks for columns ending with `id`.

    We searched for words ending with "id" just to assess the chance of this resulting in a
    false positive. Our guess is most of those words would rarely, if ever, be used as column names.
    Therefore, not adding a mandatory "_" before "id" would benefit the product by matching names
    like "userid", despite the rare "squid", "mermaid" or "android" matches.

    See: https://www.thefreedictionary.com/words-that-end-in-id

    It will always produce ID.UNKNOWN signal with FOR_SURE confidence.
    """

    type_node = InferenceSignalType.ID.UNKNOWN
    confidence = InferenceSignalConfidence.FOR_SURE
    complimentary_signal = False
    match_reason = "Column name ends with `id`"

    def match_column(self, props: ColumnProperties) -> bool:  # noqa: D
        return props.column.column_name.lower().endswith("id")


class PrimaryIdentifierByNameRule(ColumnMatcherRule):
    """Inference rule that matches primary identifiers by their names.

    It will match columns such as `db.schema.mytable.mytable_id`,
    `db.schema.mytable.mytableid` and `db.schema.mytable.id`.

    It will always produce a ID.PRIMARY signal with FOR_SURE confidence.
    """

    type_node = InferenceSignalType.ID.PRIMARY
    confidence = InferenceSignalConfidence.FOR_SURE
    complimentary_signal = False
    match_reason = "Column name matches `(table_name?)(_?)id`"

    def match_column(self, props: ColumnProperties) -> bool:  # noqa: D
        col_lower = props.column.column_name.lower()
        table_lower = props.column.table_name.lower().rstrip("s")

        if col_lower == "id":
            return True

        return col_lower == f"{table_lower}_id" or col_lower == f"{table_lower}id"


class UniqueIdentifierByDistinctCountRule(ColumnMatcherRule):
    """Inference rule that matches unique identifiers by their COUNT DISTINCT.

    It will always produce a ID.UNIQUE complimentary signal with FOR_SURE confidence.
    """

    type_node = InferenceSignalType.ID.UNIQUE
    confidence = InferenceSignalConfidence.FOR_SURE
    complimentary_signal = True
    match_reason = "The values in the column are unique"

    def match_column(self, props: ColumnProperties) -> bool:  # noqa: D
        return props.distinct_row_count == props.row_count


class ForeignIdentifierByCardinalityRatioRule(LowCardinalityRatioRule):
    """Inference rule that checks for low cardinality columns.

    It will always produce ID.FOREIGN with MEDIUM confidence (complimentary).
    """

    type_node = InferenceSignalType.ID.FOREIGN
    confidence = InferenceSignalConfidence.MEDIUM
    complimentary_signal = True


# -------------
# DIMENSIONS
# -------------


class TimeDimensionByTimeTypeRule(ColumnMatcherRule):
    """Inference rule that checks for time (time, date, datetime, timestamp) columns.

    It will always produce DIMENSION.TIME with FOR_SURE confidence.
    """

    type_node = InferenceSignalType.DIMENSION.TIME
    confidence = InferenceSignalConfidence.FOR_SURE
    complimentary_signal = False
    match_reason = "Column type is time (TIME, DATE, DATETIME, TIMESTAMP)"

    def match_column(self, props: ColumnProperties) -> bool:  # noqa: D
        return props.type == InferenceColumnType.DATETIME


class PrimaryTimeDimensionByNameRule(ColumnMatcherRule):
    """Inference rule that checks if the column name is one of `ds` or `created_at`

    It will always produce DIMENSION.PRIMARY_TIME with FOR_SURE confidence.
    """

    type_node = InferenceSignalType.DIMENSION.PRIMARY_TIME
    confidence = InferenceSignalConfidence.FOR_SURE
    complimentary_signal = False
    match_reason = "Column name is either of 'ds', 'created_at', 'created_date' or 'created_time'"

    def match_column(self, props: ColumnProperties) -> bool:  # noqa: D
        return props.column.column_name in ["ds", "created_at", "created_date", "created_time"]


class PrimaryTimeDimensionIfOnlyTimeRule(InferenceRule):
    """Inference rule for checking if the column is the only time column in the table.

    It will always produce DIMENSION.PRIMARY_TIME signal with FOR_SURE confidence.
    """

    def process(self, warehouse: DataWarehouseInferenceContext) -> List[InferenceSignal]:  # noqa: D
        signals: List[InferenceSignal] = []
        for table_props in warehouse.tables.values():
            time_cols = [
                col for col, col_props in table_props.columns.items() if col_props.type == InferenceColumnType.DATETIME
            ]
            if len(time_cols) == 1:
                signals.append(
                    InferenceSignal(
                        column=time_cols[0],
                        type_node=InferenceSignalType.DIMENSION.PRIMARY_TIME,
                        is_complimentary=False,
                        reason="The column is the only time column in its table",
                        confidence=InferenceSignalConfidence.FOR_SURE,
                    )
                )
        return signals


class CategoricalDimensionByBooleanTypeRule(ColumnMatcherRule):
    """Inference rule that checks for boolean columns.

    It will always produce DIMENSION.CATEGORICAL with FOR_SURE confidence.
    """

    type_node = InferenceSignalType.DIMENSION.CATEGORICAL
    confidence = InferenceSignalConfidence.FOR_SURE
    complimentary_signal = False
    match_reason = "Column type is BOOLEAN"

    def match_column(self, props: ColumnProperties) -> bool:  # noqa: D
        return props.type == InferenceColumnType.BOOLEAN


class CategoricalDimensionByStringTypeRule(ColumnMatcherRule):
    """Inference rule that checks for string columns.

    It will always produce DIMENSION.CATEGORICAL with MEDIUM confidence (complimentary).
    """

    type_node = InferenceSignalType.DIMENSION.CATEGORICAL
    confidence = InferenceSignalConfidence.MEDIUM
    complimentary_signal = True
    match_reason = "Column type is STRING"

    def match_column(self, props: ColumnProperties) -> bool:  # noqa: D
        return props.type == InferenceColumnType.STRING


class CategoricalDimensionByIntegerTypeRule(ColumnMatcherRule):
    """Inference rule that checks for integer columns.

    It will always produce DIMENSION.CATEGORICAL with MEDIUM confidence (complimentary).
    """

    type_node = InferenceSignalType.DIMENSION.CATEGORICAL
    confidence = InferenceSignalConfidence.MEDIUM
    complimentary_signal = True
    match_reason = "Column type is INTEGER"

    def match_column(self, props: ColumnProperties) -> bool:  # noqa: D
        return props.type == InferenceColumnType.INTEGER


class CategoricalDimensionByCardinalityRatioRule(LowCardinalityRatioRule):
    """Inference rule that checks for low cardinality columns.

    It will always produce DIMENSION.CATEGORICAL with MEDIUM confidence (complimentary).
    """

    type_node = InferenceSignalType.DIMENSION.CATEGORICAL
    confidence = InferenceSignalConfidence.MEDIUM
    complimentary_signal = True


# -------------
# MEASURES
# -------------


class MeasureByRealTypeRule(ColumnMatcherRule):
    """Inference rule that checks for real (float, double) columns.

    It will always produce MEASURE with FOR_SURE confidence.
    """

    type_node = InferenceSignalType.MEASURE.UNKNOWN
    confidence = InferenceSignalConfidence.FOR_SURE
    complimentary_signal = False
    match_reason = "Column type is real (FLOAT, DOUBLE, DOUBLE PRECISION)"

    def match_column(self, props: ColumnProperties) -> bool:  # noqa: D
        return props.type == InferenceColumnType.FLOAT


class MeasureByIntegerTypeRule(ColumnMatcherRule):
    """Inference rule that checks for integer  columns.

    It will always produce MEASURE with MEDIUM confidence (complimentary).
    """

    type_node = InferenceSignalType.MEASURE.UNKNOWN
    confidence = InferenceSignalConfidence.MEDIUM
    complimentary_signal = True
    match_reason = "Column type is INTEGER"

    def match_column(self, props: ColumnProperties) -> bool:  # noqa: D
        return props.type == InferenceColumnType.INTEGER


DEFAULT_RULESET = [
    AnyIdentifierByNameRule(),
    PrimaryIdentifierByNameRule(),
    UniqueIdentifierByDistinctCountRule(),
    ForeignIdentifierByCardinalityRatioRule(0.4),
    TimeDimensionByTimeTypeRule(),
    PrimaryTimeDimensionByNameRule(),
    PrimaryTimeDimensionIfOnlyTimeRule(),
    CategoricalDimensionByBooleanTypeRule(),
    CategoricalDimensionByStringTypeRule(),
    CategoricalDimensionByIntegerTypeRule(),
    CategoricalDimensionByCardinalityRatioRule(0.2),
    MeasureByRealTypeRule(),
    MeasureByIntegerTypeRule(),
]
