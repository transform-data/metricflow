from typing import List
from metricflow.dataflow.sql_column import SqlColumn

from metricflow.inference.rule.base import InferenceRule, InferenceSignalConfidence, InferenceSignalType
from metricflow.inference.rule.rules import ColumnMatcherRule


class RuleDefaults:
    """Static factory class for sensible default rules."""

    @staticmethod
    def _any_identifier_matcher(col: SqlColumn) -> bool:
        """This is the default matcher that is used to determine if columns are identifiers.

        It simply matches column names ending with "id", case insensitive.

        We searched for words ending with "id" just to assess the chance of this resulting in a
        false positive. Our guess is most of those words would rarely, if ever, be used as column names.
        Therefore, not adding a mandatory "_" before "id" would benefit the product by matching names
        like "userid", despite the rare "squid", "mermaid" or "android" matches.

        See: https://www.thefreedictionary.com/words-that-end-in-id
        """
        return col.column_name.lower().endswith("id")

    @staticmethod
    def _primary_identifier_matcher(col: SqlColumn) -> bool:
        """This is the default matcher that is used to determine if columns are primary identifiers.

        It matches columns named "id" or columns named "<table>id" or "<table>_id", where "<table>"
        is the name of the table this column belongs to.
        """
        col_lower = col.column_name.lower()
        table_lower = col.table_name.lower()

        if col_lower == "id":
            return True

        return col_lower == f"{table_lower}_id" or col_lower == f"{table_lower}id"

    @staticmethod
    def default_ruleset() -> List[InferenceRule]:
        """Returns a sensible default set of inference rules."""
        return [
            RuleDefaults.primary_identifier_rule(),
            RuleDefaults.any_identifier_rule(),
        ]

    @staticmethod
    def primary_identifier_rule() -> ColumnMatcherRule:
        """A default for finding primary identifiers by their names based on column name matches.

        The returned rule will match columns such as `db.schema.mytable.mytable_id`,
        `db.schema.mytable.mytableid` and `db.schema.mytable.id`.

        It will always produce a PRIMARY_IDENTIFIER signal with FOR_SURE confidence.
        """
        return ColumnMatcherRule(
            matcher=RuleDefaults._primary_identifier_matcher,
            type_node=InferenceSignalType.ID.PRIMARY,
            confidence=InferenceSignalConfidence.FOR_SURE,
        )

    @staticmethod
    def any_identifier_rule() -> ColumnMatcherRule:
        """A default for finding identifiers of any type by their names based on column name matches.

        The returned rule will match columns such as `db.schema.mytable.id`,
        `db.schema.mytable.othertable_id` and `db.schema.mytable.othertableid`

        It will always produce an IDENTIFIER signal with HIGH confidence.
        """
        return ColumnMatcherRule(
            matcher=RuleDefaults._any_identifier_matcher,
            type_node=InferenceSignalType.ID.UNKNOWN,
            confidence=InferenceSignalConfidence.HIGH,
        )
