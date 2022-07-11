from metricflow.dataflow.sql_column import SqlColumn
from metricflow.inference.rule.base import InferenceSignalConfidence, InferenceSignalType
from metricflow.inference.rule.defaults import RuleDefaults

from metricflow.inference.rule.rules import ColumnMatcherRule


def test_any_identifier_matcher():  # noqa: D
    assert RuleDefaults._any_identifier_matcher(SqlColumn.from_string("db.schema.table.id"))
    assert RuleDefaults._any_identifier_matcher(SqlColumn.from_string("db.schema.table.tableid"))
    assert RuleDefaults._any_identifier_matcher(SqlColumn.from_string("db.schema.table.table_id"))
    assert RuleDefaults._any_identifier_matcher(SqlColumn.from_string("db.schema.table.othertable_id"))
    assert not RuleDefaults._any_identifier_matcher(SqlColumn.from_string("db.schema.table.whatever"))


def test_any_identifier_rule_factory():  # noqa: D
    rule = RuleDefaults.any_identifier_rule()
    assert isinstance(rule, ColumnMatcherRule)
    assert rule.matcher == RuleDefaults._any_identifier_matcher
    assert rule.confidence == InferenceSignalConfidence.HIGH
    assert rule.type_node == InferenceSignalType.ID.UNKNOWN


def test_primary_identifier_matcher():  # noqa: D
    assert RuleDefaults._primary_identifier_matcher(SqlColumn.from_string("db.schema.table.id"))
    assert RuleDefaults._primary_identifier_matcher(SqlColumn.from_string("db.schema.table.tableid"))
    assert RuleDefaults._primary_identifier_matcher(SqlColumn.from_string("db.schema.table.table_id"))
    assert not RuleDefaults._primary_identifier_matcher(SqlColumn.from_string("db.schema.table.othertable_id"))
    assert not RuleDefaults._primary_identifier_matcher(SqlColumn.from_string("db.schema.table.othertableid"))
    assert not RuleDefaults._primary_identifier_matcher(SqlColumn.from_string("db.schema.table.whatever"))


def test_primary_identifier_rule_factory():  # noqa: D
    rule = RuleDefaults.primary_identifier_rule()
    assert isinstance(rule, ColumnMatcherRule)
    assert rule.matcher == RuleDefaults._primary_identifier_matcher
    assert rule.confidence == InferenceSignalConfidence.FOR_SURE
    assert rule.type_node == InferenceSignalType.ID.PRIMARY
