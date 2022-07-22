"""Prefixes to use when generating IDs. Listed here to avoid conflicts."""

# Nodes in a DAG have IDs of the form "prefix_number". These are the prefixes used for the different node types.
import logging
import threading

from typing import Dict, Type

DATAFLOW_NODE_AGGREGATE_MEASURES_ID_PREFIX = "am"
DATAFLOW_NODE_COMPUTE_METRICS_ID_PREFIX = "cm"
DATAFLOW_NODE_JOIN_AGGREGATED_MEASURES_BY_GROUPBY_COLUMNS_PREFIX = "jamgc"
DATAFLOW_NODE_JOIN_TO_STANDARD_OUTPUT_ID_PREFIX = "jso"
DATAFLOW_NODE_JOIN_SELF_OVER_TIME_RANGE_ID_PREFIX = "jotr"
DATAFLOW_NODE_ORDER_BY_LIMIT_ID_PREFIX = "obl"
DATAFLOW_NODE_PASS_FILTER_ELEMENTS_ID_PREFIX = "pfe"
DATAFLOW_NODE_READ_SQL_SOURCE_ID_PREFIX = "rss"
DATAFLOW_NODE_WHERE_CONSTRAINT_ID_PREFIX = "wcc"
DATAFLOW_NODE_WRITE_TO_RESULT_DATAFRAME_ID_PREFIX = "wrd"
DATAFLOW_NODE_WRITE_TO_RESULT_TABLE_ID_PREFIX = "wrt"
DATAFLOW_NODE_COMBINE_METRICS_ID_PREFIX = "cbm"
DATAFLOW_NODE_CONSTRAIN_TIME_RANGE_ID_PREFIX = "ctr"
DATAFLOW_NODE_SET_MEASURE_AGGREGATION_TIME = "sma"

SQL_EXPR_COLUMN_REFERENCE_ID_PREFIX = "cr"
SQL_EXPR_COMPARISON_ID_PREFIX = "cmp"
SQL_EXPR_FUNCTION_ID_PREFIX = "fnc"
SQL_EXPR_STRING_ID_PREFIX = "str"
SQL_EXPR_NULL_PREFIX = "null"
SQL_EXPR_LOGICAL_OPERATOR_PREFIX = "lo"
SQL_EXPR_STRING_LITERAL_PREFIX = "sl"
SQL_EXPR_IS_NULL_PREFIX = "isn"
SQL_EXPR_CAST_TO_TIMESTAMP_PREFIX = "ctt"
SQL_EXPR_DATE_TRUNC = "dt"
SQL_EXPR_RATIO_COMPUTATION = "rc"
SQL_EXPR_BETWEEN_PREFIX = "betw"

SQL_PLAN_SELECT_STATEMENT_ID_PREFIX = "ss"
SQL_PLAN_TABLE_FROM_CLAUSE_ID_PREFIX = "tfc"

EXEC_NODE_READ_SQL_QUERY = "rsq"
EXEC_NODE_NOOP = "noop"
EXEC_NODE_WRITE_TO_TABLE = "wtt"

DATAFLOW_PLAN_PREFIX = "dfp"
SQL_QUERY_PLAN_PREFIX = "sqp"
EXEC_PLAN_PREFIX = "ep"


logger = logging.getLogger(__name__)


class IdGenerator:
    """Helps generate unique ID strings."""

    def __init__(self, start_value: int = 0) -> None:  # noqa: D
        """Identifiers of the form (prefix + number) e.g. my_node_1"""
        self._next_id = start_value
        self._lock = threading.Lock()

    def create_id(self, prefix: str) -> str:
        """Create a unique ID string that can be used for naming nodes (or others) using the form <prefix>_<number>"""
        with self._lock:
            new_id = f"{prefix}_{self._next_id}"
            self._next_id += 1
            return new_id


class IdGeneratorRegistry:
    """Enumerate all IdGenerators used so that they can be patched appropriately in testing.

    See: patch_id_generators()
    """

    DEFAULT_START_VALUE = 0
    _state_lock = threading.Lock()
    _class_name_to_id_generator: Dict[str, IdGenerator] = {}

    @classmethod
    def for_class(cls, class_type: Type) -> IdGenerator:
        """Return an ID generator for the given class"""

        with cls._state_lock:
            class_name = ".".join([class_type.__module__, class_type.__name__])
            if class_name not in cls._class_name_to_id_generator:
                cls._class_name_to_id_generator[class_name] = IdGenerator(
                    start_value=IdGeneratorRegistry.DEFAULT_START_VALUE
                )
            return cls._class_name_to_id_generator[class_name]
