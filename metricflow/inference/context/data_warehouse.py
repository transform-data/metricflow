from abc import ABC, abstractmethod
from dataclasses import InitVar, dataclass, field
from datetime import date, datetime
from enum import Enum
from typing import Dict, List, Optional, TypeVar, Generic

from metricflow.dataflow.sql_column import SqlColumn
from metricflow.dataflow.sql_table import SqlTable
from metricflow.protocols.sql_client import SqlClient
from metricflow.inference.context.base import InferenceContext, InferenceContextProvider

T = TypeVar("T", str, int, float, date, datetime)


class InferenceColumnType(str, Enum):
    """Represents a column type that can be used for inference.

    This does not provide a 1 to 1 mapping between SQL types and enum values. For example,
    all possible floating point types (FLOAT, DOUBLE etc) are mapped to the same FLOAT
    value. Same for datetimes and others.
    """

    STRING = "string"
    BOOLEAN = "boolean"
    INTEGER = "integer"
    FLOAT = "float"
    DATETIME = "datetime"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class ColumnProperties(Generic[T]):
    """Holds properties about a column that were extracted from the data warehouse."""

    column: SqlColumn

    type: InferenceColumnType
    row_count: int
    distinct_row_count: int
    is_nullable: bool
    null_count: int
    min_value: Optional[T]
    max_value: Optional[T]

    @property
    def is_empty(self) -> bool:
        """Whether the column has any rows"""
        return self.row_count == 0


@dataclass(frozen=True)
class TableProperties:
    """Holds properties of a table and its columns that were extracted from the data warehouse."""

    column_props: InitVar[List[ColumnProperties]]

    table: SqlTable
    columns: Dict[SqlColumn, ColumnProperties] = field(default_factory=lambda: {}, init=False)

    def __post_init__(self, column_props: List[ColumnProperties]) -> None:  # noqa: D
        for col in column_props:
            self.columns[col.column] = col


@dataclass(frozen=True)
class DataWarehouseInferenceContext(InferenceContext):
    """The inference context for a data warehouse. Holds statistics and metadata about each table and column."""

    table_props: InitVar[List[TableProperties]]

    tables: Dict[SqlTable, TableProperties] = field(default_factory=lambda: {}, init=False)
    columns: Dict[SqlColumn, ColumnProperties] = field(default_factory=lambda: {}, init=False)

    def __post_init__(self, table_props: List[TableProperties]) -> None:  # noqa: D
        for stats in table_props:
            self.tables[stats.table] = stats
            for column in stats.columns.values():
                self.columns[column.column] = column


class DataWarehouseInferenceContextProvider(InferenceContextProvider[DataWarehouseInferenceContext], ABC):
    """Provides inference context from a data warehouse by querying data from its tables."""

    def __init__(self, client: SqlClient, tables: List[SqlTable], max_sample_size: int = 1000) -> None:
        """Initialize the class.

        client: the underlying SQL engine client that will be used for querying table data.
        tables: an exhaustive list of all tables that should be queried.
        max_sample_size: max number of rows to sample from each table
        """
        self._client = client
        self.tables = tables
        self.max_sample_size = max_sample_size

    @abstractmethod
    def _get_table_properties(self, table: SqlTable) -> TableProperties:
        """Fetch properties about a single table by querying the warehouse."""
        raise NotImplementedError

    def get_context(self) -> DataWarehouseInferenceContext:
        """Query the data warehouse for statistics about all tables and populate a context with it."""
        table_props = [self._get_table_properties(table) for table in self.tables]
        return DataWarehouseInferenceContext(table_props=table_props)
