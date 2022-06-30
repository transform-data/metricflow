from __future__ import annotations

import functools
import json
import traceback
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date
from enum import Enum
from pydantic import BaseModel, Extra
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from metricflow.model.objects.elements.dimension import DimensionType
from metricflow.model.objects.user_configured_model import UserConfiguredModel

VALIDATE_SAFELY_ERROR_STR_TMPLT = ". Issue occurred in method `{method_name}` called with {arguments_str}"
ValidationContextJSON = Dict[str, Union[str, int, None]]
ValidationIssueJSON = Dict[str, Union[str, int, ValidationContextJSON]]


class ValidationIssueLevel(Enum):
    """Categorize the issues found while validating a MQL model."""

    # Issue should be fixed, but model will still work in MQL
    WARNING = 0
    # Issue doesn't prevent model from working in MQL yet, but will eventually be an error
    FUTURE_ERROR = 1
    # Issue will prevent the model from working in MQL
    ERROR = 2
    # Issue is blocking and further validation should be stopped.
    FATAL = 3


class ModelObjectType(Enum):
    """Maps object types in the models to a readable string."""

    DATA_SOURCE = "data_source"
    MATERIALIZATION = "materialization"
    MEASURE = "measure"
    DIMENSION = "dimension"
    IDENTIFIER = "identifier"
    METRIC = "metric"


class FileContext(BaseModel):
    """The base context class for validation issues"""

    file_name: Optional[str]
    line_number: Optional[int]

    class Config:
        """Pydantic class configuration options"""

        extra = Extra.forbid

    def context_str(self) -> str:
        """Human readable stringified representation of the context"""

        context_string = ""

        if self.file_name:
            context_string += f"in file `{self.file_name}`"
            if self.line_number:
                context_string += f" on line #{self.line_number}"

        return context_string


class MaterializationContext(BaseModel):
    """The context class for validation issues involving materializations"""

    file_context: FileContext
    materialization_name: str

    def context_str(self) -> str:
        """Human readable stringified representation of the context"""
        return f"with materialization `{self.materialization_name}` {self.file_context.context_str()}"


class MetricContext(BaseModel):
    """The context class for validation issues involving metrics"""

    file_context: FileContext
    metric_name: str

    def context_str(self) -> str:
        """Human readable stringified representation of the context"""
        return f"with metric `{self.metric_name}` {self.file_context.context_str()}"


class DataSourceContext(BaseModel):
    """The context class for validation issues involving data sources"""

    file_context: FileContext
    data_source_name: str

    def context_str(self) -> str:
        """Human readable stringified representation of the context"""
        return f"with data source `{self.data_source_name}` {self.file_context.context_str()}"


class DimensionContext(BaseModel):
    """The context class for validation issues involving dimensions"""

    file_context: FileContext
    data_source_name: str
    dimension_name: str

    def context_str(self) -> str:
        """Human readable stringified representation of the context"""
        return f"with dimension `{self.dimension_name}` in data source `{self.data_source_name}` {DataSourceContext.context_str(self)}"


class IdentifierContext(BaseModel):
    """The context class for validation issues involving indentifiers"""

    file_context: FileContext
    data_source_name: str
    identifier_name: str

    def context_str(self) -> str:
        """Human readable stringified representation of the context"""
        return f"with identifier `{self.identifier_name}` in data source `{self.data_source_name}` {DataSourceContext.context_str(self)}"


class MeasureContext(BaseModel):
    """The context class for validation issues involving measures"""

    file_context: FileContext
    data_source_name: str
    measure_name: str

    def context_str(self) -> str:
        """Human readable stringified representation of the context"""
        return f"with measure `{self.measure_name}` in data source `{self.data_source_name}` {DataSourceContext.context_str(self)}"


ValidationContext = Union[
    FileContext,
    MaterializationContext,
    MetricContext,
    DimensionContext,
    MeasureContext,
    IdentifierContext,
    DataSourceContext,
]


class ValidationIssue(BaseModel):
    """An issue that was found while validating the MetricFlow model."""

    level: ValidationIssueLevel
    message: str
    context: Optional[ValidationContext]
    # Consider adding a enum here that categories the type of validation issue and standardize the error messages.

    def as_readable_str(self, with_level: bool = True) -> str:
        """Return a easily readable string that can be used to log the issue."""
        msg_base = f"{self.level.name} " if with_level else ""
        msg_base += self.context.context_str() if self.context else ""
        if msg_base:
            msg_base += " - "
        return msg_base + self.message

    @staticmethod
    def from_json(json_str: str) -> ValidationIssue:
        """Convert a json string representing a ValidationIssue into a ValidationIssue"""

        return ValidationIssue.from_dict(json_dict=json.loads(json_str))

    def from_dict(json_dict: ValidationIssueJSON) -> ValidationIssue:
        """Convert a dict representing a ValidationIssue into a ValidationIssue"""

        context_dict = json_dict["context"]
        assert isinstance(context_dict, Dict)
        context = ValidationContext.from_dict(context_dict)

        message = json_dict["message"]
        level = json_dict["level"]

        if level == ValidationIssueLevel.FUTURE_ERROR.value:
            error_date_str = json_dict["error_date"]
            assert isinstance(error_date_str, str)
            error_date = date.fromisoformat(error_date_str)

            return ValidationFutureError(context=context, message=message, error_date=error_date)
        else:
            return ValidationIssue(
                context=context,
                level=level,
                message=message,
            )


class ValidationWarning(ValidationIssue):
    """A warning that was found while validation the model."""

    level = ValidationIssueLevel.WARNING


class ValidationFutureError(ValidationIssue):
    """A future error that was found while validation the model."""

    error_date: date
    level = ValidationIssueLevel.FUTURE_ERROR

    def as_readable_str(self, with_level: bool = True) -> str:
        """Return a easily readable string that can be used to log the issue."""
        return (
            f"{ValidationIssue.as_readable_str(self, with_level=with_level)}"
            f"IMPORTANT: this error will break your model starting {self.error_date.strftime('%b %d, %Y')}. "
        )


class ValidationError(ValidationIssue):
    """An error that was found while validating the model."""

    level = ValidationIssueLevel.ERROR


class ValidationFatal(ValidationIssue):
    """A fatal issue that was found while validation the model."""

    level = ValidationIssueLevel.FATAL


ValidationIssueType = Union[ValidationIssue, ValidationWarning, ValidationFutureError, ValidationError, ValidationFatal]


def generate_exception_issue(
    what_was_being_done: str,
    e: Exception,
    context: Optional[ValidationContext] = None,
) -> ValidationIssue:
    """Generates a validation issue for exceptions"""
    return ValidationError(
        context=context,
        message=f"An error occured while {what_was_being_done} - {''.join(traceback.format_exception(etype=type(e), value=e, tb=e.__traceback__))}",
    )


def _func_args_to_string(*args: Any, **kwargs: Any) -> str:  # type: ignore
    return f"positional args: {args}, key word args: {kwargs}"


def validate_safely(whats_being_done: str) -> Callable:
    """Decorator to safely run validation checks"""

    def decorator_check_element_safely(func: Callable) -> Callable:  # noqa
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> List[ValidationIssueType]:  # type: ignore
            """Safely run a check on model elements"""
            issues: List[ValidationIssueType]
            try:
                issues = func(*args, **kwargs)
            except Exception as e:
                arguments_str = _func_args_to_string(*args, **kwargs)
                issues = [
                    generate_exception_issue(
                        what_was_being_done=whats_being_done
                        + VALIDATE_SAFELY_ERROR_STR_TMPLT.format(
                            method_name=func.__name__, arguments_str=arguments_str
                        ),
                        e=e,
                    )
                ]
            return issues

        return wrapper

    return decorator_check_element_safely


@dataclass(frozen=True)
class DimensionInvariants:
    """Helper object to ensure consistent dimension attributes across data sources.

    All dimensions with a given name in all data sources should have attributes matching these values.
    """

    type: DimensionType
    is_partition: bool


class ModelValidationRule(ABC):
    """Encapsulates logic for checking the values of objects in a model."""

    @staticmethod
    @abstractmethod
    def validate_model(model: UserConfiguredModel) -> List[ValidationIssueType]:
        """Check the given model and return a list of validation issues"""
        pass


class ModelValidationException(Exception):
    """Exception raised when validation of a model fails."""

    def __init__(self, issues: Tuple[ValidationIssueType, ...]) -> None:  # noqa: D
        issues_str = "\n".join([x.as_readable_str() for x in issues])
        super().__init__(f"Error validating model. Issues:\n{issues_str}")
