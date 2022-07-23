from __future__ import annotations

from hashlib import sha1
from typing import Any, List, Optional

from metricflow.errors.errors import ParsingException
from metricflow.model.objects.common import Metadata
from metricflow.model.objects.constraints.where import WhereClauseConstraint
from metricflow.model.objects.base import HashableBaseModel, ModelWithMetadataParsing, PydanticCustomInputParser
from metricflow.object_utils import ExtendedEnum
from metricflow.specs import MeasureReference
from metricflow.time.time_granularity import TimeGranularity
from metricflow.time.time_granularity import string_to_time_granularity


class MetricType(ExtendedEnum):
    """Currently supported metric types"""

    MEASURE_PROXY = "measure_proxy"
    RATIO = "ratio"
    EXPR = "expr"
    CUMULATIVE = "cumulative"


class MetricInputMeasure(PydanticCustomInputParser, HashableBaseModel):
    """Provides a pointer to a measure along with metric-specific processing directives

    If an alias is set, this will be used as the string name reference for this measure after the aggregation
    phase in the SQL plan.
    """

    name: str
    constraint: Optional[WhereClauseConstraint]
    alias: Optional[str]

    @classmethod
    def _from_yaml_value(cls, input: Any):
        """Parses a MetricInputMeasure from a string (name only) or object (struct spec) input

        Internally, we will pass fully formed instance of a MetricInputMeasure through in any case where
        the Measure object defined in the DataSource has `create_metric` set to True. For these cases, we
        do not need to do further parsing.

        For user input cases, the original YAML spec for a Metric included measure(s) specified as string names
        or lists of string names. As such, configs pre-dating the addition of this model type will only provide the
        base name for this object. The addition of new properties requires a key/value input for each
        entry, which would need to be handled by object (dict) type parsing logic.
        """
        if isinstance(input, MetricInputMeasure):
            # Internally constructed, pass it through and ignore this case in error messaging
            return input
        elif isinstance(input, str):
            return MetricInputMeasure(name=input)
        elif isinstance(input, dict):
            return MetricInputMeasure(**input)
        else:
            raise ValueError(
                f"MetricInputMeasure inputs from model configs are expected to be of either type string or "
                f"object (key/value pairs), but got type {type(input)} with value: {input}"
            )

    @property
    def measure_reference(self) -> MeasureReference:
        """Property accessor to get the MeasureReference associated with this metric input measure"""
        return MeasureReference(element_name=self.name)

    @property
    def post_aggregation_measure_reference(self) -> MeasureReference:
        """Property accessor to get the MeasureReference with the aliased name, if appropriate"""
        return MeasureReference(element_name=self.alias or self.name)


class CumulativeMetricWindow(PydanticCustomInputParser, HashableBaseModel):
    """Describes the window of time the metric should be accumulated over. ie '1 day', '2 weeks', etc"""

    count: int
    granularity: TimeGranularity

    def to_string(self) -> str:  # noqa: D
        return f"{self.count} {self.granularity.value}"

    @classmethod
    def _from_yaml_value(cls, input: Any) -> CumulativeMetricWindow:
        """Parses a CumulativeMetricWindow from a string input found in a user provided model specification

        The CumulativeMetricWindow is always expected to be provided as a string in user-defined YAML configs.
        It may also be a CumulativeMetricWindow in cases where we are internally constructing Metric objects.
        """
        if isinstance(input, str):
            return CumulativeMetricWindow.parse(input)
        elif isinstance(input, CumulativeMetricWindow):
            # This is internally constructed, pass it through and ignore it in error messaging
            return input
        else:
            raise ValueError(
                f"CumulativeMetricWindow inputs from model configs are expected to always be of type string, but got "
                f"type {type(input)} with value: {input}"
            )

    @staticmethod
    def parse(window: str) -> CumulativeMetricWindow:
        """Returns window values if parsing succeeds, None otherwise

        Output of the form: (<time unit count>, <time granularity>, <error message>) - error message is None if window is formatted properly
        """
        parts = window.split(" ")
        if len(parts) != 2:
            raise ParsingException(
                f"Invalid window ({window}) in cumulative metric. Should be of the form `<count> <granularity>`, ie `28 days`",
            )

        granularity = parts[1]
        # if we switched to python 3.9 this could just be `granularity = parts[0].removesuffix('s')
        if granularity.endswith("s"):
            # months -> month
            granularity = granularity[:-1]
        if granularity not in [item.value for item in TimeGranularity]:
            raise ParsingException(
                f"Invalid time granularity {granularity} in cumulative metric window string: ({window})",
            )

        count = parts[0]
        if not count.isdigit():
            raise ParsingException(f"Invalid count ({count}) in cumulative metric window string: ({window})")

        return CumulativeMetricWindow(
            count=int(count),
            granularity=string_to_time_granularity(granularity),
        )


class MetricTypeParams(HashableBaseModel):
    """Type params add additional context to certain metric types (the context depends on the metric type)"""

    measure: Optional[MetricInputMeasure]
    measures: Optional[List[MetricInputMeasure]]
    numerator: Optional[MetricInputMeasure]
    denominator: Optional[MetricInputMeasure]
    expr: Optional[str]
    window: Optional[CumulativeMetricWindow]
    grain_to_date: Optional[TimeGranularity]

    @property
    def numerator_measure_reference(self) -> Optional[MeasureReference]:
        """Return the measure reference, if any, associated with the metric input measure defined as the numerator"""
        return self.numerator.measure_reference if self.numerator else None

    @property
    def denominator_measure_reference(self) -> Optional[MeasureReference]:
        """Return the measure reference, if any, associated with the metric input measure defined as the denominator"""
        return self.denominator.measure_reference if self.denominator else None


class Metric(HashableBaseModel, ModelWithMetadataParsing):
    """Describes a metric"""

    name: str
    description: Optional[str]
    type: MetricType
    type_params: MetricTypeParams
    constraint: Optional[WhereClauseConstraint]
    metadata: Optional[Metadata]

    @property
    def input_measures(self) -> List[MetricInputMeasure]:
        """Return the complete list of input measure configurations for this metric"""
        tp = self.type_params
        res = tp.measures or []
        if tp.measure:
            res.append(tp.measure)
        if tp.numerator:
            res.append(tp.numerator)
        if tp.denominator:
            res.append(tp.denominator)

        return res

    @property
    def measure_references(self) -> List[MeasureReference]:
        """Return the measure references associated with all input measure configurations for this metric"""
        return [x.measure_reference for x in self.input_measures]

    @property
    def definition_hash(self) -> str:  # noqa: D
        values: List[Optional[str]] = [self.name, self.type_params.expr or ""]
        if self.constraint:
            values.append(self.constraint.where)
            if self.constraint.linkable_names:
                values.extend(self.constraint.linkable_names)
        values.extend([m.element_name for m in self.measure_references])

        hash_builder = sha1()
        for s in values:
            if s is None:
                continue
            hash_builder.update(s.encode("utf-8"))
        return hash_builder.hexdigest()
