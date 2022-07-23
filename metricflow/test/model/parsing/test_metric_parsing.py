import textwrap

import pytest

from metricflow.errors.errors import ParsingException
from metricflow.model.objects.common import YamlConfigFile
from metricflow.model.objects.constraints.where import WhereClauseConstraint
from metricflow.model.objects.metric import CumulativeMetricWindow, MetricType, MetricInputMeasure
from metricflow.model.parsing.dir_to_model import parse_yaml_files_to_model
from metricflow.sql.sql_bind_parameters import SqlBindParameters
from metricflow.time.time_granularity import TimeGranularity


def test_legacy_measure_metric_parsing() -> None:
    """Test for parsing a simple metric specification with the `measure` parameter instead of `measures`"""
    yaml_contents = textwrap.dedent(
        """\
        metric:
          name: legacy_test
          type: measure_proxy
          type_params:
            measure: legacy_measure
        """
    )
    file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)

    model = parse_yaml_files_to_model(files=[file])

    assert len(model.metrics) == 1
    metric = model.metrics[0]
    assert metric.name == "legacy_test"
    assert metric.type is MetricType.MEASURE_PROXY
    assert metric.type_params.measure == MetricInputMeasure(name="legacy_measure")
    assert metric.type_params.measures is None


def test_legacy_metric_input_measure_object_parsing() -> None:
    """Test for parsing a simple metric specification with the `measure` parameter set with object notation"""
    yaml_contents = textwrap.dedent(
        """\
        metric:
          name: legacy_test
          type: measure_proxy
          type_params:
            measure:
              name: legacy_measure_from_object
              constraint: some_bool
        """
    )
    file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)

    model = parse_yaml_files_to_model(files=[file])

    assert len(model.metrics) == 1
    metric = model.metrics[0]
    assert metric.type_params.measure == MetricInputMeasure(
        name="legacy_measure_from_object",
        constraint=WhereClauseConstraint(
            where="some_bool", linkable_names=["some_bool"], sql_params=SqlBindParameters()
        ),
    )


def test_metric_metadata_parsing() -> None:
    """Test for asserting that internal metadata is parsed into the Metric object"""
    yaml_contents = textwrap.dedent(
        """\
        metric:
          name: metadata_test
          type: measure_proxy
          type_params:
            measures:
              - metadata_test_measure
        """
    )
    file = YamlConfigFile(filepath="test_dir/inline_for_test", contents=yaml_contents)

    model = parse_yaml_files_to_model(files=[file])

    assert len(model.metrics) == 1
    metric = model.metrics[0]
    assert metric.metadata is not None
    assert metric.metadata.repo_file_path == "test_dir/inline_for_test"
    assert metric.metadata.file_slice.filename == "inline_for_test"
    expected_metadata_content = textwrap.dedent(
        """\
        name: metadata_test
        type: measure_proxy
        type_params:
          measures:
          - metadata_test_measure
        """
    )
    assert metric.metadata.file_slice.content == expected_metadata_content


def test_ratio_metric_parsing() -> None:
    """Test for parsing a ratio metric specification with numerator and denominator"""
    yaml_contents = textwrap.dedent(
        """\
        metric:
          name: ratio_test
          type: ratio
          type_params:
            numerator: numerator_measure
            denominator: denominator_measure
        """
    )
    file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)

    model = parse_yaml_files_to_model(files=[file])

    assert len(model.metrics) == 1
    metric = model.metrics[0]
    assert metric.name == "ratio_test"
    assert metric.type is MetricType.RATIO
    assert metric.type_params.numerator == MetricInputMeasure(name="numerator_measure")
    assert metric.type_params.denominator == MetricInputMeasure(name="denominator_measure")
    assert metric.type_params.measures is None


def test_ratio_metric_input_measure_object_parsing() -> None:
    """Test for parsing a ratio metric specification with object inputs for numerator and denominator"""
    yaml_contents = textwrap.dedent(
        """\
        metric:
          name: ratio_test
          type: ratio
          type_params:
            numerator:
              name: numerator_measure_from_object
              constraint: "WHERE some_number > 5"
            denominator:
              name: denominator_measure_from_object
        """
    )
    file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)

    model = parse_yaml_files_to_model(files=[file])

    assert len(model.metrics) == 1
    metric = model.metrics[0]
    assert metric.type_params.numerator == MetricInputMeasure(
        name="numerator_measure_from_object",
        constraint=WhereClauseConstraint(
            where="some_number > 5", linkable_names=["some_number"], sql_params=SqlBindParameters()
        ),
    )
    assert metric.type_params.denominator == MetricInputMeasure(name="denominator_measure_from_object")


def test_expr_metric_parsing() -> None:
    """Test for parsing a metric specification with an expr and a list of measures"""
    yaml_contents = textwrap.dedent(
        """\
        metric:
          name: expr_test
          type: expr
          type_params:
            measures:
              - measure_one
              - measure_two
        """
    )
    file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)

    model = parse_yaml_files_to_model(files=[file])

    assert len(model.metrics) == 1
    metric = model.metrics[0]
    assert metric.name == "expr_test"
    assert metric.type is MetricType.EXPR
    assert metric.type_params.measures == [
        MetricInputMeasure(name="measure_one"),
        MetricInputMeasure(name="measure_two"),
    ]


def test_expr_metric_input_measure_object_parsing() -> None:
    """Test for parsing a metric specification with object inputs for the list of measures"""
    yaml_contents = textwrap.dedent(
        """\
        metric:
          name: expr_test
          type: expr
          type_params:
            measures:
              - name: measure_one_from_object
                constraint: some_bool
              - name: measure_two_from_object
        """
    )
    file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)

    model = parse_yaml_files_to_model(files=[file])

    assert len(model.metrics) == 1
    metric = model.metrics[0]
    assert metric.name == "expr_test"
    assert metric.type is MetricType.EXPR
    assert metric.type_params.measures == [
        MetricInputMeasure(
            name="measure_one_from_object",
            constraint=WhereClauseConstraint(
                where="some_bool", linkable_names=["some_bool"], sql_params=SqlBindParameters()
            ),
        ),
        MetricInputMeasure(name="measure_two_from_object"),
    ]


def test_cumulative_window_metric_parsing() -> None:
    """Test for parsing a metric specification with a cumulative window"""
    yaml_contents = textwrap.dedent(
        """\
        metric:
          name: cumulative_test
          type: cumulative
          type_params:
            measures:
              - cumulative_measure
            window: "7 days"
        """
    )
    file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)

    model = parse_yaml_files_to_model(files=[file])

    assert len(model.metrics) == 1
    metric = model.metrics[0]
    assert metric.name == "cumulative_test"
    assert metric.type is MetricType.CUMULATIVE
    assert metric.type_params.measures == [MetricInputMeasure(name="cumulative_measure")]
    assert metric.type_params.window == CumulativeMetricWindow(count=7, granularity=TimeGranularity.DAY)


def test_grain_to_date_metric_parsing() -> None:
    """Test for parsing a metric specification with the grain to date cumulative setting"""
    yaml_contents = textwrap.dedent(
        """\
        metric:
          name: grain_to_date_test
          type: cumulative
          type_params:
            measures:
              - cumulative_measure
            grain_to_date: "week"
        """
    )
    file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)

    model = parse_yaml_files_to_model(files=[file])

    assert len(model.metrics) == 1
    metric = model.metrics[0]
    assert metric.name == "grain_to_date_test"
    assert metric.type is MetricType.CUMULATIVE
    assert metric.type_params.measures == [MetricInputMeasure(name="cumulative_measure")]
    assert metric.type_params.window is None
    assert metric.type_params.grain_to_date is TimeGranularity.WEEK


def test_constraint_metric_parsing() -> None:
    """Test for parsing a metric specification with a constraint included"""
    yaml_contents = textwrap.dedent(
        """\
        metric:
          name: constraint_test
          type: measure_proxy
          type_params:
            measures:
              - input_measure
          constraint: "some_dimension IN ('value1', 'value2')"
        """
    )
    file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)

    model = parse_yaml_files_to_model(files=[file])

    assert len(model.metrics) == 1
    metric = model.metrics[0]
    assert metric.name == "constraint_test"
    assert metric.type is MetricType.MEASURE_PROXY
    assert metric.constraint == WhereClauseConstraint(
        where="some_dimension IN ('value1', 'value2')",
        linkable_names=["some_dimension"],
        sql_params=SqlBindParameters(),
    )


def test_invalid_metric_type_parsing_error() -> None:
    """Test for error detection when parsing a metric specification with an invalid MetricType input value"""
    yaml_contents = textwrap.dedent(
        """\
        metric:
          name: invalid_type_test
          type: this is not a valid type
          type_params:
            measures:
              - input_measure
        """
    )
    file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)

    with pytest.raises(ParsingException, match="'this is not a valid type' is not one of"):
        parse_yaml_files_to_model(files=[file])


def test_invalid_cumulative_metric_window_format_parsing_error() -> None:
    """Test for errror detection when parsing malformed cumulative metric window entries"""
    yaml_contents = textwrap.dedent(
        """\
        metric:
          name: invalid_cumulative_format_test
          type: cumulative
          type_params:
            measures:
              - cumulative_measure
            window: "7 days long"
        """
    )
    file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)

    with pytest.raises(ParsingException, match="Invalid window"):
        parse_yaml_files_to_model(files=[file])


def test_invalid_cumulative_metric_window_granularity_parsing_error() -> None:
    """Test for errror detection when parsing malformed cumulative metric window entries"""
    yaml_contents = textwrap.dedent(
        """\
        metric:
          name: invalid_cumulative_granularity_test
          type: cumulative
          type_params:
            measures:
              - cumulative_measure
            window: "7 moons"
        """
    )
    file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)

    with pytest.raises(ParsingException, match="Invalid time granularity"):
        parse_yaml_files_to_model(files=[file])


def test_invalid_cumulative_metric_window_count_parsing_error() -> None:
    """Test for errror detection when parsing malformed cumulative metric window entries"""
    yaml_contents = textwrap.dedent(
        """\
        metric:
          name: invalid_cumulative_count_test
          type: cumulative
          type_params:
            measures:
              - cumulative_measure
            window: "six days"
        """
    )
    file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)

    with pytest.raises(ParsingException, match="Invalid count"):
        parse_yaml_files_to_model(files=[file])
