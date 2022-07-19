import datetime
import logging
import traceback
from typing import List
from metricflow.instances import MaterializationModelReference

from metricflow.dataflow.builder.node_data_set import DataflowPlanNodeOutputDataSetResolver
from metricflow.dataflow.builder.source_node import SourceNodeBuilder
from metricflow.dataset.convert_data_source import DataSourceToDataSetConverter
from metricflow.dataset.data_source_adapter import DataSourceDataSet
from metricflow.dataset.dataset import DataSet
from metricflow.model.objects.materialization import Materialization
from metricflow.model.objects.user_configured_model import UserConfiguredModel
from metricflow.model.semantic_model import SemanticModel
from metricflow.model.validations.validator_helpers import (
    FileContext,
    MaterializationContext,
    ModelValidationRule,
    ValidationError,
    ValidationIssueType,
    validate_safely,
    ValidationFutureError,
)
from metricflow.naming.linkable_spec_name import StructuredLinkableSpecName
from metricflow.plan_conversion.column_resolver import DefaultColumnAssociationResolver
from metricflow.plan_conversion.time_spine import TimeSpineSource
from metricflow.query.query_parser import MetricFlowQueryParser

logger = logging.getLogger(__name__)


class ValidMaterializationRule(ModelValidationRule):
    """Check that a materialization config is valid.

    * Metrics listed are valid.
    * Dimensions or identifiers are valid for the listed metrics.
    * Primary time dimension is listed in the dimensions section.
    """

    @staticmethod
    @validate_safely(whats_being_done="checking materialization is defined correctly")
    def _validate_materialization(
        materialization: Materialization,
        mf_query_parser: MetricFlowQueryParser,
    ) -> List[ValidationIssueType]:
        issues: List[ValidationIssueType] = []

        context = MaterializationContext(
            file_context=FileContext.from_metadata(metadata=materialization.metadata),
            materialization=MaterializationModelReference(materialization_name=materialization.name),
        )

        if not materialization.dimensions:
            return [
                ValidationError(
                    context=context,
                    message=f"Materialization '{materialization.name}' does not have dimensions listed",
                )
            ]

        try:
            mf_query_parser.parse_and_validate_query(
                metric_names=materialization.metrics,
                group_by_names=materialization.dimensions,
            )
        # TODO: Using broad exception clause until the query validation returns a list of errors.
        except Exception as err:
            issues.append(
                ValidationFutureError(
                    context=context,
                    message=str(err),
                    error_date=datetime.date(2022, 5, 23),
                    extra_detail="".join(traceback.format_tb(err.__traceback__)),
                )
            )

        # Primary dimension checks
        contained_metric_time_dimension_names: List[str] = []
        for mat_dimension_or_identifier_name in materialization.dimensions:
            structured_spec = StructuredLinkableSpecName.from_name(mat_dimension_or_identifier_name)
            if structured_spec.element_name == DataSet.metric_time_dimension_name():
                contained_metric_time_dimension_names.append(mat_dimension_or_identifier_name)

        if len(contained_metric_time_dimension_names) == 0:
            issues.append(
                # Leaving as a warning to avoid model validation failures during the transition
                ValidationFutureError(
                    context=context,
                    message=f"Metric time dimension {DataSet.metric_time_dimension_name()} not listed"
                    f" as a dimension in materialization {materialization.name}",
                    error_date=datetime.date(2022, 8, 1),
                )
            )

        if len(contained_metric_time_dimension_names) > 1:
            issues.append(
                ValidationError(
                    context=context,
                    message=f"Multiple metric time dimensions {contained_metric_time_dimension_names} listed in "
                    f"materialization {materialization.name}",
                )
            )

        return issues

    @staticmethod
    @validate_safely(whats_being_done="running model validation ensuring materializations are valid")
    def validate_model(model: UserConfiguredModel) -> List[ValidationIssueType]:  # noqa: D
        """Check that all of the metrics and dimensions listed in a materialization are valid."""
        issues: List[ValidationIssueType] = []

        semantic_model = SemanticModel(model)
        source_data_sets: List[DataSourceDataSet] = []
        converter = DataSourceToDataSetConverter(
            column_association_resolver=DefaultColumnAssociationResolver(semantic_model)
        )
        for data_source in semantic_model.user_configured_model.data_sources:
            data_set = converter.create_sql_source_data_set(data_source)
            source_data_sets.append(data_set)

        # Any schema will work since we're just using it to render the output.
        time_spine_source = TimeSpineSource(schema_name="dummy_schema")
        node_output_resolver = DataflowPlanNodeOutputDataSetResolver[DataSourceDataSet](
            column_association_resolver=DefaultColumnAssociationResolver(semantic_model),
            semantic_model=semantic_model,
            time_spine_source=time_spine_source,
        )
        source_node_builder = SourceNodeBuilder(semantic_model)
        source_nodes = source_node_builder.create_from_data_sets(source_data_sets)

        mf_query_parser = MetricFlowQueryParser(
            model=SemanticModel(user_configured_model=model),
            source_nodes=source_nodes,
            node_output_resolver=node_output_resolver,
        )

        for materialization in model.materializations:
            issues += ValidMaterializationRule._validate_materialization(
                materialization=materialization,
                mf_query_parser=mf_query_parser,
            )
        return issues
