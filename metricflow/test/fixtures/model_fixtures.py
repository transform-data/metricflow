from __future__ import annotations

import logging
import os
from collections import OrderedDict
from dataclasses import dataclass
from typing import List, Dict, Sequence

import pytest

from metricflow.dataflow.builder.source_node import SourceNodeBuilder
from metricflow.dataflow.dataflow_plan import ReadSqlSourceNode, BaseOutput
from metricflow.dataset.convert_data_source import DataSourceToDataSetConverter
from metricflow.model.objects.data_source import DataSource
from metricflow.model.objects.user_configured_model import UserConfiguredModel
from metricflow.model.parsing.dir_to_model import parse_directory_of_yaml_files_to_model
from metricflow.model.semantic_model import SemanticModel
from metricflow.plan_conversion.column_resolver import DefaultColumnAssociationResolver
from metricflow.dataset.data_source_adapter import DataSourceDataSet
from metricflow.test.fixtures.id_fixtures import IdNumberSpace, patch_id_generators_helper
from metricflow.test.fixtures.setup_fixtures import MetricFlowTestSessionState

logger = logging.getLogger(__name__)


def _data_set_to_read_nodes(
    data_sets: OrderedDict[str, DataSourceDataSet]
) -> OrderedDict[str, ReadSqlSourceNode[DataSourceDataSet]]:
    """Return a mapping from the name of the data source to the dataflow plan node that reads from it."""
    return_dict: OrderedDict[str, ReadSqlSourceNode[DataSourceDataSet]] = OrderedDict()
    for data_source_name, data_set in data_sets.items():
        return_dict[data_source_name] = ReadSqlSourceNode[DataSourceDataSet](data_set)
        logger.debug(f"For data source {data_source_name}, creating node_id {return_dict[data_source_name].node_id}")

    return return_dict


def _data_set_to_source_nodes(
    semantic_model: SemanticModel, data_sets: OrderedDict[str, DataSourceDataSet]
) -> Sequence[BaseOutput[DataSourceDataSet]]:
    source_node_builder = SourceNodeBuilder(semantic_model)
    return source_node_builder.create_from_data_sets(list(data_sets.values()))


@dataclass(frozen=True)
class ConsistentIdObjectRepository:
    """Stores all objects that should have consistent IDs in tests."""

    simple_model_data_sets: OrderedDict[str, DataSourceDataSet]
    simple_model_read_nodes: OrderedDict[str, ReadSqlSourceNode[DataSourceDataSet]]
    simple_model_source_nodes: Sequence[BaseOutput[DataSourceDataSet]]

    multihop_model_read_nodes: OrderedDict[str, ReadSqlSourceNode[DataSourceDataSet]]
    multihop_model_source_nodes: Sequence[BaseOutput[DataSourceDataSet]]

    composite_model_read_nodes: OrderedDict[str, ReadSqlSourceNode[DataSourceDataSet]]
    composite_model_source_nodes: Sequence[BaseOutput[DataSourceDataSet]]


@pytest.fixture(scope="session")
def consistent_id_object_repository(
    simple_semantic_model: SemanticModel,
    multi_hop_join_semantic_model: SemanticModel,
    composite_identifier_semantic_model: SemanticModel,
) -> ConsistentIdObjectRepository:  # noqa: D
    """Create objects that have incremental numeric IDs with a consistent value.

    This should use IDs with a high enough value so that when other tests run with ID generators set to 0 at the start
    of the test and create objects, there is no overlap in the IDs.
    """

    with patch_id_generators_helper(start_value=IdNumberSpace.CONSISTENT_ID_REPOSITORY):
        sm_data_sets = create_data_sets(simple_semantic_model)
        multihop_data_sets = create_data_sets(multi_hop_join_semantic_model)
        composite_data_sets = create_data_sets(composite_identifier_semantic_model)

        return ConsistentIdObjectRepository(
            simple_model_data_sets=sm_data_sets,
            simple_model_read_nodes=_data_set_to_read_nodes(sm_data_sets),
            simple_model_source_nodes=_data_set_to_source_nodes(simple_semantic_model, sm_data_sets),
            multihop_model_read_nodes=_data_set_to_read_nodes(multihop_data_sets),
            multihop_model_source_nodes=_data_set_to_source_nodes(multi_hop_join_semantic_model, multihop_data_sets),
            composite_model_read_nodes=_data_set_to_read_nodes(composite_data_sets),
            composite_model_source_nodes=_data_set_to_source_nodes(
                composite_identifier_semantic_model, composite_data_sets
            ),
        )


def create_data_sets(multihop_semantic_model: SemanticModel) -> OrderedDict[str, DataSourceDataSet]:
    """Convert the DataSources in the model to SqlDataSets.

    Key is the name of the data source, value is the associated data set.
    """
    # Use ordered dict and sort by name to get consistency when running tests.
    data_sets = OrderedDict()
    data_sources: List[DataSource] = multihop_semantic_model.user_configured_model.data_sources
    data_sources.sort(key=lambda x: x.name)

    converter = DataSourceToDataSetConverter(
        column_association_resolver=DefaultColumnAssociationResolver(multihop_semantic_model)
    )

    for data_source in data_sources:
        data_sets[data_source.name] = converter.create_sql_source_data_set(data_source)

    return data_sets


@pytest.fixture(scope="session")
def template_mapping(mf_test_session_state: MetricFlowTestSessionState) -> Dict[str, str]:
    """Mapping for template variables in the model YAML files."""
    schema = mf_test_session_state.mf_source_schema
    return {
        "bookings_source_query": f"SELECT * FROM {schema}.fct_bookings_dt ",
        "bookings_source_table": f"{schema}.fct_bookings",
        "views_source_table": f"{schema}.fct_views",
        "listings_latest_table": f"{schema}.dim_listings_latest",
        "listings_latest": f"{schema}.dim_listings_latest_table",
        "dim_listings_latest_table": f"{schema}.dim_listings_latest",
        "users_latest_table": f"{schema}.dim_users_latest",
        "dim_users_table": f"{schema}.dim_users",
        "fct_id_verifications_table": f"{schema}.fct_id_verifications",
        "fct_revenue_table": f"{schema}.fct_revenue",
        "dim_lux_listing_id_mapping_table": f"{schema}.dim_lux_listing_id_mapping",
        "thorium_table": f"{schema}.thorium",
        "osmium_table": f"{schema}.osmium",
        "dysprosium_table": f"{schema}.dysprosium",
        "dim_companies_table": f"{schema}.dim_companies",
        "source_schema": schema,
    }


@pytest.fixture(scope="session")
def simple_semantic_model_non_ds(template_mapping: Dict[str, str]) -> SemanticModel:  # noqa: D
    model_build_result = parse_directory_of_yaml_files_to_model(
        os.path.join(os.path.dirname(__file__), "model_yamls/non_ds_model"), template_mapping=template_mapping
    )
    return SemanticModel(model_build_result.model)


@pytest.fixture(scope="session")
def simple_semantic_model(template_mapping: Dict[str, str]) -> SemanticModel:  # noqa: D
    model_build_result = parse_directory_of_yaml_files_to_model(
        os.path.join(os.path.dirname(__file__), "model_yamls/simple_model"), template_mapping=template_mapping
    )
    return SemanticModel(model_build_result.model)


@pytest.fixture(scope="session")
def composite_identifier_semantic_model(  # noqa: D
    template_mapping: Dict[str, str], mf_test_session_state: MetricFlowTestSessionState
) -> SemanticModel:
    schema = mf_test_session_state.mf_source_schema
    template_mapping = {
        "fct_messages_table": f"{schema}.fct_messages",
        "fct_users_table": f"{schema}.fct_users",
        "fct_users_more_table": f"{schema}.fct_users_more",
    }
    model_build_result = parse_directory_of_yaml_files_to_model(
        os.path.join(os.path.dirname(__file__), "model_yamls/composite_identifier_model"),
        template_mapping=template_mapping,
    )
    return SemanticModel(model_build_result.model)


@pytest.fixture(scope="session")
def multi_hop_join_semantic_model(mf_test_session_state: MetricFlowTestSessionState) -> SemanticModel:  # noqa: D
    schema = mf_test_session_state.mf_source_schema
    template_mapping = {
        "account_month_txns": f"{schema}.account_month_txns",
        "customer_table": f"{schema}.customer_table",
        "third_hop_table": f"{schema}.third_hop_table",
        "customer_other_data": f"{schema}.customer_other_data",
        "bridge_table": f"{schema}.bridge_table",
    }
    model_build_result = parse_directory_of_yaml_files_to_model(
        os.path.join(os.path.dirname(__file__), "model_yamls/multi_hop_join_model/partitioned_data_sources"),
        template_mapping=template_mapping,
    )
    return SemanticModel(model_build_result.model)


@pytest.fixture(scope="session")
def unpartitioned_multi_hop_join_semantic_model(  # noqa: D
    mf_test_session_state: MetricFlowTestSessionState,
) -> SemanticModel:
    schema = mf_test_session_state.mf_source_schema
    template_mapping = {
        "account_month_txns": f"{schema}.account_month_txns",
        "customer_table": f"{schema}.customer_table",
        "third_hop_table": f"{schema}.third_hop_table",
        "customer_other_data": f"{schema}.customer_other_data",
        "bridge_table": f"{schema}.bridge_table",
    }
    model_build_result = parse_directory_of_yaml_files_to_model(
        os.path.join(os.path.dirname(__file__), "model_yamls/multi_hop_join_model/unpartitioned_data_sources"),
        template_mapping=template_mapping,
    )
    return SemanticModel(model_build_result.model)


@pytest.fixture(scope="session")
def simple_user_configured_model(template_mapping: Dict[str, str]) -> UserConfiguredModel:
    """Model used for many tests."""

    model_build_result = parse_directory_of_yaml_files_to_model(
        os.path.join(os.path.dirname(__file__), "model_yamls/simple_model"), template_mapping=template_mapping
    )
    return model_build_result.model


@pytest.fixture(scope="session")
def simple_model__pre_transforms(template_mapping: Dict[str, str]) -> UserConfiguredModel:
    """Model used for tests pre-transformations."""

    model_build_result = parse_directory_of_yaml_files_to_model(
        os.path.join(os.path.dirname(__file__), "model_yamls/simple_model"),
        template_mapping=template_mapping,
        apply_pre_transformations=True,
        apply_post_transformations=False,
    )
    return model_build_result.model


@pytest.fixture(scope="session")
def extended_date_semantic_model(mf_test_session_state: MetricFlowTestSessionState) -> SemanticModel:  # noqa: D
    schema = mf_test_session_state.mf_source_schema
    template_mapping = {
        "fct_bookings_extended_table": f"{schema}.fct_bookings_extended",
        "fct_bookings_extended_monthly_table": f"{schema}.fct_bookings_extended_monthly",
        "dim_listings_extended_table": f"{schema}.dim_listings_extended",
    }
    model_build_result = parse_directory_of_yaml_files_to_model(
        os.path.join(os.path.dirname(__file__), "model_yamls/extended_date_model"),
        template_mapping=template_mapping,
    )
    return SemanticModel(model_build_result.model)


@pytest.fixture(scope="session")
def data_warehouse_validation_model(template_mapping: Dict[str, str]) -> UserConfiguredModel:
    """Model used for data warehouse validation tests"""

    model_build_result = parse_directory_of_yaml_files_to_model(
        os.path.join(os.path.dirname(__file__), "model_yamls/data_warehouse_validation_model"),
        template_mapping=template_mapping,
    )
    return model_build_result.model


@pytest.fixture(scope="session")
def config_linter_model_path() -> str:
    """Model path for ConfigLinter tests"""

    return os.path.join(os.path.dirname(__file__), "model_yamls/config_linter_model")
