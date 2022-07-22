import datetime as dt

from dateutil.parser import parse
from typing import Optional

from metricflow.configuration.constants import CONFIG_MODEL_PATH
from metricflow.configuration.yaml_handler import YamlFileHandler
from metricflow.errors.errors import ModelCreationException
from metricflow.model.objects.user_configured_model import UserConfiguredModel
from metricflow.model.parsing.dir_to_model import ModelBuildResult, parse_directory_of_yaml_files_to_model
from metricflow.sql_clients.common_client import not_empty


def path_to_models(handler: YamlFileHandler) -> str:
    """Given a YamlFileHandler, return the path to the YAML model config files"""
    return not_empty(handler.get_value(CONFIG_MODEL_PATH), CONFIG_MODEL_PATH, handler.url)


def model_build_result_from_config(
    handler: YamlFileHandler, raise_issues_as_exceptions: bool = True
) -> ModelBuildResult:
    """Given a yaml file, creates a ModelBuildResult.

    Args:
        handler: a file handler for loading the configs from
        raise_issues_as_exceptions: determines if issues should be raised, or returned as issues

    Returns:
        ModelBuildResult that contains the UserConfigureModel and any associated ValidationIssues
    """
    models_path = path_to_models(handler=handler)
    try:
        return parse_directory_of_yaml_files_to_model(
            models_path, raise_issues_as_exceptions=raise_issues_as_exceptions
        )
    except Exception as e:
        raise ModelCreationException from e


def build_user_configured_model_from_config(handler: YamlFileHandler) -> UserConfiguredModel:
    """Given a yaml file, create a UserConfiguredModel."""
    return model_build_result_from_config(handler=handler).model


def convert_to_datetime(datetime_str: Optional[str]) -> Optional[dt.datetime]:
    """Callback to convert string to datetime given as an iso8601 timestamp."""
    if datetime_str is None:
        return None

    try:
        return parse(datetime_str)
    except Exception:
        raise ValueError(f"'{datetime_str}' is not a valid iso8601 timestamp")
