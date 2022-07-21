from typing import Dict, Any

from jsonschema import RefResolver
from metricflow.model.parsing.schema_validator import SchemaValidator

from metricflow.model.parsing.schemas import (
    metric_schema,
    data_source_schema,
    materialization_schema,
    derived_group_by_element_schema,
    metric_input_measure_schema,
    metric_type_params_schema,
    identifier_schema,
    measure_schema,
    dimension_schema,
    dimension_type_params_schema,
    mutability_schema,
    mutability_type_params_schema,
    composite_sub_identifier_schema,
    materialization_destination_schema,
)


def add_transform_metadata_fields_to_spec(spec: Dict[str, Any]) -> None:  # type: ignore[misc]
    """Adds transform metadata fields a spec"""
    properties = spec["properties"]
    transform_metadata_fields = {
        "description": {"type": "string"},
        "owners": {
            "type": "array",
            "items": {"type": "string"},
        },
        "display_name": {"type": "string"},
        "tier": {"type": ["string", "integer"]},
    }

    for k, v in transform_metadata_fields.items():
        if k in properties:
            raise RuntimeError(f"Spec with id: {spec['$id']} contains transform metadata field: {k}")

        properties[k] = v


def add_locked_metadata_to_spec(spec: Dict[str, Any]) -> None:  # type: ignore[misc]
    """Adds locked metadata field to a spec"""
    properties = spec["properties"]
    transform_metadata_fields = {
        "locked_metadata": {"$ref": "locked_metadata"},
    }

    for k, v in transform_metadata_fields.items():
        if k in properties:
            raise RuntimeError(f"Spec with id: {spec['$id']} contains transform metadata field: {k}")

        properties[k] = v


# Sub-object schemas
locked_metadata_schema = {
    "$id": "locked_metadata",
    "type": "object",
    "properties": {
        "value_format": {"type": "string"},
        "description": {"type": "string"},
        "display_name": {"type": "string"},
        "tier": {"type": ["string", "integer"]},
        "increase_is_good": {"type": "boolean"},
    },
    "additionalProperties": False,
}


# Add transform metadata fields to top level objects (metric, data source, derived identifier)
add_transform_metadata_fields_to_spec(dimension_schema)
add_transform_metadata_fields_to_spec(measure_schema)
add_transform_metadata_fields_to_spec(identifier_schema)

add_transform_metadata_fields_to_spec(metric_schema)
add_locked_metadata_to_spec(metric_schema)

add_transform_metadata_fields_to_spec(data_source_schema)
add_transform_metadata_fields_to_spec(materialization_schema)
add_transform_metadata_fields_to_spec(derived_group_by_element_schema)


schema_store = {
    # Top level schemas
    metric_schema["$id"]: metric_schema,
    data_source_schema["$id"]: data_source_schema,
    derived_group_by_element_schema["$id"]: derived_group_by_element_schema,
    materialization_schema["$id"]: materialization_schema,
    # Sub-object schemas
    metric_input_measure_schema["$id"]: metric_input_measure_schema,
    metric_type_params_schema["$id"]: metric_type_params_schema,
    locked_metadata_schema["$id"]: locked_metadata_schema,
    identifier_schema["$id"]: identifier_schema,
    measure_schema["$id"]: measure_schema,
    dimension_schema["$id"]: dimension_schema,
    dimension_type_params_schema["$id"]: dimension_type_params_schema,
    mutability_schema["$id"]: mutability_schema,
    mutability_type_params_schema["$id"]: mutability_type_params_schema,
    composite_sub_identifier_schema["$id"]: composite_sub_identifier_schema,
    materialization_destination_schema["$id"]: materialization_destination_schema,
}

resolver = RefResolver.from_schema(schema=metric_schema, store=schema_store)
data_source_validator = SchemaValidator(data_source_schema, resolver=resolver)
derived_group_by_element_validator = SchemaValidator(derived_group_by_element_schema, resolver=resolver)
materialization_validator = SchemaValidator(materialization_schema, resolver=resolver)
metric_validator = SchemaValidator(metric_schema, resolver=resolver)
