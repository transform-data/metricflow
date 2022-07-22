from jsonschema import RefResolver

from metricflow.model.parsing.schema_validator import SchemaValidator

TRANSFORM_OBJECT_NAME_PATTERN = "(?!.*__).*^[a-z][a-z0-9_]*[a-z0-9]$"


# Enums
materialization_location_tableau_values = ["TABLEAU", "tableau"]
materialization_location_values = ["DW", "FAST_CACHE"]
materialization_location_values += [x.lower() for x in materialization_location_values]
materialization_location_values += materialization_location_tableau_values

materialization_format_values = ["WIDE"]
materialization_format_values += [x.lower() for x in materialization_format_values]

metric_types_enum_values = ["MEASURE_PROXY", "RATIO", "EXPR", "CUMULATIVE"]
metric_types_enum_values += [x.lower() for x in metric_types_enum_values]

mutability_type_values = ["IMMUTABLE", "APPEND_ONLY", "FULL_MUTATION", "DS_APPEND_ONLY"]
mutability_type_values += [x.lower() for x in mutability_type_values]

identifier_type_enum_values = ["PRIMARY", "UNIQUE", "FOREIGN", "RENDER_ONLY"]
identifier_type_enum_values += [x.lower() for x in identifier_type_enum_values]

aggregation_type_values = ["SUM", "MIN", "MAX", "AVERAGE", "COUNT_DISTINCT", "BOOLEAN", "SUM_BOOLEAN"]
aggregation_type_values += [x.lower() for x in aggregation_type_values]

time_granularity_values = ["DAY", "WEEK", "MONTH", "QUARTER", "YEAR"]
time_granularity_values += [x.lower() for x in time_granularity_values]

dimension_type_values = ["CATEGORICAL", "TIME"]
dimension_type_values += [x.lower() for x in dimension_type_values]

time_dimension_type_values = ["TIME", "time"]

tableau_params = {
    "$id": "materialization_params_tableau",
    "type": "object",
    "properties": {
        "projects": {
            "type": "array",
            "projects": {"type": "string"},
        },
    },
}

materialization_destination_schema = {
    "$id": "materialization_destination_schema",
    "type": "object",
    "properties": {
        "location": {"enum": materialization_location_values},
        "format": {"enum": materialization_format_values},
        "rollups": {
            "type": "array",
            "items": {
                "type": "array",
                "items": {"type": "string"},
            },
        },
        "tableau_params": tableau_params,
    },
    "anyOf": [{"not": {"$ref": "#/definitions/is-tableau"}}, {"required": ["tableau_params"]}],
    "definitions": {"is-tableau": {"properties": {"location": {"enum": materialization_location_tableau_values}}}},
    "additionalProperties": False,
}

metric_input_measure_schema = {
    "$id": "metric_input_measure_schema",
    "oneOf": [
        {"type": "string"},
        {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
            },
            "additionalProperties": False,
        },
    ],
}

metric_type_params_schema = {
    "$id": "metric_type_params",
    "type": "object",
    "properties": {
        "numerator": {"$ref": "metric_input_measure_schema"},
        "denominator": {"$ref": "metric_input_measure_schema"},
        "measure": {"$ref": "metric_input_measure_schema"},
        "measures": {
            "type": "array",
            "items": {"$ref": "metric_input_measure_schema"},
        },
        "expr": {"type": ["string", "boolean"]},
        "window": {"type": "string"},
        "grain_to_date": {"type": "string"},
    },
    "additionalProperties": False,
}

composite_sub_identifier_schema = {
    "$id": "composite_sub_identifier_schema",
    "type": "object",
    "properties": {
        "name": {"type": "string"},
        "expr": {"type": ["string", "boolean"]},
        "ref": {"type": "string"},
    },
    "additionalProperties": False,
}

identifier_schema = {
    "$id": "identifier_schema",
    "type": "object",
    "properties": {
        "name": {
            "type": "string",
            "pattern": TRANSFORM_OBJECT_NAME_PATTERN,
        },
        "type": {"enum": identifier_type_enum_values},
        "role": {"type": "string"},
        "expr": {"type": ["string", "boolean"]},
        "entity": {"type": "string"},
        "identifiers": {
            "type": "array",
            "items": {"$ref": "composite_sub_identifier_schema"},
        },
    },
    "additionalProperties": False,
    "required": ["name", "type"],
}

dimension_type_params_schema = {
    "$id": "dimension_type_params_schema",
    "type": "object",
    "properties": {
        "is_primary": {"type": "boolean"},
        "time_format": {"type": "string"},
        "time_granularity": {"enum": time_granularity_values},
    },
    "additionalProperties": False,
    "required": ["time_granularity"],
}

measure_schema = {
    "$id": "measure_schema",
    "type": "object",
    "properties": {
        "name": {
            "type": "string",
            "pattern": TRANSFORM_OBJECT_NAME_PATTERN,
        },
        "agg": {"enum": aggregation_type_values},
        "agg_time_dimension": {
            "type": "string",
            "pattern": TRANSFORM_OBJECT_NAME_PATTERN,
        },
        "expr": {"type": ["string", "integer", "boolean"]},
        "create_metric": {"type": "boolean"},
        "create_metric_display_name": {"type": "string"},
    },
    "additionalProperties": False,
    "required": ["name", "agg"],
}

dimension_schema = {
    "$id": "dimension_schema",
    "type": "object",
    "properties": {
        "name": {
            "type": "string",
            "pattern": TRANSFORM_OBJECT_NAME_PATTERN,
        },
        "type": {"enum": dimension_type_values},
        "is_partition": {"type": "boolean"},
        "expr": {"type": ["string", "boolean"]},
        "type_params": {"$ref": "dimension_type_params_schema"},
    },
    # dimension must have type_params if its a time dimension
    "anyOf": [{"not": {"$ref": "#/definitions/is-time-dimension"}}, {"required": ["type_params"]}],
    "definitions": {
        "is-time-dimension": {
            "properties": {"type": {"enum": time_dimension_type_values}},
            "required": ["type"],
        },
    },
    "additionalProperties": False,
    "required": ["name", "type"],
}

mutability_type_params_schema = {
    "$id": "mutability_type_params_schema",
    "type": "object",
    "properties": {
        "min": {"type": "string"},
        "max": {"type": "string"},
        "update_cron": {"type": "string"},
        "along": {"type": "string"},
    },
    "additionalProperties": False,
}

mutability_schema = {
    "$id": "mutability_schema",
    "type": "object",
    "properties": {
        "type": {"enum": mutability_type_values},
        "type_params": {"$ref": "mutability_type_params_schema"},
    },
    "additionalProperties": False,
    "required": ["type"],
}


# Top level object schemas
metric_schema = {
    "$id": "metric",
    "type": "object",
    "properties": {
        "name": {
            "type": "string",
            "pattern": TRANSFORM_OBJECT_NAME_PATTERN,
        },
        "type": {"enum": metric_types_enum_values},
        "type_params": {"$ref": "metric_type_params"},
        "constraint": {"type": "string"},
        "where_constraint": {"type": "string"},
    },
    "additionalProperties": False,
    "required": ["name", "type", "type_params"],
}

data_source_schema = {
    "$id": "data_source",
    "type": "object",
    "properties": {
        "name": {
            "type": "string",
            "pattern": TRANSFORM_OBJECT_NAME_PATTERN,
        },
        "sql_table": {"type": "string"},
        "sql_query": {"type": "string"},
        "dbt_model": {"type": "string"},
        "identifiers": {"type": "array", "items": {"$ref": "identifier_schema"}},
        "measures": {"type": "array", "items": {"$ref": "measure_schema"}},
        "dimensions": {"type": "array", "items": {"$ref": "dimension_schema"}},
        "mutability": {"$ref": "mutability_schema"},
        "constraint": {"$ref": "constraint_schema"},
    },
    "additionalProperties": False,
    "required": ["name"],
}

derived_group_by_element_schema = {
    "$id": "derived_group_by_element_schema",
    "type": "object",
    "properties": {
        "name": {
            "type": "string",
            "pattern": TRANSFORM_OBJECT_NAME_PATTERN,
        },
        "expr": {"type": ["string", "boolean"]},
        "expr_elements": {
            "type": "array",
            "items": {"type": "string"},
        },
    },
    "additionalProperties": False,
    "required": ["name", "expr"],
}

materialization_schema = {
    "$id": "materialization_schema",
    "type": "object",
    "properties": {
        "name": {
            "type": "string",
            "pattern": TRANSFORM_OBJECT_NAME_PATTERN,
        },
        "dimensions": {
            "type": "array",
            "items": {"type": "string"},
            "minItems": 1,
        },
        "metrics": {
            "type": "array",
            "items": {"type": "string"},
            "minItems": 1,
        },
        "destinations": {
            "type": "array",
            "items": {"$ref": "materialization_destination_schema"},
        },
        "destination_table": {"type": "string"},
    },
    "additionalProperties": False,
    "required": ["name", "dimensions", "metrics"],
}

schema_store = {
    # Top level schemas
    metric_schema["$id"]: metric_schema,
    data_source_schema["$id"]: data_source_schema,
    derived_group_by_element_schema["$id"]: derived_group_by_element_schema,
    materialization_schema["$id"]: materialization_schema,
    # Sub-object schemas
    metric_input_measure_schema["$id"]: metric_input_measure_schema,
    metric_type_params_schema["$id"]: metric_type_params_schema,
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
