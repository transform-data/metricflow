import re
from jsonschema import ValidationError, _validators, _types
from jsonschema._utils import extras_msg, load_schema
from jsonschema.validators import create


def custom_find_additional_properties(instance, schema):
    """Return the set of additional properties for the given ``instance``.

    NOTE: This is a modified copy of the ``find_additional_properties`` method
    defined in jsonschema/_utils.py found here
    https://github.com/python-jsonschema/jsonschema/blob/72c3200d9b14139c9ab3094d0da4c0dd68acafe3/jsonschema/_utils.py#L87

    Weeds out properties that should have been validated by ``properties`` and
    / or ``patternProperties``. Additionally, completely ignores any properties
    matching ``^__(.+)__$``

    Assumes ``instance`` is dict-like already.
    """

    properties = schema.get("properties", {})
    schema_patterns = schema.get("patternProperties", {})
    # we ignore all things that match "^__(.+)__$" in all objects
    schema_patterns["^__(.+)__$"] = {}
    patterns = "|".join(schema_patterns)
    for property in instance:
        if property not in properties:
            if patterns and re.search(patterns, property):
                continue
            yield property


def customAdditionalProperties(validator, aP, instance, schema):
    """Validator for checking if a schema has additionalProperties when it shouldn't

    NOTE: This is a modified copy of the ``additionalProperties`` method
    defined in jsonschema/_validators.py found here
    https://github.com/python-jsonschema/jsonschema/blob/72c3200d9b14139c9ab3094d0da4c0dd68acafe3/jsonschema/_validators.py#L38
    """
    if not validator.is_type(instance, "object"):
        return

    extras = set(custom_find_additional_properties(instance, schema))

    if validator.is_type(aP, "object"):
        for extra in extras:
            for error in validator.descend(instance[extra], aP, path=extra):
                yield error
    elif not aP and extras:
        if "patternProperties" in schema:
            patterns = sorted(schema["patternProperties"])
            if len(extras) == 1:
                verb = "does"
            else:
                verb = "do"
            error = "%s %s not match any of the regexes: %s" % (
                ", ".join(map(repr, sorted(extras))),
                verb,
                ", ".join(map(repr, patterns)),
            )
            yield ValidationError(error)
        else:
            error = "Additional properties are not allowed (%s %s unexpected)"
            yield ValidationError(error % extras_msg(extras))


SchemaValidator = create(
    meta_schema=load_schema("draft7"),
    validators={
        "$ref": _validators.ref,
        "additionalItems": _validators.additionalItems,
        "additionalProperties": customAdditionalProperties,
        "allOf": _validators.allOf,
        "anyOf": _validators.anyOf,
        "const": _validators.const,
        "contains": _validators.contains,
        "dependencies": _validators.dependencies,
        "enum": _validators.enum,
        "exclusiveMaximum": _validators.exclusiveMaximum,
        "exclusiveMinimum": _validators.exclusiveMinimum,
        "format": _validators.format,
        "if": _validators.if_,
        "items": _validators.items,
        "maxItems": _validators.maxItems,
        "maxLength": _validators.maxLength,
        "maxProperties": _validators.maxProperties,
        "maximum": _validators.maximum,
        "minItems": _validators.minItems,
        "minLength": _validators.minLength,
        "minProperties": _validators.minProperties,
        "minimum": _validators.minimum,
        "multipleOf": _validators.multipleOf,
        "oneOf": _validators.oneOf,
        "not": _validators.not_,
        "pattern": _validators.pattern,
        "patternProperties": _validators.patternProperties,
        "properties": _validators.properties,
        "propertyNames": _validators.propertyNames,
        "required": _validators.required,
        "type": _validators.type,
        "uniqueItems": _validators.uniqueItems,
    },
    type_checker=_types.draft7_type_checker,
    version="draft7",
)
