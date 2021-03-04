import numpy as np
import os


def property_types():
    return {
        int: create_integer_property,
        np.int64: create_integer_property,
        float: create_number_property,
        np.float64: create_number_property,
        str: create_string_property,
        bool: create_boolean_property,
        np.bool: create_boolean_property,
        dict: create_object_property,
        list: create_array_property,
    }


def create_integer_property(value=None):
    return {"type": ["null", "integer"]}


def create_number_property(value=None):
    return {"type": ["null", "number"]}


def create_boolean_property(value=None):
    return {"type": ["null", "boolean"]}


def create_string_property(value=None):
    return {"type": ["null", "string"]}


def create_object_property(obj={}):
    instance = {"type": ["null", "object"]}
    properties = {}
    for key, value in obj.items():
        value_type = property_types().get(type(value), create_string_property)(value)
        key_property = {key: value_type}
        properties.update(key_property)
    instance["properties"] = properties
    return instance


def create_array_property(items=[]):
    instance = {"type": ["null", "array"]}
    item = next(iter(items))
    instance["items"] = property_types().get(type(item), create_string_property)(item)
    return instance


def create_json_schema(object):
    return create_object_property(object)


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)
