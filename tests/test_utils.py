import pytest

from tap_s3.utils import *


def test_create_integer_property():
    assert create_integer_property() == {"type": ["null", "integer"]}


def test_create_number_property():
    assert create_number_property() == {"type": ["null", "number"]}


def test_create_boolean_property():
    assert create_boolean_property() == {"type": ["null", "boolean"]}


def test_create_string_property():
    assert create_string_property() == {"type": ["null", "string"]}


def create_object_property_test_inputs():
    return [
        (
            {"name": "Bob", "age": 100, "is_active": True},
            {
                "type": ["null", "object"],
                "properties": {
                    "name": {"type": ["null", "string"]},
                    "age": {"type": ["null", "integer"]},
                    "is_active": {"type": ["null", "boolean"]},
                },
            },
        ),
        (
            {"address": {"street": "123 Main", "state": "CO"}},
            {
                "type": ["null", "object"],
                "properties": {
                    "address": {
                        "type": ["null", "object"],
                        "properties": {
                            "street": {"type": ["null", "string"]},
                            "state": {"type": ["null", "string"]},
                        },
                    }
                },
            },
        ),
    ]


@pytest.mark.parametrize("obj,expected", create_object_property_test_inputs())
def test_create_object_property(obj, expected):
    assert create_object_property(obj) == expected


def create_array_property_test_inputs():
    return [
        (
            [1.0, 2.0],
            {
                "type": ["null", "array"],
                "items": {
                    "type": ["null", "number"],
                },
            },
        ),
        (
            [{"name": "Spot", "type": "Dog"}, {"name": "Kitty", "type": "Cat"}],
            {
                "type": ["null", "array"],
                "items": {
                    "type": ["null", "object"],
                    "properties": {
                        "name": {"type": ["null", "string"]},
                        "type": {"type": ["null", "string"]},
                    },
                },
            },
        ),
    ]


@pytest.mark.parametrize("items,expected", create_array_property_test_inputs())
def test_create_array_property(items, expected):
    assert create_array_property(items) == expected
