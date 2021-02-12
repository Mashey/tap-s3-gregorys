import io
import pandas as pd
import numpy as np
import pytest

from tap_s3.utils import *

@pytest.fixture
def people_csv_data():
    return """id,name,age,cash,is_active,phone
    0,dan,10,11.0,true,5555451234
    1,ana,,50.4,false,555-545-1234
    2,,13,1.1,,5559994321
    """

@pytest.fixture
def records():
    return [
        {'id': 0, 'name': 'dan', 'age': 10, 'cash': 11.0, 'is_active': True, 'phone': '5555451234'},
        {'id': 1, 'name': 'ana', 'age': None, 'cash': 50.4, 'is_active': False, 'phone': '555-545-1234'},
        {'id': 2, 'name': None, 'age': 13, 'cash': 1.1, 'is_active': None, 'phone': '5559994321'}
    ]

@pytest.fixture
def raw_people_df(people_csv_data):
    yield pd.read_csv(
        io.StringIO(people_csv_data),
        index_col=None,
        dtype=str)

def test_infer():
    assert infer_type('10.0') == float
    assert infer_type('10') == int
    assert infer_type('1000004213') == int
    assert infer_type('Hello') == str
    assert infer_type('1Hello') == str
    assert infer_type('He11o') == str
    assert infer_type('') == str
    assert infer_type('true') == bool
    assert infer_type('false') == bool
    assert infer_type('True') == bool
    assert infer_type('False') == bool
    assert infer_type('tRue') == str
    assert infer_type('FalSE') == str

def test_clean_dataframe(raw_people_df, records):
    cleaned_df = clean_dataframe(raw_people_df)
    cleaned_records = cleaned_df.replace({np.nan:None}).to_dict('records')

    assert cleaned_records == records

def test_create_integer_property():
    assert create_integer_property() == { "type": ['null', 'integer'] }

def test_create_number_property():
    assert create_number_property() == { "type": ['null', 'number'] }

def test_create_boolean_property():
    assert create_boolean_property() == { "type": ['null', 'boolean'] }

def test_create_string_property():
    assert create_string_property() == { "type": ['null', 'string'] }

def test_create_none_property():
    assert create_none_property() == { "type": ['null'] }

def create_object_property_test_inputs():
    return [
        (
            {"name": "Bob", "age": 100, 'is_active': True},
            {
                "type": ["null", "object"],
                "properties": {
                    "name": {"type": ['null', 'string']},
                    "age": {"type": ['null', 'integer']},
                    "is_active": {"type": ['null', 'boolean']}
                }
            }
        ),
        (
            {"address": {"street": "123 Main", "state": "CO"}},
            {
                "type": ["null", "object"],
                "properties": {
                    "address": {
                        "type": ["null", "object"],
                        "properties": {
                            "street": {"type": ['null', 'string']},
                            "state": {"type": ['null', 'string']}
                        }
                    } 
                }
            }
        )
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
                }
            }
        ),
        (
            [{"name": "Spot", "type": "Dog"}, {"name": "Kitty", "type": "Cat"}],
            {
                "type": ["null", "array"],
                "items": {
                    "type": ["null", "object"],
                    "properties": {
                        "name": {"type": ['null', 'string']},
                        "type": {"type": ['null', 'string']}
                    }
                }
            }
        )
    ]

@pytest.mark.parametrize("items,expected", create_array_property_test_inputs())
def test_create_array_property(items, expected):
    assert create_array_property(items) == expected

