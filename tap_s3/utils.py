import numpy as np
import pandas as pd
import os

BOOLEAN_VALUES = {
    'true': True,
    'True': True,
    'false': False,
    'False': False
}

def infer_type(data):
    try:
        int(data)
        return int
    except (ValueError, TypeError):
        pass
    try:
        float(data)
        return float
    except (ValueError, TypeError):
        pass
    if data in BOOLEAN_VALUES:
        return bool
    else:
        return str


def clean_dataframe(df):
    for column, column_vals in df.iteritems():
        inferred_type = str
        valid_value_index = column_vals.first_valid_index()
        if valid_value_index is not None:
            raw_value = column_vals[valid_value_index]
            inferred_type = infer_type(raw_value)
        df = convert_data(df, column, inferred_type)
    return df


def convert_data(df, column, type):
    if type == int:
        try:
            casted_vals = []
            for val in df[column]:
                if pd.notna(val):
                    casted_vals.append(int(val))
                else:
                    casted_vals.append(val)
            df[column] = pd.Series(casted_vals, dtype=pd.Int64Dtype())
        except:
            try:
                df = df.astype({column: float})
            except:
                pass       
    elif type == float:
        try:
            df = df.astype({column: float})
        except:
            pass
    elif type == bool:
        df[column] = df[column].map(BOOLEAN_VALUES)
    else:
        return df
    return df


def property_types():
    return {
        int: create_integer_property,
        np.int64: create_integer_property,
        float: create_number_property,
        np.float64: create_number_property,
        str: create_string_property,
        bool: create_boolean_property,
        np.bool_: create_boolean_property,
        dict: create_object_property,
        list: create_array_property
    }

def create_integer_property(value=None):
    return { "type": ['null', 'integer'] }


def create_number_property(value=None):
    return { "type": ['null', 'number'] }


def create_boolean_property(value=None):
    return { "type": ['null', 'boolean'] }


def create_string_property(value=None):
    return { "type": ['null', 'string'] }


def create_none_property(value=None):
    return { "type": ['null'] }


def create_object_property(obj={}):
    instance = { "type": ['null', 'object'] }
    properties = {}
    for key, value in obj.items():
        value_type = property_types().get(
                type(value),
                create_string_property)(value)
        key_property = {
            key: value_type
        }
        properties.update(key_property)
    instance['properties'] = properties
    return instance


def create_array_property(items=[]):
    instance = {
        "type": ['null', 'array']
    }
    item = next(iter(items))
    instance['items'] = property_types().get(
            type(item),
            create_string_property)(item)
    return instance


def create_json_schema(object):
    return create_object_property(object)


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

