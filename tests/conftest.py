from _pytest.fixtures import pytest_fixture_setup
import boto3
from tap_s3.client import S3Client
import os
import pytest

from moto import mock_s3


@pytest.fixture
def aws_credentials():
    return {
        "key": "testing_key",
        "secret": "testing_secret",
    }

@pytest.fixture
def setup_aws_credentials(aws_credentials):
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = aws_credentials['key']
    os.environ["AWS_SECRET_ACCESS_KEY"] = aws_credentials['secret']


@pytest.fixture
def s3(setup_aws_credentials):
    with mock_s3():
        session = boto3.Session(region_name='us-east-1')
        yield session.resource('s3')


@pytest.fixture
def people_1_csv_data():
    return """id,name,age,cash,is_active,phone
    0,bob,50,10.0,true,5555451234
    1,jane,32,10.5,false,None
    """


@pytest.fixture
def people_2_csv_data():
    return """id,name,age,cash,is_active,phone
    3,bill,33,3.3,true,555-123-1223
    4,jill,35,7.0,false,555-321-2222
    """


@pytest.fixture
def people_3_csv_data():
    return """id,name,age,cash,is_active,phone
    5,dan,10,,true,1115455555
    6,ana,,151.0,false,5559874321
    """


@pytest.fixture
def people_schema():
    return {
        "type": ["null", "object"],
        "properties": {
            "id": {
                "type": ["null", "integer"]
            },
            "name": {
                "type": ["null", "string"]
            },
            "age": {
                "type": ["null", "integer"]
            },
            "cash": {
                "type": ["null", "number"]
            },
            'is_active': {
                "type": ["null", "boolean"]
            },
            'phone': {
                "type": ["null", "string"]
            }
        }
    }


@pytest.fixture
def houses_1_csv_data():
    return """id,person_id,state
    0,0,FL
    1,3,CO
    """


@pytest.fixture
def houses_2_csv_data():
    return """id,person_id,state
    3,0,CO
    4,1,IN
    """


@pytest.fixture
def houses_schema():
    return {
        "type": ["null", "object"],
        "properties": {
            "id": {
                "type": ["null", "integer"]
            },
            "person_id": {
                "type": ["null", "integer"]
            },
            "state": {
                "type": ["null", "string"]
            }
        }
    }


@pytest.fixture
def houses_schema_metadata():
    return {
        'houses': [
            {
                'breadcrumb': (),
                'metadata': {
                    'forced-replication-method': 'INCREMENTAL',
                    'inclusion': 'available',
                    'table-key-properties': ['id'],
                    'valid-replication-keys': ['']
                }
            },
            {
                'breadcrumb': ('properties', 'id'),
                'metadata': {'inclusion': 'automatic'}},
            {
                'breadcrumb': ('properties', 'person_id'),
                'metadata': {'inclusion': 'available'}},
            {
                'breadcrumb': ('properties', 'state'),
                'metadata': {'inclusion': 'available'}
            }
        ]
    }


@pytest.fixture
def people_schema_metadata():
    return {
        'people': [
            {
                'breadcrumb': (),
                'metadata': {
                    'forced-replication-method': 'FULL_TABLE',
                    'inclusion': 'available',
                    'table-key-properties': ['id']
                }
            },
            {
                'breadcrumb': ('properties', 'id'),
                'metadata': {'inclusion': 'automatic'}
            },
            {
                'breadcrumb': ('properties', 'name'),
                'metadata': {'inclusion': 'available'}
            },
            {
                'breadcrumb': ('properties', 'age'),
                'metadata': {'inclusion': 'available'}
            },
            {
                'breadcrumb': ('properties', 'cash'),
                'metadata': {'inclusion': 'available'}
            },
            {
                'breadcrumb': ('properties', 'is_active'),
                'metadata': {'inclusion': 'available'}
            },
            {
                'breadcrumb': ('properties', 'phone'),
                'metadata': {'inclusion': 'available'}
            }
        ]
    }


@pytest.fixture
def schemas_metadata(people_schema_metadata, houses_schema_metadata):
    return { **people_schema_metadata, **houses_schema_metadata }


@pytest.fixture
def schemas(people_schema, houses_schema):
    return { 
        'people': people_schema, 
        'houses': houses_schema 
    }



@pytest.fixture
def bucket_1_name():
    return 'test_bucket_1'


@pytest.fixture
def bucket_1(s3, bucket_1_name, people_1_csv_data, people_2_csv_data, people_3_csv_data, houses_1_csv_data, houses_2_csv_data):
    bucket = s3.create_bucket(Bucket=bucket_1_name)
    bucket.put_object(Body=people_1_csv_data, Key='prefix_1/20201123/people.csv')
    bucket.put_object(Body=people_2_csv_data, Key='prefix_1/20201124/people.csv')
    bucket.put_object(Body=people_3_csv_data, Key='prefix_1/20201125/people.csv')
    bucket.put_object(Body=houses_1_csv_data, Key='prefix_1/20201123/houses.csv')
    bucket.put_object(Body=houses_2_csv_data, Key='prefix_1/20201124/houses.csv')
    yield


@pytest.fixture
def houses_table_config(bucket_1_name):
    return {
        "houses": {
            "bucket_name": bucket_1_name,
            "search_prefix": "prefix_1",
            "search_pattern": "houses",
            "file_type": 'csv',
            "delimiter": "/",
            "tap_stream_id": "houses",
            "primary_key": ["id"],
            "replication_method": "INCREMENTAL",
            "valid_replication_keys": [""],
            "replication_key": "",
            "object_type": "HOUSE"
        }
    }


@pytest.fixture
def people_table_config(bucket_1_name):
    return {
        "people": {
            "bucket_name": bucket_1_name,
            "search_prefix": "prefix_1",
            "search_pattern": "people",
            "file_type": 'csv',
            "delimiter": "/",
            "tap_stream_id": "people",
            "primary_key": ["id"],
            "replication_method": "FULL_TABLE",
            "valid_replication_keys": [],
            "replication_key": "",
            "object_type": "PERSON"
        }
    }


@pytest.fixture
def table_configs(people_table_config, houses_table_config):
    return { **people_table_config, **houses_table_config }


@pytest.fixture
def config(aws_credentials, table_configs):
    yield {
        "aws_access_key_id": aws_credentials['key'],
        "aws_secret_access_key": aws_credentials['secret'],
        "start_date": "2020-08-21T00:00:00Z",
        "tables": table_configs
    }


@pytest.fixture()
def client(aws_credentials):
    yield S3Client(aws_credentials['key'], aws_credentials['secret'])