from tap_s3.discover import *
import pytest


@pytest.fixture
def s3_client(s3, bucket_1):
    yield s3


def test_get_schemas(s3_client, config, schemas, schemas_metadata):
    ret_schemas, ret_schemas_metadata = get_schemas(config)
    assert ret_schemas == schemas
    assert ret_schemas_metadata == schemas_metadata


def test_discover(s3_client, config):
    catalog = discover(config)
    assert type(catalog) == Catalog