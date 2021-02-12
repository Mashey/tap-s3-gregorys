from singer import catalog
from tap_s3.discover import *
from tap_s3.sync import *
import pytest

@pytest.fixture
def s3_client(s3, bucket_1):
    yield s3

@pytest.fixture
def state():
    return {
        "bookmarks": {
            "people": {
                "last_modified": "2021-01-01T00:00:00Z"
            }
        }
    }

def test_sync(s3_client, config, state):
    catalog = discover(config)

    time_now_str = singer.utils.strftime(singer.utils.now(), format_str=singer.utils.DATETIME_PARSE)

    sync(config, state, catalog)
    bookmarks = state['bookmarks']
    assert 'houses' in bookmarks
    assert bookmarks['houses']['last_modified'] == time_now_str
    assert 'people' in bookmarks
    assert bookmarks['people']['last_modified'] == time_now_str
