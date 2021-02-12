# tap-s3

This is a [Singer](https://singer.io) tap that reads data from files located inside a given S3 bucket and produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

## How to use it

`tap-s3` works together with any other [Singer Target](https://singer.io) to move data from s3 to any target destination.

### Install

First, make sure Python 3 is installed on your system or follow these
installation instructions for [Mac](http://docs.python-guide.org/en/latest/starting/install3/osx/) or
[Ubuntu](https://www.digitalocean.com/community/tutorials/how-to-install-python-3-and-set-up-a-local-programming-environment-on-ubuntu-16-04).

This project is set up using [Python Poetry](https://python-poetry.org/). Once cloned and within the project directory, install dependencies with:

```bash
poetry install
``

### Configuration

Here is an example of basic config, and a bit of a run down on each of the properties:

```json
{
    "aws_access_key_id": "key",
    "aws_secret_access_key": "secret",
    "start_date": "2020-08-21T00:00:00Z",
    "tables": {
        "people": {
            "bucket_name": "bucket_name",
            "search_prefix": "prefix",
            "search_pattern": "people",
            "file_type": "csv",
            "delimiter": "/",
            "tap_stream_id": "people",
            "primary_key": ["id"],
            "replication_method": "FULL_TABLE",
            "valid_replication_keys": [""],
            "replication_key": "",
            "object_type": "PERSON"
        },
        "houses": {
            "bucket_name": "bucket_name",
            "search_prefix": "prefix_1",
            "search_pattern": "houses",
            "file_type": "csv",
            "delimiter": "/",
            "tap_stream_id": "houses",
            "primary_key": ["id"],
            "replication_method": "INCREMENTAL",
            "valid_replication_keys": [""],
            "replication_key": "",
            "object_type": "HOUSE"
        }
    }
}
```

- **aws_access_key_id**: This is your AWS access key
- **aws_secret_access_key**: This is your AWS secret key.
- **start_date**: This is the datetime that the tap will use to look for newly updated or created files, based on the modified timestamp of the file.
- **tables**: A dictionary of how to find the tables you want to create from S3 **Currently only supports CSV format**.

```json
    {
        "bucket_name": "bucket_name",
        "search_prefix": "prefix_1",
        "search_pattern": "houses.csv",
        "delimiter": "/",
        "tap_stream_id": "houses",
        "primary_key": ["id"],
        "replication_method": "INCREMENTAL",
        "valid_replication_keys": [""],
        "replication_key": "",
        "object_type": "HOUSE"
    }
```

- **bucket_name**: This is the name of the bucket in which the objects reside.
- **search_prefix**: This is a prefix to apply after the bucket, but before the file search pattern, to allow you to find files in "directories" below the bucket.
- **search_pattern**: This is based on the key of the object you wish to retrieve.
- **file_type**: This is the file extention of the object you wish to read. **Currently only csv file extensions are supported**
- **delimiter**: This allows you to specify a custom delimiter, such as `\t` or `|`, if that applies to your files. **Currently not used.**
- **tap_stream_id**: The name of the tap stream and the name of the resulting table. This should be unique within this tap.
- **primary_key**: The primary key(s) for the stream.
- **replication_method**: Whether the stream should sync all records from `start_date` on every run, or whether the stream should only sync new/updated records. **Valid values: "INCREMENTAL", "FULL_TABLE"**

A sample configuration is available inside [config.sample.json](config.sample.json)

---

Copyright &copy; 2018 Stitch
