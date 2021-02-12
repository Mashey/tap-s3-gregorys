from .client import S3Client

import json

from singer import metadata, get_logger
from singer.catalog import Catalog

from .streams import Stream

LOGGER = get_logger()

def get_schemas(config):
    schemas = {}
    schemas_metadata = {}
    client = S3Client(config['aws_access_key_id'], config['aws_secret_access_key'])

    for tap_stream_id, table_spec in config['tables'].items():
        LOGGER.info(f'Starting discovery for {tap_stream_id}')
        stream_object = Stream(client, table_spec, None)
        stream_schema = stream_object.get_schema()

        meta = metadata.get_standard_metadata(
            schema=stream_schema,
            key_properties=stream_object.key_properties,
            replication_method=stream_object.replication_method
        )

        meta = metadata.to_map(meta)

        if stream_object.valid_replication_keys:
            meta = metadata.write(meta, (), 'valid-replication-keys', stream_object.valid_replication_keys)
        if stream_object.replication_key:
            meta = metadata.write(meta, ('properties', stream_object.replication_key), 'inclusion', 'automatic')

        meta = metadata.to_list(meta)

        schemas[tap_stream_id] = stream_schema
        schemas_metadata[tap_stream_id] = meta

    return schemas, schemas_metadata

def discover(config):
    schemas, schemas_metadata = get_schemas(config)
    streams = []

    for schema_name, schema in schemas.items():
        schema_meta = schemas_metadata[schema_name]

        catalog_entry = {
            'stream': schema_name,
            'tap_stream_id': schema_name,
            'schema': schema,
            'metadata': schema_meta
        }

        streams.append(catalog_entry)

    return Catalog.from_dict({'streams': streams})