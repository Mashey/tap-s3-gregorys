import singer
from singer import Transformer, metadata
from .client import S3Client
from .streams import Stream

LOGGER = singer.get_logger()

def sync(config, state, catalog):
    client = S3Client(config['aws_access_key_id'], config['aws_secret_access_key'])

    with Transformer() as transformer:
        for stream in catalog.streams:
            tap_stream_id   = stream.tap_stream_id
            table_spec      = config['tables'][tap_stream_id]
            stream_obj      = Stream(client, table_spec, state)
            replication_key = stream_obj.replication_key
            stream_schema   = stream.schema.to_dict()
            stream_metadata = metadata.to_map(stream.metadata)

            LOGGER.info('Staring sync for stream: %s', tap_stream_id)

            state = singer.set_currently_syncing(state, tap_stream_id)
            singer.write_state(state)

            singer.write_schema(
                tap_stream_id,
                stream_schema,
                stream_obj.key_properties,
                stream.replication_key
            )

            last_modified = singer.utils.strptime_with_tz(
                singer.get_bookmark(
                    state,
                    tap_stream_id,
                    'last_modified',
                    config['start_date']))

            for record in stream_obj.sync(last_modified):
                LOGGER.debug(f'Attempting to write {record}')
                transformed_record = transformer.transform(record, stream_schema, stream_metadata)
                LOGGER.info(f"Writing record: {transformed_record}")
                singer.write_record(
                    tap_stream_id,
                    transformed_record,
                )

            singer.write_bookmark(
                state,
                tap_stream_id,
                'last_modified',
                singer.utils.strftime(singer.utils.now(), format_str=singer.utils.DATETIME_PARSE))
            singer.write_state(state)

    state = singer.set_currently_syncing(state, None)
    singer.write_state(state)