import sys
import io
import argparse
import ujson
from collections import defaultdict
from joblib import Parallel, delayed, parallel_backend
from typing import Dict, List, Any

from target_postgres.postgres_sink import PostgresSink
from target_postgres.utils import get_logger, test_postgres_connection


LOGGER = get_logger("target_postgres")
DEFAULT_PARALLELISM = 0  # 0 The number of threads used to flush tables
DEFAULT_MAX_PARALLELISM = 16  # Don't use more than this number of threads by default when flushing streams in parallel


def validate_config(config):
    LOGGER.info(f"Validating configuration")
    errors = []
    required_config_keys = [
        "host",
        "port",
        "user",
        "password",
        "dbname"
    ]

    # Check if mandatory keys exist
    for k in required_config_keys:
        if not config.get(k, None):
            errors.append("Required key is missing from config: [{}]".format(k))

    # Check target schema config
    config_default_target_schema = config.get("default_target_schema", None)
    config_schema_mapping = config.get("schema_mapping", None)
    if not config_default_target_schema and not config_schema_mapping:
        errors.append("Neither 'default_target_schema' (string) nor 'schema_mapping' (object) keys set in config.")

    if errors:
        errors_str = "\n   * ".join(errors)
        raise Exception(f"Invalid configuration:\n   * {errors_str}")


def flush_stream(sink_list: List[PostgresSink]) -> None:
    number_of_sinks = len(sink_list)
    for index, sink in enumerate(sink_list):
        LOGGER.info(f"Flushing sink {index + 1} of {number_of_sinks}")
        # only create indices for the stream if it's the last sink
        create_indices = index == number_of_sinks - 1
        sink.sync_table()
        sink.flush_csv_to_db(create_indices=create_indices)


def flush_streams(streams_dict: Dict[str, List[PostgresSink]], config: Dict[str, Any]) -> None:
    # first sync all schemas synchronously
    for sink_list in streams_dict.values():
        for sink in sink_list:
            sink.sync_schema()

    parallelism = config.get("parallelism", DEFAULT_PARALLELISM)
    max_parallelism = config.get("max_parallelism", DEFAULT_MAX_PARALLELISM)

    # Parallelism 0 means auto parallelism:
    #
    # Auto parallelism trying to flush streams efficiently with auto defined number
    # of threads where the number of threads is the number of streams that need to
    # be loaded but it's not greater than the value of max_parallelism
    if parallelism == 0:
        n_streams_to_flush = len(streams_dict.keys())
        if n_streams_to_flush > max_parallelism:
            parallelism = max_parallelism
        else:
            parallelism = n_streams_to_flush

    with parallel_backend('threading', n_jobs=parallelism):
        Parallel()(delayed(flush_stream)(
            sink_list
        ) for sink_list in streams_dict.values())


def process_singer_messages(config, singer_messages):
    """Process singer messages"""
    LOGGER.info(f"Processing singer messages")

    streams_to_sync = defaultdict[str, List[PostgresSink]](list)
    
    for line in singer_messages:
        try:
            singer_message = ujson.loads(line)
        except ujson.JSONDecodeError:
            LOGGER.error("Unable to parse:\n%s", line)
            raise

        if "type" not in singer_message:
            raise Exception("Singer message is missing required key 'type': {}".format(line))
        message_type = singer_message["type"]

        if message_type == "RECORD":
            if "stream" not in singer_message:
                raise Exception("Singer message is missing required key 'stream': {}".format(line))
            
            if "record" not in singer_message:
                raise Exception("Singer message is missing required key 'record': {}".format(line))

            if singer_message["stream"] not in streams_to_sync:
                raise Exception("A record for stream '{}' was encountered before a corresponding schema message".format(singer_message["stream"]))

            stream_name = singer_message["stream"]
            # get last sink for the stream
            stream_sink = streams_to_sync[stream_name][-1]
            stream_sink.process_record_message(singer_message["record"])

        elif message_type == "STATE":
            # We don't implement state management because we flush all data at once
            LOGGER.debug('Singer message: STATE not implemented - {singer_message}')

        elif message_type == "SCHEMA":
            if "stream" not in singer_message:
                raise Exception("Singer message is missing required key 'stream': {}".format(line))
            stream = singer_message["stream"]

            if "key_properties" not in singer_message:
                raise Exception(f"'key_properties' key missing - {singer_message}")

            if "schema" not in singer_message:
                raise Exception(f"'schema' key missing - {singer_message}")

            if "properties" not in singer_message["schema"]:
                raise Exception(f"'schema.properties' key missing - {singer_message}")

            if config.get("primary_key_required", False) and len(singer_message["key_properties"]) == 0:
                error = f"Primary key is set to mandatory but not defined for the '{stream}' stream"
                LOGGER.critical(error)
                raise Exception(error)

            if not config.get("insertion_method_tables"):
                config["insertion_method_tables"] = []
    
            if stream in streams_to_sync:
                if streams_to_sync[stream][-1].stream_schema_message == singer_message:
                    LOGGER.info(f"Schema message already processed for stream {stream}")
                    continue

            streams_to_sync[stream].append(PostgresSink(config, singer_message))

        elif message_type == "ACTIVATE_VERSION":
            LOGGER.debug(f"Singer message: ACTIVATE_VERSION not implemented - {singer_message}")

        else:
            raise Exception("Unknown message type '{}' in message {}"
                            .format(singer_message["type"], singer_message))

    LOGGER.info(f"Finished processing singer messages")
    LOGGER.info(f"Starting to sync data with DB")

    flush_streams(streams_to_sync, config)

    LOGGER.info(f"Finished syncing data with DB")


def main():
    """Main entry point"""
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("-c", "--config", help="Config file")
    args = arg_parser.parse_args()

    if args.config:
        with open(args.config) as config_input:
            config = ujson.load(config_input)
    else:
        config = {}

    validate_config(config)
    test_postgres_connection(config)

    # Consume singer messages
    singer_messages = io.TextIOWrapper(sys.stdin.buffer, encoding="utf-8")
    process_singer_messages(config, singer_messages)

    LOGGER.debug("Exiting normally")


if __name__ == "__main__":
    main()
