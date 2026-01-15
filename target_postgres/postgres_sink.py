import os
import ujson
import uuid
import psycopg2.extras
import time
import re
import functools
import inflection

from typing import Tuple
from tempfile import mkstemp
import fastjsonschema

from target_postgres.utils import get_logger, float_to_decimal, open_postgres_connection


LOGGER = get_logger("target_postgres")


class RecordValidationException(Exception):
    """Exception to raise when record validation failed"""


class InvalidValidationOperationException(Exception):
    """Exception to raise when internal JSON schema validation process failed"""


class TransformStream:
    def __init__(self, temp_file_handler):
        self.temp_file_handler = temp_file_handler

    def _readline(self):
        for row in self.temp_file_handler:
            # data for ducplicated records is replaced with '\n'
            # so we wanna skip that and don't send unnecessary data to postgres
            if row == b'\n':
                continue
            yield row
        yield ''

    def read(self, *args, **kwargs):
        return next(self._readline())


def column_type(schema_property):
    property_type = schema_property['type']
    property_format = schema_property['format'] if 'format' in schema_property else None
    col_type = 'character varying'
    if 'object' in property_type or 'array' in property_type:
        col_type = 'jsonb'

    # Every date-time JSON value is currently mapped to TIMESTAMP WITHOUT TIME ZONE
    #
    # TODO: Detect if timezone postfix exists in the JSON and find if TIMESTAMP WITHOUT TIME ZONE or
    # TIMESTAMP WITH TIME ZONE is the better column type
    elif property_format == 'date-time':
        col_type = 'timestamp without time zone'
    elif property_format == 'time':
        col_type = 'time without time zone'
    elif 'number' in property_type:
        col_type = 'double precision'
    elif 'integer' in property_type and 'string' in property_type:
        col_type = 'character varying'
    elif 'integer' in property_type:
        if 'maximum' in schema_property:
            if schema_property['maximum'] <= 32767:
                col_type = 'smallint'
            elif schema_property['maximum'] <= 2147483647:
                col_type = 'integer'
            elif schema_property['maximum'] <= 9223372036854775807:
                col_type = 'bigint'
        else:
            col_type = 'numeric'
    elif 'boolean' in property_type:
        col_type = 'boolean'

    LOGGER.debug("schema_property: %s -> col_type: %s", schema_property, col_type)

    return col_type


@functools.cache
def safe_column_name(name):
    if len(name) >= 63:
        name = re.sub(r'[a-z]', '', inflection.camelize(name))
    return '"{}"'.format(name).lower()


def column_clause(name, schema_property):
    return '{} {}'.format(safe_column_name(name), column_type(schema_property))


def primary_column_names(stream_schema_message):
    return [safe_column_name(p) for p in stream_schema_message['key_properties']]


def get_jsonb_columns(stream_schema_message):
    columns = [
        name
        for (name, schema) in stream_schema_message if column_type(schema) == "jsonb"
    ]
    return columns


class PostgresSink:
    def __init__(self, config, stream_schema_message):
        LOGGER.info(f"Initializing PostgresSink for stream {stream_schema_message['stream']}")
        self.row_count = 0
        self.config = config
        self.stream_schema_message = stream_schema_message
        self.key_properties = self.stream_schema_message.get("key_properties", [])
        self.jsonb_columns = get_jsonb_columns(self.stream_schema_message["schema"]["properties"].items())

        self.validator = None
        self.should_validate_records = config.get('validate_records', False)

        if self.should_validate_records:
            try:
                self.validator = fastjsonschema.compile(self.stream_schema_message["schema"])
            except Exception as ex:
                LOGGER.error(f"Invalid JSON schema for stream {stream_name}: {ex}")
                raise

        self.schema_name = None
        self.grantees = None
        self.indices = []

        # store record start/end position in csv file for deduplication
        # key: record primary key, value: set of start/end positions in csv file
        self.existing_record_position_lookup = dict[str, Tuple[int, int]]()
        

        #  Define target schema name.
        #  --------------------------
        #  Target schema name can be defined in multiple ways:
        #
        #   1: 'default_target_schema' key  : Target schema is the same for every incoming stream if
        #       not specified explicitly for a given stream in the `schema_mapping` object
        #   2: 'schema_mapping' key : Target schema defined explicitly for a given stream.
        #       Example config.json:
        #           "schema_mapping": {
        #               "my_tap_stream_id": {
        #                   "target_schema": "my_postgres_schema",
        #                   "target_schema_select_permissions": [ "role_with_select_privs" ],
        #                   "indices": {
        #                       "table_one": ["column_1", "column_2"],
        #                       "table_two": ["column_3", "column_4"]
        #                   }
        #               }
        #           }
        config_default_target_schema = (self.config.get('default_target_schema', '') or '').strip()
        config_schema_mapping = self.config.get('schema_mapping', {})

        stream_name = stream_schema_message['stream']
        stream_schema_name = self.stream_name_to_dict(stream_name)['schema_name']
        stream_table_name = self.stream_name_to_dict(stream_name)['table_name']
        if config_schema_mapping and stream_schema_name in config_schema_mapping:
            self.schema_name = config_schema_mapping[stream_schema_name].get('target_schema')

            # Get indices to create for the target table
            indices = config_schema_mapping[stream_schema_name].get('indices', {})
            if stream_table_name in indices:
                self.indices.extend(indices.get(stream_table_name, []))

        elif config_default_target_schema:
            self.schema_name = config_default_target_schema

        if not self.schema_name:
            raise Exception("Target schema name not defined in config. Neither 'default_target_schema' (string)"
                            "nor 'schema_mapping' (object) defines target schema for '{}' stream."
                            .format(stream_name))

        #  Define grantees
        #  ---------------
        #  Grantees can be defined in multiple ways:
        #
        #   1: 'default_target_schema_select_permissions' key  : USAGE and SELECT privileges will be granted on
        #       every table to a given role for every incoming stream if not specified explicitly in the
        #       `schema_mapping` object
        #   2: 'target_schema_select_permissions' key : Roles to grant USAGE and SELECT privileges defined
        #       explicitly for a given stream.
        #           Example config.json:
        #               "schema_mapping": {
        #                   "my_tap_stream_id": {
        #                       "target_schema": "my_postgres_schema",
        #                       "target_schema_select_permissions": [ "role_with_select_privs" ],
        #                       "indices": {
        #                           "table_one": ["column_1", "column_2"],
        #                           "table_two": ["column_3", "column_4"]
        #                       }
        #                   }
        #               }
        self.grantees = self.config.get('default_target_schema_select_permissions')
        if config_schema_mapping and stream_schema_name in config_schema_mapping:
            self.grantees = config_schema_mapping[stream_schema_name].get('target_schema_select_permissions',
                                                                              self.grantees)

        self.temp_file_name, self.temp_file_handler = self.initialize_csv_file(stream_name, config.get('temp_dir'))
        LOGGER.info(f"Done initializing PostgresSink for stream {stream_schema_message['stream']}")


    def __del__(self):
        self.close_csv_file()


    def close_csv_file(self):
        if hasattr(self, 'temp_file_handler') and self.temp_file_handler:
            self.temp_file_handler.close()
        
        if hasattr(self, 'temp_file_name') and self.temp_file_name and os.path.exists(self.temp_file_name):
            LOGGER.info(f"Cleaning up temp file {self.temp_file_name}")
            os.remove(self.temp_file_name)


    def initialize_csv_file(self, stream_name, temp_dir):
        if temp_dir:
            temp_dir = os.path.expanduser(temp_dir)
            os.makedirs(temp_dir, exist_ok=True)

        csv_fd, csv_file = mkstemp(suffix='.csv', prefix=f'{stream_name}_', dir=temp_dir)
        file_handler = open(csv_fd, 'w+b')
        return csv_file, file_handler


    def get_table_name(self, stream_name, is_temporary=False, without_schema=False):
        stream_dict = self.stream_name_to_dict(stream_name)
        table_name = stream_dict['table_name']
        pg_table_name = table_name.replace('.', '_').replace('-', '_').lower()

        if is_temporary:
            return 'tmp_{}'.format(str(uuid.uuid4()).replace('-', '_'))

        if without_schema:
            return f'"{pg_table_name.lower()}"'

        return f'{self.schema_name}."{pg_table_name.lower()}"'


    def query(self, connection, query, params=None):
        LOGGER.debug("Running query: %s", query)
        with connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(
                query,
                params
            )

            if cur.rowcount > 0:
                return cur.fetchall()

            return []


    def sync_schema(self):
        stream_name = self.stream_schema_message['stream']
        LOGGER.info(f"Syncing schema for stream {stream_name}")
        with open_postgres_connection(self.config) as conn:
            self.create_schema_if_not_exists(conn)
        LOGGER.info(f"Done syncing schema for stream {stream_name}")


    def sync_table(self):
        stream_name = self.stream_schema_message['stream']
        LOGGER.info(f"Syncing table for stream {stream_name}")
        with open_postgres_connection(self.config) as conn:
            self._sync_table(conn)
        LOGGER.info(f"Done syncing table for stream {stream_name}")


    def create_schema_if_not_exists(self, conn):
        schema_name = self.schema_name
        schema_rows = 0
        
        schema_rows = self.query(
            conn,
            'SELECT LOWER(schema_name) schema_name FROM information_schema.schemata WHERE LOWER(schema_name) = %s',
            (schema_name.lower(),)
        )

        if len(schema_rows) == 0:
            query = "CREATE SCHEMA IF NOT EXISTS {}".format(schema_name)
            LOGGER.info("Schema '%s' does not exist. Creating... %s", schema_name, query)
            self.query(conn, query)

            self.grant_privilege(conn, schema_name, self.grantees, self.grant_usage_on_schema)


    def _sync_table(self, conn):
        stream_schema_message = self.stream_schema_message
        stream = stream_schema_message['stream']
        table_name = self.get_table_name(stream, without_schema=True)
        insertion_method = self.config.get("insertion_method")
        insertion_method_tables = self.config.get("insertion_method_tables")
        ## drops table if insertion_method is "truncate"
        if insertion_method and insertion_method == "truncate":
            if len(insertion_method_tables) == 0: #if no insertion_method_tables specified, remove all tables and repopulate
                self.query(conn, self.drop_table_query())
            elif table_name[1:-1] in insertion_method_tables: #and if table name in insertion_method_tables
                self.query(conn, self.drop_table_query())
        
        found_tables = [table for table in (self.get_tables(conn)) if f'"{table["table_name"].lower()}"' == table_name]
        if len(found_tables) == 0:
            query = self.create_table_query()
            LOGGER.info("Table '%s' does not exist. Creating... %s", table_name, query)
            self.query(conn, query)

            self.grant_privilege(conn, f"{self.schema_name}.{table_name}", self.grantees, self.grant_select_on_table)
        else:
            LOGGER.info("Table '%s' exists", table_name)
            # Log existing column types before updating
            columns = self.get_table_columns(conn, table_name)
            LOGGER.info("Existing column types in table '%s': %s", table_name, 
                            ujson.dumps({col['column_name']: col['data_type'] for col in columns}))
            self.update_columns(conn)


    def create_table_query(self, table_name=None, is_temporary=False):
        stream_schema_message = self.stream_schema_message
        columns = [
            column_clause(
                name,
                schema
            )
            for (name, schema) in self.stream_schema_message["schema"]["properties"].items()
        ]

        primary_key = ["PRIMARY KEY ({})".format(', '.join(primary_column_names(stream_schema_message)))] \
            if len(stream_schema_message['key_properties']) > 0 else []

        if not table_name:
            gen_table_name = self.get_table_name(stream_schema_message['stream'], is_temporary=is_temporary)

        return 'CREATE {}TABLE IF NOT EXISTS {} ({})'.format(
            'TEMP ' if is_temporary else '',
            table_name if table_name else gen_table_name,
            ', '.join(columns + primary_key)
        )


    def get_tables(self, conn):
        return self.query(
            conn,
            'SELECT table_name FROM information_schema.tables WHERE table_schema = %s',
            (self.schema_name,)
        )


    def get_table_columns(self, conn, table_name):
        return self.query(conn, """SELECT column_name, data_type
            FROM information_schema.columns
            WHERE lower(table_name) = %s AND lower(table_schema) = %s""", (table_name.replace("\"", "").lower(),
                                                                            self.schema_name.lower(),))


    def update_columns(self, conn):
        stream_schema_message = self.stream_schema_message
        stream = stream_schema_message['stream']
        table_name = self.get_table_name(stream, without_schema=True)
        columns = self.get_table_columns(conn, table_name)
        columns_dict = {column['column_name'].lower(): column for column in columns}

        columns_to_add = [
            column_clause(
                name,
                properties_schema
            )
            for (name, properties_schema) in self.stream_schema_message["schema"]["properties"].items()
            if name.lower() not in columns_dict
        ]

        for column in columns_to_add:
            self.add_column(conn, column, stream)

        columns_to_replace = [
            (safe_column_name(name), column_clause(
                name,
                properties_schema
            ))
            for (name, properties_schema) in self.stream_schema_message["schema"]["properties"].items()
            if name.lower() in columns_dict and
            columns_dict[name.lower()]['data_type'].lower() != column_type(properties_schema).lower()
        ]

        for (column_name, column) in columns_to_replace:
            # NOTE: We only do this if versioning is turned on, otherwise we can safely ignore.
            if self.config.get('is_versioning_enabled', False):
                self.version_column(column_name, stream)
                self.add_column(conn, column, stream)


    def add_column(self, conn, column, stream):
        add_column = "ALTER TABLE {} ADD COLUMN IF NOT EXISTS {}".format(self.get_table_name(stream), column)
        LOGGER.info('Adding column: %s', add_column)
        self.query(conn, add_column)


    def drop_table_query(self):
        stream_schema_message = self.stream_schema_message
        table = self.get_table_name(stream_schema_message['stream'])
        return "DROP TABLE IF EXISTS {}".format(table)


    def version_column(self, conn, column_name, stream):
        version_column = "ALTER TABLE {} RENAME COLUMN {} TO \"{}_{}\"".format(self.get_table_name(stream, False),
                                                                               column_name,
                                                                               column_name.replace("\"", ""),
                                                                               time.strftime("%Y%m%d_%H%M"))
        LOGGER.info('Versioning column: %s', version_column)
        self.query(conn, version_column)


    def grant_usage_on_schema(self, conn, schema_name, grantee):
        query = "GRANT USAGE ON SCHEMA {} TO GROUP {}".format(schema_name, grantee)
        LOGGER.info("Granting USAGE privilege on '%s' schema to '%s'... %s", schema_name, grantee, query)
        self.query(conn, query)


    def grant_select_on_table(self, conn, table_name, grantee):
        query = "GRANT SELECT ON TABLE {} TO GROUP {}".format(table_name, grantee)
        LOGGER.info("Granting SELECT ON TABLE privilege on '%s' to '%s'... %s", table_name, grantee, query)
        self.query(conn, query)


    @classmethod
    def grant_privilege(cls, conn, schema, grantees, grant_method):
        if isinstance(grantees, list):
            for grantee in grantees:
                grant_method(conn, schema, grantee)
        elif isinstance(grantees, str):
            grant_method(conn, schema, grantees)


    def process_record_message(self, record):
        # Validate record
        if self.should_validate_records:
            try:
                self.validator(float_to_decimal(record))
            except Exception as ex:
                if type(ex).__name__ == "InvalidOperation":
                    raise InvalidValidationOperationException(
                        f"Data validation failed and cannot load to destination. RECORD: {record}\n"
                        "multipleOf validations that allows long precisions are not supported (i.e. with 15 digits"
                        "or more) Try removing 'multipleOf' methods from JSON schema.") from ex
                raise RecordValidationException(
                    f"Record does not pass schema validation. RECORD: {record}") from ex

        for jsonb_column in self.jsonb_columns:
            if jsonb_column in record and record[jsonb_column]:
                record[jsonb_column] = ujson.dumps(record[jsonb_column])

        self.add_record_to_csv(record)


    def record_primary_key_string(self, record):
        if len(self.key_properties) == 0:
            return None

        absent_pks = []
        for p in self.key_properties:
            if p not in record:
                absent_pks.append(p)
        if len(absent_pks) > 0:
            raise Exception("Primary key(s) are not defined in the record: {}".format(', '.join(absent_pks)))
        key_props = [str(record[p]) for p in self.key_properties]
        return ','.join(key_props)


    def record_to_csv_line(self, record):
        return ','.join(
            [
                ujson.dumps(record[name], ensure_ascii=False, escape_forward_slashes=False)
                if name in record and (record[name] == 0 or record[name]) else ''
                for name in self.stream_schema_message["schema"]["properties"]
            ]
        )


    def add_record_to_csv(self, record):
        primary_key_string = self.record_primary_key_string(record)
        current_position = self.temp_file_handler.tell()

        # if it's a duplicated record, replace it in the csv file with '\n'
        if primary_key_string and primary_key_string in self.existing_record_position_lookup:
            existing_record_start_pos, existing_record_end_pos = self.existing_record_position_lookup[primary_key_string]
            self.temp_file_handler.seek(existing_record_start_pos)
            self.temp_file_handler.write(bytes('\n' * (existing_record_end_pos - existing_record_start_pos), 'UTF-8'))
            self.temp_file_handler.seek(current_position)
        else:
            self.row_count += 1
            if self.row_count % 100000 == 0:
                LOGGER.info(f"Processed {self.row_count} rows for stream {self.stream_schema_message['stream']}")
        
        # append record to the csv file
        self.temp_file_handler.write(bytes(self.record_to_csv_line(record) + '\n', 'UTF-8'))

        if primary_key_string:
            self.existing_record_position_lookup[primary_key_string] = ((current_position, self.temp_file_handler.tell()))


    def stream_name_to_dict(self, stream_name, separator='-'):
        catalog_name = None
        schema_name = None
        table_name = stream_name

        # Schema and table name can be derived from stream if it's in <schema_nama>-<table_name> format
        s = stream_name.split(separator)
        if len(s) == 2:
            schema_name = s[0]
            table_name = s[1]
        if len(s) > 2:
            catalog_name = s[0]
            schema_name = s[1]
            table_name = '_'.join(s[2:])

        return {
            'catalog_name': catalog_name,
            'schema_name': schema_name,
            'table_name': table_name
        }


    def column_names(self):
        return [safe_column_name(name) for name in self.stream_schema_message["schema"]["properties"].keys()]


    def primary_key_condition(self, right_table):
        stream_schema_message = self.stream_schema_message
        names = primary_column_names(stream_schema_message)
        return ' AND '.join(['s.{} = {}.{}'.format(c, right_table, c) for c in names])


    def primary_key_null_condition(self, right_table):
        stream_schema_message = self.stream_schema_message
        names = primary_column_names(stream_schema_message)
        return ' AND '.join(['{}.{} is null'.format(right_table, c) for c in names])


    def insert_from_temp_table(self, temp_table):
        stream_schema_message = self.stream_schema_message
        columns = self.column_names()
        table = self.get_table_name(stream_schema_message['stream'])

        if len(stream_schema_message['key_properties']) == 0:
            return """INSERT INTO {} ({})
                    (SELECT s.* FROM {} s)
                    """.format(table,
                               ', '.join(columns),
                               temp_table)

        return """INSERT INTO {} ({})
        (SELECT s.* FROM {} s LEFT OUTER JOIN {} t ON {} WHERE {})
        """.format(table,
                   ', '.join(columns),
                   temp_table,
                   table,
                   self.primary_key_condition('t'),
                   self.primary_key_null_condition('t'))


    def update_from_temp_table(self, temp_table):
        stream_schema_message = self.stream_schema_message
        columns = self.column_names()
        table = self.get_table_name(stream_schema_message['stream'])

        return """UPDATE {} SET {} FROM {} s
        WHERE {}
        """.format(table,
                   ', '.join(['{}=s.{}'.format(c, c) for c in columns]),
                   temp_table,
                   self.primary_key_condition(table))


    def flush_csv_to_db(self, create_indices=True):
        stream_schema_message = self.stream_schema_message
        stream_name = stream_schema_message['stream']
        LOGGER.info(f"Flushing CSV to DB for stream {stream_name}")

        with open_postgres_connection(self.config) as connection:
            with connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                inserts = 0
                updates = 0

                table_name = self.get_table_name(stream_name)
                temp_table_name = self.get_table_name(stream_name, is_temporary=True)
                cur.execute(self.create_table_query(table_name=temp_table_name, is_temporary=True))

                LOGGER.info("Loading %d rows into '%s'", self.row_count, temp_table_name)

                copy_sql = "COPY {} ({}) FROM STDIN WITH (FORMAT CSV, ESCAPE '\\')".format(
                    temp_table_name,
                    ', '.join(self.column_names())
                )

                LOGGER.debug(copy_sql)

                self.temp_file_handler.seek(0)
                csv_rows = TransformStream(self.temp_file_handler)
                cur.copy_expert(copy_sql, csv_rows)

                if len(self.stream_schema_message['key_properties']) > 0:
                    try:
                        LOGGER.info("Updating rows from temp table '%s' into main table '%s'",  temp_table_name, table_name)
                        cur.execute(self.update_from_temp_table(temp_table_name))
                        updates = cur.rowcount
                    except Exception as e:
                        # NOTE: We are pruning the \n from here to make sure we capture the error properly in stdout
                        LOGGER.error("Error: %s", str(e).replace('\n', ' '))
                        raise Exception(str(e).replace('\n', ' '))
                LOGGER.info("Inserting rows from temp table '%s' into main table '%s'",  temp_table_name, table_name)
                cur.execute(self.insert_from_temp_table(temp_table_name))
                inserts = cur.rowcount

                size_bytes = os.path.getsize(self.temp_file_name)
                LOGGER.info('Loading into %s: %s',
                                 table_name,
                                 ujson.dumps({'inserts': inserts, 'updates': updates, 'size_bytes': size_bytes}))

                LOGGER.info(f"Done flushing CSV to DB for stream {stream_name}")
                if create_indices:
                    self.create_indices(connection)


    def create_index(self, conn, stream, column):
        table = self.get_table_name(stream)
        table_without_schema = self.get_table_name(stream, without_schema=True)
        index_name = 'i_{}_{}'.format(table_without_schema[:30].replace(' ', '').replace('"', ''),
                                      column.replace(',', '_'))
        query = "CREATE INDEX IF NOT EXISTS {} ON {} ({})".format(index_name, table, column)
        LOGGER.info("Creating index on '%s' table on '%s' column(s)... %s", table, column, query)
        self.query(conn, query)

    def create_indices(self, conn):
        if self.indices and isinstance(self.indices, list):
            stream_name = self.stream_schema_message['stream']
            LOGGER.info(f"Creating indices for stream {stream_name}")
            for index in self.indices:
                self.create_index(conn, stream_name, index)
            LOGGER.info(f"Done creating indices for stream {stream_name}")
