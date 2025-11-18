import datetime
import unittest
import ujson
import os
from psycopg2.errors import InvalidTextRepresentation

import integration.utils as test_utils
import target_postgres
from target_postgres.postgres_sink import RecordValidationException
from target_postgres.utils import open_postgres_connection


class TestIntegration(unittest.TestCase):
    """
    Integration Tests
    """
    @classmethod
    def setUpClass(cls):
        print("Setting up Test class")
        cls.config = test_utils.get_db_config()
        cls.conn = open_postgres_connection(cls.config)
        cls.conn.commit()
        cls.conn.autocommit = True


    @classmethod
    def tearDownClass(cls):
        print("Tearing down Test class")
        if cls.conn:
            test_utils.clear_schemas(cls.conn)
            cls.conn.close()


    @classmethod
    def setUp(cls):
        print("Setting up Test")
        cls.config = test_utils.get_db_config()
        test_utils.clear_schemas(cls.conn)
    

    @classmethod
    def tearDown(cls):
        print("Tearing down Test")


    def test_invalid_json(self):
        """Receiving invalid JSONs should raise an exception"""
        tap_lines = test_utils.get_test_tap_lines("invalid-json.json")
        with self.assertRaises(ujson.JSONDecodeError):
            target_postgres.process_singer_messages(self.config, tap_lines)

    def test_message_order(self):
        """RECORD message without a previously received SCHEMA message should raise an exception"""
        tap_lines = test_utils.get_test_tap_lines("invalid-message-order.json")
        with self.assertRaises(Exception) as cm:
            target_postgres.process_singer_messages(self.config, tap_lines)
        self.assertEqual(
            f"A record for stream 'tap_mysql_test-test_table_two' was encountered before a corresponding schema message",
            str(cm.exception)
        )

    def test_message_with_unknown_type(self):
        """Message with an unknown type should raise an exception"""
        tap_lines = test_utils.get_test_tap_lines("message-with-unknown-type.json")
        with self.assertRaises(Exception) as cm:
            target_postgres.process_singer_messages(self.config, tap_lines)
        self.assertEqual(
            "Unknown message type 'UNKNOWN' in message {}".format(tap_lines[0].replace('"', "'")),
            str(cm.exception)
        )

    def test_schema_message_missing_stream_key(self):
        """SCHEMA message without the 'stream' key should raise an exception"""
        tap_lines = test_utils.get_test_tap_lines("message-schema-missing-stream-key.json")
        with self.assertRaises(Exception) as cm:
            target_postgres.process_singer_messages(self.config, tap_lines)
        self.assertEqual(
            f"Singer message is missing required key 'stream': {tap_lines[0]}",
            str(cm.exception)
        )

    def test_schema_message_missing_type_key(self):
        """SCHEMA message without the 'type' key should raise an exception"""
        tap_lines = test_utils.get_test_tap_lines("message-schema-missing-type-key.json")
        with self.assertRaises(Exception) as cm:
            target_postgres.process_singer_messages(self.config, tap_lines)
        self.assertEqual(
            f"Singer message is missing required key 'type': {tap_lines[0]}",
            str(cm.exception)
        )

    def test_record_message_missing_stream_key(self):
        """RECORD message without the 'stream' key should raise an exception"""
        tap_lines = test_utils.get_test_tap_lines("message-record-missing-stream-key.json")
        with self.assertRaises(Exception) as cm:
            target_postgres.process_singer_messages(self.config, tap_lines)
        self.assertEqual(
            f"Singer message is missing required key 'stream': {tap_lines[1]}",
            str(cm.exception)
        )

    def test_record_message_missing_type_key(self):
        """RECORD message without the 'type' key should raise an exception"""
        tap_lines = test_utils.get_test_tap_lines("message-record-missing-type-key.json")
        with self.assertRaises(Exception) as cm:
            target_postgres.process_singer_messages(self.config, tap_lines)
        self.assertEqual(
            f"Singer message is missing required key 'type': {tap_lines[1]}",
            str(cm.exception)
        )

    def test_schema_message_missing_schema_key(self):
        """SCHEMA message without the 'schema' key should raise an exception"""
        tap_lines = test_utils.get_test_tap_lines("message-schema-missing-schema-key.json")
        with self.assertRaises(Exception) as cm:
            target_postgres.process_singer_messages(self.config, tap_lines)
        self.assertEqual(
            "'schema' key missing - {}".format(tap_lines[0].replace('"', "'")),
            str(cm.exception)
        )

    def test_schema_message_missing_key_properties_key(self):
        """SCHEMA message without the 'key_properties' key should raise an exception"""
        tap_lines = test_utils.get_test_tap_lines("message-schema-missing-key-properties-key.json")
        with self.assertRaises(Exception) as cm:
            target_postgres.process_singer_messages(self.config, tap_lines)
        self.assertEqual(
            "'key_properties' key missing - {}".format(tap_lines[0].replace('"', "'")),
            str(cm.exception)
        )

    def test_schema_message_missing_properties_key(self):
        """SCHEMA message without the 'key_properties' key should raise an exception"""
        tap_lines = test_utils.get_test_tap_lines("message-schema-missing-properties-key.json")
        with self.assertRaises(Exception) as cm:
            target_postgres.process_singer_messages(self.config, tap_lines)
        self.assertEqual(
            "'schema.properties' key missing - {}".format(tap_lines[0].replace('"', "'")),
            str(cm.exception)
        )

    def test_record_message_missing_record_key(self):
        """RECORD message without the 'record' key should raise an exception"""
        tap_lines = test_utils.get_test_tap_lines("message-record-missing-record-key.json")
        with self.assertRaises(Exception) as cm:
            target_postgres.process_singer_messages(self.config, tap_lines)
        self.assertEqual(
            f"Singer message is missing required key 'record': {tap_lines[1]}",
            str(cm.exception)
        )
    
    def test_primary_key_required_false_and_no_primary_key_defined(self):
        """Primary key required is false and no primary key defined should not raise an exception"""
        self.config["primary_key_required"] = False
        tap_lines = test_utils.get_test_tap_lines("messages-no-primary-key.json")
        target_postgres.process_singer_messages(self.config, tap_lines)
        table_one = test_utils.query_db(self.conn, f"SELECT * FROM {self.config['default_target_schema']}.test_table_one ORDER BY c_pk")
        expected_table_one = [
            {'c_int': 1, 'c_pk': 1, 'c_varchar': '1'}
        ]
        self.assertEqual(table_one, expected_table_one)

    def test_primary_key_required_false_and_primary_key_defined(self):
        """Primary key required is false and primary key IS defined should not raise an exception"""
        self.config["primary_key_required"] = False
        tap_lines = test_utils.get_test_tap_lines("messages-with-primary-key.json")
        target_postgres.process_singer_messages(self.config, tap_lines)
        table_one = test_utils.query_db(self.conn, f"SELECT * FROM {self.config['default_target_schema']}.test_table_one ORDER BY c_pk")
        expected_table_one = [
            {'c_int': 1, 'c_pk': 1, 'c_varchar': '1'}
        ]
        self.assertEqual(table_one, expected_table_one)

    def test_primary_key_required_true_and_primary_key_defined(self):
        """Primary key required is true and primary key IS defined should not raise an exception"""
        self.config["primary_key_required"] = True
        tap_lines = test_utils.get_test_tap_lines("messages-with-primary-key.json")
        target_postgres.process_singer_messages(self.config, tap_lines)
        table_one = test_utils.query_db(self.conn, f"SELECT * FROM {self.config['default_target_schema']}.test_table_one ORDER BY c_pk")
        expected_table_one = [
            {'c_int': 1, 'c_pk': 1, 'c_varchar': '1'}
        ]
        self.assertEqual(table_one, expected_table_one)

    def test_primary_key_required_true_and_no_primary_key_defined(self):
        """Primary key required is true and NO primary key is defined should raise an exception"""
        self.config["primary_key_required"] = True
        tap_lines = test_utils.get_test_tap_lines("messages-no-primary-key.json")
        with self.assertRaises(Exception) as cm:
            target_postgres.process_singer_messages(self.config, tap_lines)
        self.assertEqual(
            f"Primary key is set to mandatory but not defined for the 'test_table_one' stream",
            str(cm.exception)
        )

    def test_no_default_target_schema_and_no_schema_mapping(self):
        """No default target schema and no schema mapping should raise an exception"""
        self.config["default_target_schema"] = None
        self.config["schema_mapping"] = None
        tap_lines = test_utils.get_test_tap_lines("messages-no-primary-key.json")
        with self.assertRaises(Exception) as cm:
            target_postgres.process_singer_messages(self.config, tap_lines)
        self.assertEqual(
            "Target schema name not defined in config. Neither 'default_target_schema' (string)"
                            "nor 'schema_mapping' (object) defines target schema for 'test_table_one' stream.",
            str(cm.exception)
        )

    def test_default_target_schema_was_created(self):
        """Check that the default target schema was created"""
        schemas = test_utils.get_schemas(self.conn)
        self.assertEqual(schemas, set())

        tap_lines = test_utils.get_test_tap_lines("messages-no-primary-key.json")
        target_postgres.process_singer_messages(self.config, tap_lines)

        schemas = test_utils.get_schemas(self.conn)
        self.assertEqual(schemas, {self.config['default_target_schema']})

    def test_schema_from_schema_mapping(self):
        """Check that the schema from schema mapping was created"""
        schemas = test_utils.get_schemas(self.conn)
        self.assertEqual(schemas, set())

        self.config["default_target_schema"] = None
        self.config["schema_mapping"] = {
            "tap_mysql_test": {
                "target_schema": "test_schema_one"
            }
        }

        tap_lines = test_utils.get_test_tap_lines("messages-schema-mapping.json")
        target_postgres.process_singer_messages(self.config, tap_lines)

        schemas = test_utils.get_schemas(self.conn)
        self.assertEqual(schemas, {"test_schema_one"})

        table_one = test_utils.query_db(self.conn, "SELECT * FROM test_schema_one.tap_mysql_test_test_table_one ORDER BY c_pk")
        expected_table_one = [
            {'c_int': 1, 'c_pk': 1, 'c_varchar': '1'}
        ]
        self.assertEqual(table_one, expected_table_one)

    def test_indices_from_schema_mapping(self):
        """Check that the indices from schema mapping were created"""
        indices = test_utils.get_table_indexes(self.conn, "test_schema_one.tap_mysql_test_test_table_one")
        self.assertEqual(indices, [])

        self.config["default_target_schema"] = None
        self.config["schema_mapping"] = {
            "tap_mysql_test": {
                "target_schema": "test_schema_one",
                "indices": {
                    "test_table_one": ["c_varchar"]
                }
            }
        }

        tap_lines = test_utils.get_test_tap_lines("messages-schema-mapping.json")
        target_postgres.process_singer_messages(self.config, tap_lines)

        indices = test_utils.get_table_indexes(self.conn, "test_schema_one.tap_mysql_test_test_table_one")

        expected_indices = [
            {
                "indexname": "i_tap_mysql_test_test_table_one_c_varchar",
                "indexdef": "CREATE INDEX i_tap_mysql_test_test_table_one_c_varchar ON test_schema_one.tap_mysql_test_test_table_one USING btree (c_varchar)"
            }
        ]

        self.assertEqual(indices, expected_indices)

    def test_indices_from_schema_mapping_2(self):
        """Check that the indices from schema mapping were created"""
        indices = test_utils.get_table_indexes(self.conn, "test_schema_one.tap_mysql_test_test_table_one")
        self.assertEqual(indices, [])

        self.config["default_target_schema"] = None
        self.config["schema_mapping"] = {
            "tap_mysql_test": {
                "target_schema": "test_schema_one",
                "indices": {
                    "test_table_one": ["c_varchar", "c_int"]
                }
            }
        }

        tap_lines = test_utils.get_test_tap_lines("messages-schema-mapping.json")
        target_postgres.process_singer_messages(self.config, tap_lines)

        indices = test_utils.get_table_indexes(self.conn, "test_schema_one.tap_mysql_test_test_table_one")

        expected_indices = [
            {
                "indexname": "i_tap_mysql_test_test_table_one_c_int",
                "indexdef": "CREATE INDEX i_tap_mysql_test_test_table_one_c_int ON test_schema_one.tap_mysql_test_test_table_one USING btree (c_int)"
            },
            {
                "indexname": "i_tap_mysql_test_test_table_one_c_varchar",
                "indexdef": "CREATE INDEX i_tap_mysql_test_test_table_one_c_varchar ON test_schema_one.tap_mysql_test_test_table_one USING btree (c_varchar)"
            }
        ]

        self.assertEqual(indices, expected_indices)

    def test_table_pks_exists_in_db(self):
        """Check that the table PKs exist in the database"""
        pks = test_utils.get_table_primary_keys(self.conn, "test_public.test_table_one")
        self.assertEqual(pks, [])

        tap_lines = test_utils.get_test_tap_lines("messages-with-primary-key.json")
        target_postgres.process_singer_messages(self.config, tap_lines)

        pks = test_utils.get_table_primary_keys(self.conn, "test_public.test_table_one")
        
        expected_pks = ["c_pk"]

        self.assertEqual(pks, expected_pks)

    def test_table_pks_exists_in_db_2(self):
        """Check that the table PKs exist in the database"""
        pks = test_utils.get_table_primary_keys(self.conn, "test_public.test_table_one")
        self.assertEqual(pks, [])

        tap_lines = test_utils.get_test_tap_lines("messages-with-primary-key-2.json")
        target_postgres.process_singer_messages(self.config, tap_lines)

        pks = test_utils.get_table_primary_keys(self.conn, "test_public.test_table_one")
        
        expected_pks = ["c_pk", "c_varchar"]

        self.assertEqual(pks, expected_pks)

    def test_column_types_are_correct(self):
        """Check that the column types are correct"""
        columns = test_utils.get_table_columns(self.conn, "test_public.test_table_one")
        self.assertEqual(columns, [])

        tap_lines = test_utils.get_test_tap_lines("messages-column-types.json")
        target_postgres.process_singer_messages(self.config, tap_lines)

        columns = test_utils.get_table_columns(self.conn, "test_public.test_table_one")

        expected_columns = [
            {
                "schema_and_table": "test_public.test_table_one",
                "column_name": "c_pk",
                "data_type": "integer",
                "is_nullable": "NO",
                "is_pk": "YES"
            },
            {
                "schema_and_table": "test_public.test_table_one",
                "column_name": "c_varchar",
                "data_type": "character varying",
                "is_nullable": "YES",
                "is_pk": "NO"
            },
            {
                "schema_and_table": "test_public.test_table_one",
                "column_name": "c_smallint",
                "data_type": "smallint",
                "is_nullable": "YES",
                "is_pk": "NO"
            },
            {
                "schema_and_table": "test_public.test_table_one",
                "column_name": "c_int",
                "data_type": "integer",
                "is_nullable": "YES",
                "is_pk": "NO"
            },
            {
                "schema_and_table": "test_public.test_table_one",
                "column_name": "c_bigint",
                "data_type": "bigint",
                "is_nullable": "YES",
                "is_pk": "NO"
            },
            {
                "schema_and_table": "test_public.test_table_one",
                "column_name": "c_numeric",
                "data_type": "numeric",
                "is_nullable": "YES",
                "is_pk": "NO"
            },
            {
                "schema_and_table": "test_public.test_table_one",
                "column_name": "c_number",
                "data_type": "double precision",
                "is_nullable": "YES",
                "is_pk": "NO"
            },
            {
                "schema_and_table": "test_public.test_table_one",
                "column_name": "c_boolean",
                "data_type": "boolean",
                "is_nullable": "YES",
                "is_pk": "NO"
            },
            {
                "schema_and_table": "test_public.test_table_one",
                "column_name": "c_time",
                "data_type": "time without time zone",
                "is_nullable": "YES",
                "is_pk": "NO"
            },
            {
                "schema_and_table": "test_public.test_table_one",
                "column_name": "c_datetime",
                "data_type": "timestamp without time zone",
                "is_nullable": "YES",
                "is_pk": "NO"
            },
            {
                "schema_and_table": "test_public.test_table_one",
                "column_name": "c_object",
                "data_type": "jsonb",
                "is_nullable": "YES",
                "is_pk": "NO"
            },
            {
                "schema_and_table": "test_public.test_table_one",
                "column_name": "c_array",
                "data_type": "jsonb",
                "is_nullable": "YES",
                "is_pk": "NO"
            }
        ]

        self.assertEqual(columns, expected_columns)

    def test_table_data_is_correct(self):
        """Check that the table data is correct"""
        tap_lines = test_utils.get_test_tap_lines("messages-multiple-lines.json")
        target_postgres.process_singer_messages(self.config, tap_lines)

        data = test_utils.query_db(self.conn, "SELECT * FROM test_public.test_table_one ORDER BY c_pk")

        expected_data = [
            {
                "c_pk": 1,
                "c_varchar": "varchar_value_1",
                "c_smallint": 1,
                "c_int": 2147483647,
                "c_bigint": 2147483648,
                "c_numeric": 1,
                "c_number": 1.25,
                "c_boolean": True,
                "c_time": datetime.time(12, 00),
                "c_datetime": datetime.datetime(2019, 1, 31, 15, 51, 47, 465408),
                "c_object": {"a": 1},
                "c_array": [1, 2, 3]
            },
            {
                "c_pk": 2,
                "c_varchar": "varchar_value_2",
                "c_smallint": 2,
                "c_int": 2147483646,
                "c_bigint": 2147483649,
                "c_numeric": 2,
                "c_number": 2.25,
                "c_boolean": False,
                "c_time": datetime.time(13, 00),
                "c_datetime": datetime.datetime(2020, 1, 31, 15, 51, 47, 465408),
                "c_object": {"b": 2},
                "c_array": [4, 5, 6]
            }
        ]
        self.assertEqual(data, expected_data)

    def test_table_data_is_correct_multiple_streams(self):
        """Check that the table data is correct for multiple streams"""
        tap_lines = test_utils.get_test_tap_lines("messages-multiple-streams.json")
        target_postgres.process_singer_messages(self.config, tap_lines)

        data_one = test_utils.query_db(self.conn, "SELECT * FROM test_public.test_table_one ORDER BY c_pk")
        data_two = test_utils.query_db(self.conn, "SELECT * FROM test_public.test_table_two ORDER BY c_pk")

        expected_data_one = [
            {
                "c_pk": 1,
                "c_varchar": "varchar_value_1",
                "c_int": 1
            },
            {
                "c_pk": 2,
                "c_varchar": "varchar_value_2",
                "c_int": 2,
            }
        ]

        expected_data_two = [
            {
                "c_pk": 1,
                "c_varchar": "varchar_value_3",
                "c_int": 3
            },
            {
                "c_pk": 2,
                "c_varchar": "varchar_value_4",
                "c_int": 4,
            }
        ]

        self.assertEqual(data_one, expected_data_one)
        self.assertEqual(data_two, expected_data_two)

    def test_table_data_json_values_are_correct(self):
        """Check that the table data json values are correct"""
        tap_lines = test_utils.get_test_tap_lines("messages-multiple-lines-json-values.json")
        target_postgres.process_singer_messages(self.config, tap_lines)

        data = test_utils.query_db(self.conn, "SELECT * FROM test_public.test_table_one ORDER BY c_pk")

        expected_data = [
            {
                "c_pk": 1,
                "c_object": {
                    "a": 1,
                    "b": 2,
                    "c": {
                        "d": 3
                    }
                },
                "c_array": [1, "2", {"e": 3}]
            },
            {
                "c_pk": 2,
                "c_object": {"b": 2, "f": None},
                "c_array": [None, 5, 6]
            },
            {
                "c_pk": 3,
                "c_object": None,
                "c_array": None
            }
        ]
        self.assertEqual(data, expected_data)

    def test_loading_tables_with_custom_temp_dir(self):
        """Loading multiple tables from the same input tap using custom temp directory"""
        self.config["temp_dir"] = "/tmp/test_temp_dir"
        tap_lines = test_utils.get_test_tap_lines("messages-multiple-lines.json")
        target_postgres.process_singer_messages(self.config, tap_lines)

        self.assertTrue(os.path.exists(self.config["temp_dir"]))

    def test_validate_records_with_valid_records(self):
        """Validate records with valid records"""
        self.config["validate_records"] = True
        tap_lines = test_utils.get_test_tap_lines("messages-multiple-lines.json")
        target_postgres.process_singer_messages(self.config, tap_lines)

        data = test_utils.query_db(self.conn, "SELECT * FROM test_public.test_table_one ORDER BY c_pk")

        expected_data = [
            {
                "c_pk": 1,
                "c_varchar": "varchar_value_1",
                "c_smallint": 1,
                "c_int": 2147483647,
                "c_bigint": 2147483648,
                "c_numeric": 1,
                "c_number": 1.25,
                "c_boolean": True,
                "c_time": datetime.time(12, 00),
                "c_datetime": datetime.datetime(2019, 1, 31, 15, 51, 47, 465408),
                "c_object": {"a": 1},
                "c_array": [1, 2, 3]
            },
            {
                "c_pk": 2,
                "c_varchar": "varchar_value_2",
                "c_smallint": 2,
                "c_int": 2147483646,
                "c_bigint": 2147483649,
                "c_numeric": 2,
                "c_number": 2.25,
                "c_boolean": False,
                "c_time": datetime.time(13, 00),
                "c_datetime": datetime.datetime(2020, 1, 31, 15, 51, 47, 465408),
                "c_object": {"b": 2},
                "c_array": [4, 5, 6]
            }
        ]
        self.assertEqual(data, expected_data)

    def test_validate_records_with_invalid_record(self):
        """Validate records with invalid record"""
        self.config["validate_records"] = True
        tap_lines = test_utils.get_test_tap_lines("messages-validation-invalid-record.json")
        
        # Loading invalid records when record validation enabled should fail at ...
        with self.assertRaises(RecordValidationException):
            target_postgres.process_singer_messages(self.config, tap_lines)

        # Loading invalid records when record validation disabled should fail at load time
        self.config['validate_records'] = False
        with self.assertRaises(InvalidTextRepresentation):
            target_postgres.process_singer_messages(self.config, tap_lines)

    def test_insertion_method_append(self):
        """Loading data with insertion_method 'append' (default) should append data to the table"""
        tap_lines = test_utils.get_test_tap_lines("messages-no-primary-key.json")
        target_postgres.process_singer_messages(self.config, tap_lines)

        count = test_utils.query_db(self.conn, "SELECT count(*) FROM test_public.test_table_one")
        self.assertEqual(count[0]['count'], 1)

        # load more data
        target_postgres.process_singer_messages(self.config, tap_lines)
        
        count = test_utils.query_db(self.conn, "SELECT count(*) FROM test_public.test_table_one")
        self.assertEqual(count[0]['count'], 2)

    def test_insertion_method_truncate(self):
        """Loading data with insertion_method 'truncate' should truncate the table"""
        self.config["insertion_method"] = "truncate"
        tap_lines = test_utils.get_test_tap_lines("messages-no-primary-key.json")
        target_postgres.process_singer_messages(self.config, tap_lines)

        count = test_utils.query_db(self.conn, "SELECT count(*) FROM test_public.test_table_one")
        self.assertEqual(count[0]['count'], 1)

        # load more data
        target_postgres.process_singer_messages(self.config, tap_lines)
        
        count = test_utils.query_db(self.conn, "SELECT count(*) FROM test_public.test_table_one")
        self.assertEqual(count[0]['count'], 1)

    def test_insertion_method_truncate_with_insertion_method_tables(self):
        """Loading data with insertion_method 'truncate' and insertion_method_tables
        specified should truncate only the specified tables"""
        self.config["insertion_method"] = "truncate"
        self.config["insertion_method_tables"] = ["test_table_one"]
        tap_lines = test_utils.get_test_tap_lines("messages-multiple-streams-no-primary-key.json")
        target_postgres.process_singer_messages(self.config, tap_lines)

        count_table_one = test_utils.query_db(self.conn, "SELECT count(*) FROM test_public.test_table_one")
        count_table_two = test_utils.query_db(self.conn, "SELECT count(*) FROM test_public.test_table_two")
        self.assertEqual(count_table_one[0]['count'], 1)
        self.assertEqual(count_table_two[0]['count'], 1)

        # load more data
        target_postgres.process_singer_messages(self.config, tap_lines)

        count_table_one = test_utils.query_db(self.conn, "SELECT count(*) FROM test_public.test_table_one")
        count_table_two = test_utils.query_db(self.conn, "SELECT count(*) FROM test_public.test_table_two")
        # only table one should be truncated
        self.assertEqual(count_table_one[0]['count'], 1)
        self.assertEqual(count_table_two[0]['count'], 2)
