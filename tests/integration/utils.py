import os
import psycopg2
import psycopg2.extras


SCHEMA_PREFIX = "test_"


def get_db_config():
    config = {}

    # --------------------------------------------------------------------------
    # Default configuration settings for integration tests.
    # --------------------------------------------------------------------------
    # Postgres instance
    config['host'] = "localhost"
    config['port'] = 5432
    config['user'] = "postgres"
    config['password'] = "postgres"
    config['dbname'] = "target_test_db"
    config['default_target_schema'] = f"{SCHEMA_PREFIX}public"

    return config


def query_db(connection, query, params=None):
    with connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            query,
            params
        )

        if cur.rowcount > 0:
            return cur.fetchall()

        return []


def get_schemas(conn):
    schemas = query_db(conn, """
        SELECT schema_name
        FROM information_schema.schemata
        WHERE schema_name like %s;
    """, (f"{SCHEMA_PREFIX}%",))
    return {schema["schema_name"] for schema in schemas}


def clear_schemas(conn):
    schemas = get_schemas(conn)
    for schema in schemas:
        query_db(conn, f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    pass


def get_table_indexes(conn, table_name):
    indexes = query_db(conn, """
        SELECT
            indexname, indexdef
        FROM pg_indexes
        WHERE concat(schemaname, '.', tablename) = %s
        ORDER BY indexname;
    """, (table_name,))
    return indexes


def get_table_columns(conn, table_name):
    columns = query_db(conn, """
        SELECT
            concat(c.table_schema, '.', c.table_name) as schema_and_table,
			c.column_name,
			c.data_type,
			c.is_nullable,
			CASE
				WHEN (SELECT count(1)
						FROM information_schema.table_constraints tc 
						JOIN information_schema.constraint_column_usage AS ccu USING (constraint_schema, constraint_name) 
						WHERE constraint_type = 'PRIMARY KEY' and tc.table_name = c.table_name
							and ccu.column_name = c.column_name and tc.table_schema = c.table_schema) = 1 THEN 'YES'
				ELSE 'NO'
			END as is_pk
		FROM information_schema.columns AS c
        WHERE concat(table_schema, '.', table_name) = %s;
    """, (table_name,))
    return columns


def get_table_primary_keys(conn, table_name):
    primary_keys = get_table_columns(conn, table_name)
    primary_keys = [column["column_name"] for column in primary_keys if column["is_pk"] == "YES"]
    return primary_keys


def get_test_tap_lines(filename):
    lines = []
    with open('{}/resources/{}'.format(os.path.dirname(__file__), filename)) as tap_stdout:
        for line in tap_stdout.readlines():
            lines.append(line)

    return lines

