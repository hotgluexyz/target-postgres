import os
import logging
import logging.config
import psycopg2
from decimal import Decimal

def get_logger(name):
    """Return a Logger instance to use in singer."""
    # Use custom logging config provided by environment variable
    if 'LOGGING_CONF_FILE' in os.environ and os.environ['LOGGING_CONF_FILE']:
        path = os.environ['LOGGING_CONF_FILE']
        logging.config.fileConfig(path, disable_existing_loggers=False)
    # Use the default logging conf that meets the singer specs criteria
    else:
        this_dir, _ = os.path.split(__file__)
        path = os.path.join(this_dir, 'logging.conf')
        logging.config.fileConfig(path, disable_existing_loggers=False)

    return logging.getLogger(name)


def float_to_decimal(value):
    """Walk the given data structure and turn all instances of float into
    double."""
    if isinstance(value, float):
        return Decimal(str(value))
    if isinstance(value, list):
        return [float_to_decimal(child) for child in value]
    if isinstance(value, dict):
        return {k: float_to_decimal(v) for k, v in value.items()}
    return value


def open_postgres_connection(config):
        conn_string = "host='{}' dbname='{}' user='{}' password='{}' port='{}'".format(
            config['host'],
            config['dbname'],
            config['user'],
            config['password'],
            config['port']
        )

        if 'ssl' in config and config['ssl'] in [True, 'true']:
            conn_string += " sslmode='require'"

        conn = psycopg2.connect(conn_string)
        
        # set statement timeout to 20 minutes (after connection is established)
        # This avoids issues with pooled connections that don't support startup parameters
        with conn.cursor() as cur:
            cur.execute(f"SET statement_timeout = {str(60000 * 20)}")
        
        return conn


def test_postgres_connection(config):
    LOGGER = get_logger("target_postgres")
    LOGGER.info(f"Testing PostgreSQL connection")
    with open_postgres_connection(config) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
            LOGGER.info(f"PostgreSQL connection test successful")
