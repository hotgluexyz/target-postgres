import psycopg2
import time
import singer

from contextlib import contextmanager
from psycopg2.extras import LoggingConnection, LoggingCursor

LIMIT_RETRIES = 5

class _MillisLoggingCursor(LoggingCursor):
    """
    An implementation of LoggingCursor which tracks duration of queries.
    """

    def execute(self, query, vars=None):
        self.timestamp = time.monotonic()
        return super(_MillisLoggingCursor, self).execute(query, vars)

    def callproc(self, procname, vars=None):
        self.timestamp = time.monotonic()
        return super(_MillisLoggingCursor, self).callproc(procname, vars)


class MillisLoggingConnection(LoggingConnection):
    """
    An implementation of LoggingConnection which tracks duration of queries.
    """

    def filter(self, msg, curs):
        return "MillisLoggingConnection: {} millis spent executing: {}".format(
            int((time.monotonic() - curs.timestamp) * 1000),
            msg
        )

    def cursor(self, *args, **kwargs):
        kwargs.setdefault('cursor_factory', _MillisLoggingCursor)
        return LoggingConnection.cursor(self, *args, **kwargs)

class PostgresClient():
    LOGGER = singer.get_logger()

    def __init__(self, host, port, dbname, user, password, sslmode, sslcert, sslkey, sslrootcert, sslcrl):
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.dbname = dbname
        self.sslmode = sslmode
        self.sslcert = sslcert
        self.sslkey = sslkey
        self.sslrootcert = sslrootcert
        self.sslcrl = sslcrl
        self._connection = None
        self._connect()

    def __enter__(self):
        return self
    
    def __exit__(self, type, value, traceback):
        if self._connection and not self._connection.closed:
            self._connection.close()
            self._connection = None

    def _connect(self, retry_counter=0):
        if not self._connection or self._connection.closed:
            try:
                self._connection = psycopg2.connect(
                    connection_factory=MillisLoggingConnection,
                    host=self.host,
                    port=self.port,
                    dbname=self.dbname,
                    user=self.user,
                    password=self.password,
                    sslmode=self.sslmode,
                    sslcert=self.sslcert,
                    sslkey=self.sslkey,
                    sslrootcert=self.sslrootcert,
                    sslcrl=self.sslcrl
                )
                retry_counter = 0
                try:
                    self._connection.initialize(self.LOGGER)
                    self.LOGGER.debug('PostgresTarget set to log all queries.')
                except AttributeError:
                    self.LOGGER.debug('PostgresTarget disabling logging all queries.')
                
                return self._connection
            except psycopg2.OperationalError as error:
                if retry_counter >= LIMIT_RETRIES:
                    raise error
                else:
                    retry_counter += 1
                    print("got error {}. reconnecting {}".format(str(error).strip(), retry_counter))
                    time.sleep(5)
                    self._connect(retry_counter)
            except (Exception, psycopg2.Error) as error:
                raise error

    @contextmanager
    def cursor(self):
        if not self._connection or self._connection.closed:
            self._connect()
        
        with self._connection.cursor() as cursor:
            yield cursor

    @property
    def dsn(self):
        return self._connection.dsn

    def get_dsn_parameters(self):
        return self._connection.get_dsn_parameters()

    def close(self):
        if self._connection:
            self._connection.close()
            print("PostgreSQL connection is closed")
        self._connection = None
