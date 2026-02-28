import logging
import threading
import time
import psycopg2
from psycopg2.pool import SimpleConnectionPool

logger = logging.getLogger(__name__)


class BaseWriter:

    def __init__(self, connection_string, option_type):
        """
        Initialize a BaseWriter instance.

        Args:
            connection_string (str): The database connection string.
            option_type (str): The type of option (CE/PE).

        Raises:
            ValueError: If connection string is missing.
        """
        self.connection_string = connection_string.strip() if connection_string else None
        self.option_type = option_type
        self.pool = None
        self.running = True

        if not self.connection_string:
            raise ValueError("Missing connection string for %s" % option_type)

        self.connect()
        self.start_keep_alive()

    def connect(self):
        
        """
        Establish a connection to the database using the provided connection string.

        Raises:
            OperationalError: If the connection fails.
        """
        try:
            self.pool = SimpleConnectionPool(minconn=1, maxconn=5, dsn=self.connection_string)
            logger.info("%s DB pool connected", self.option_type)
        except psycopg2.OperationalError as e:
            logger.error("%s DB connection failed: %s", self.option_type, e)
            raise

    def start_keep_alive(self):
        """
        Starts a keep-alive thread that pings the database every 30 seconds to
        prevent idle connections from being closed by the database server.

        The thread attempts to reconnect if the ping fails. If the reconnect
        attempt fails, an error message is logged.
        """
        def ping():
                
            """
            Pings the database every 30 seconds to prevent idle connections from being closed by
            the database server. If the ping fails, attempts to reconnect. If the reconnect
            attempt fails, an error message is logged.
            """
            while self.running:
                time.sleep(30)
                try:
                    conn = self.pool.getconn()
                    conn.cursor().execute("SELECT 1;")
                    self.pool.putconn(conn)
                except Exception as e:
                    logger.warning("%s ping failed — reconnecting: %s", self.option_type, e)
                    try:
                        self.pool.closeall()
                    except Exception:
                        pass
                    try:
                        self.connect()
                        logger.info("%s reconnected", self.option_type)
                    except Exception as re:
                        logger.error("%s reconnect failed: %s", self.option_type, re)

        threading.Thread(target=ping, daemon=True, name="%s-ping" % self.option_type).start()
        logger.info("%s DB keep-alive started", self.option_type)

    def close(self):
        """
        Closes the database connection pool and stops the keep-alive thread.

        If the connection pool is not None, it attempts to close all connections in the pool.
        If the close operation fails, a warning message is logged with the error details.
        Finally, it sets the running flag to False and logs an info message indicating that the database connection has been disconnected.
        """
        self.running = False
        if self.pool:
            try:
                self.pool.closeall()
                logger.info("%s DB disconnected", self.option_type)
            except psycopg2.Error as e:
                logger.warning("%s disconnect error: %s", self.option_type, e)